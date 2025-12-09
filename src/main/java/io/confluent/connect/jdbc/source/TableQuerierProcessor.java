/*
 * Copyright [2024 - 2024] Confluent Inc.
 */

package io.confluent.connect.jdbc.source;

import io.confluent.connect.jdbc.dialect.DatabaseDialect;
import io.confluent.connect.jdbc.util.CachedConnectionProvider;
import io.confluent.connect.jdbc.util.LogUtil;
import io.confluent.connect.jdbc.util.RecordDestination;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.source.SourceRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLException;
import java.sql.SQLNonTransientException;
import java.time.Duration;
import java.util.PriorityQueue;

public class TableQuerierProcessor {

  private static final Logger log = LoggerFactory.getLogger(TableQuerierProcessor.class);

  private final JdbcSourceTaskConfig config;
  private final Time time;
  private final PriorityQueue<TableQuerier> tableQueue;
  private final Boolean shouldRedactSensitiveLogs;
  private CachedConnectionProvider cachedConnectionProvider;
  private final int maxRetriesPerQuerier;
  private final Duration timeout = Duration.ofSeconds(90);

  public TableQuerierProcessor(
      JdbcSourceTaskConfig config,
      Time time,
      PriorityQueue<TableQuerier> tableQueue,
      CachedConnectionProvider cachedConnectionProvider,
      DatabaseDialect dialect
  ) {
    this.config = config;
    this.time = time;
    this.tableQueue = tableQueue;
    this.cachedConnectionProvider = cachedConnectionProvider;
    this.maxRetriesPerQuerier = config.getInt(JdbcSourceConnectorConfig.QUERY_RETRIES_CONFIG);
    this.shouldRedactSensitiveLogs = config.isQueryMasked();
  }

  public long process(RecordDestination<SourceRecord> destination) {
    if (!isReadyToProcess()) {
      return 0;
    }

    while (destination.isRunning() && !tableQueue.isEmpty()) {
      final TableQuerier querier = tableQueue.peek();

      if (!querier.querying()) {
        // If not in the middle of an update, wait for next update time
        final long nextUpdate = querier.getLastUpdate()
            + config.getInt(JdbcSourceTaskConfig.POLL_INTERVAL_MS_CONFIG);
        final long now = time.milliseconds();
        final long sleepMs = Math.min(nextUpdate - now, 100);

        if (sleepMs > 0) {
          log.trace("Waiting {} ms to poll {} next", sleepMs, querier.toString());
          time.sleep(sleepMs);
          continue; // Re-check stop flag before continuing
        }
      }

      if (!destination.isRunning()) {
        break;
      }

      try {
        processQuerier(destination, querier);
      } catch (SQLNonTransientException sqle) {
        handleNonTransientException(destination, querier, sqle);
        return 0; // Exit the processing loop after failure
      } catch (SQLException sqle) {
        handleSqlException(destination, querier, sqle);
        if (maxRetriesPerQuerier > 0 && querier.getAttemptedRetryCount() >= maxRetriesPerQuerier) {
          return 0; // Exit after max retries
        }
      } catch (InterruptedException e) {
        handleInterruptedException(querier, e);
        return 0; // Exit on interruption
      } catch (Throwable t) {
        handleThrowable(destination, querier, t);
        return 0; // Exit on any other throwable
      }
    }
    log.info("Task has been stopped, exiting the processor");
    return 0;
  }

  private boolean isReadyToProcess() {
    // If the call to get tables has not completed we will not do anything.
    // This is only valid in table mode.
    Boolean tablesFetched = config.getBoolean(JdbcSourceTaskConfig.TABLES_FETCHED);
    return config.getQuery().isPresent() || tablesFetched;
  }

  private void processQuerier(RecordDestination<SourceRecord> destination, TableQuerier querier)
      throws SQLException, InterruptedException {
    log.debug("Checking for next block of results from {}", querier.toString());
    querier.maybeStartQuery(cachedConnectionProvider.getConnection());
    int numPolledRecords = 0;
    while (destination.isRunning() && querier.next()) {
      try {
        numPolledRecords++;
        sendToQueue(destination, querier.extractRecord());
      } catch (RecordDestination.DestinationClosedException e) {
        log.error("Destination was closed, exiting");
        return;
      } catch (InterruptedException e) {
        log.error("Interrupted while waiting to send to destination, exiting. "
            + "Exception: ", e);
        throw e;
      }
    }
    querier.resetRetryCount();
    // We are finished processing the results from the current querier, we can reset and send
    // the querier to the tail of the queue
    resetAndRequeueHead(querier, false);

    if (numPolledRecords == 0) {
      log.trace("No updates for {}", querier.toString());
    } else {
      log.debug("Enqueued {} records for {}", numPolledRecords, querier);
    }
  }

  private void handleNonTransientException(RecordDestination<SourceRecord> destination, 
                                           TableQuerier querier, SQLNonTransientException sqle) {
    SQLException redactedException = shouldRedactSensitiveLogs
              ? LogUtil.redactSensitiveData(sqle) : sqle;
    log.error("Non-transient SQL exception while running query for table: {}",
        querier, redactedException);
    resetAndRequeueHead(querier, true);
    // This task has failed, report failure to destination
    destination.failWith(new ConnectException(redactedException));
  }

  private void handleSqlException(RecordDestination<SourceRecord> destination,
                                  TableQuerier querier, SQLException sqle) {
    SQLException redactedException = shouldRedactSensitiveLogs
              ? LogUtil.redactSensitiveData(sqle) : sqle;
    log.error(
        "SQL exception while running query for table: {}." + " Attempting retry {} of {} attempts.",
        querier,
        querier.getAttemptedRetryCount() + 1,
        maxRetriesPerQuerier,
        redactedException);

    resetAndRequeueHead(querier, false);
    if (maxRetriesPerQuerier > 0 && querier.getAttemptedRetryCount() + 1 >= maxRetriesPerQuerier) {
      destination.failWith(
          new ConnectException("Failed to query table after retries", redactedException));
      return;
    }
    querier.incrementRetryCount();
  }

  private void handleInterruptedException(TableQuerier querier, InterruptedException e) {
    resetAndRequeueHead(querier, true);
    // Interruption should not be treated as a failure, just stop processing
    Thread.currentThread().interrupt();
  }

  private void handleThrowable(RecordDestination<SourceRecord> destination, 
                               TableQuerier querier, Throwable t) {
    log.error("Failed to run query for table: {}", querier, t);
    resetAndRequeueHead(querier, true);
    // This task has failed, report failure to destination
    destination.failWith(new ConnectException("Error while processing table querier", t));
  }

  private void sendToQueue(RecordDestination<SourceRecord> destination, SourceRecord sourceRecord)
      throws RecordDestination.DestinationClosedException, InterruptedException {
    while (!destination.send(sourceRecord, timeout)) {
      log.warn("Unable to add record to eventQueue within {} s; retrying", timeout);
    }
  }

  public void shutdown() {
    while (!tableQueue.isEmpty()) {
      TableQuerier querier = tableQueue.peek();
      if (querier != null) {
        resetAndRequeueHead(querier, true);
      }
    }
  }

  private synchronized void resetAndRequeueHead(TableQuerier expectedHead, boolean resetOffset) {
    log.debug("Resetting querier {}", expectedHead.toString());
    TableQuerier removedQuerier = tableQueue.poll();
    assert removedQuerier == expectedHead;
    expectedHead.reset(time.milliseconds(), resetOffset);
    if (!resetOffset) {
      tableQueue.add(expectedHead);
    }
  }


}
