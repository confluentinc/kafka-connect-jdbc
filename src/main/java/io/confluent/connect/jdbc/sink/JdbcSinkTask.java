/*
 * Copyright 2018 Confluent Inc.
 *
 * Licensed under the Confluent Community License (the "License"); you may not use
 * this file except in compliance with the License.  You may obtain a copy of the
 * License at
 *
 * http://www.confluent.io/confluent-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OF ANY KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations under the License.
 */

package io.confluent.connect.jdbc.sink;

import io.confluent.connect.jdbc.util.LogUtil;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.errors.RetriableException;
import org.apache.kafka.connect.sink.ErrantRecordReporter;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLException;
import java.util.Collection;
import java.util.Collections;
import java.util.EnumSet;
import java.util.Map;
import java.util.function.LongSupplier;

import io.confluent.connect.jdbc.dialect.DatabaseDialect;
import io.confluent.connect.jdbc.dialect.DatabaseDialects;
import io.confluent.connect.jdbc.dialect.PostgreSqlDatabaseDialect;
import io.confluent.connect.jdbc.dialect.SqliteDatabaseDialect;
import io.confluent.connect.jdbc.util.TableType;
import io.confluent.connect.jdbc.util.Version;

public class JdbcSinkTask extends SinkTask {
  private static final Logger log = LoggerFactory.getLogger(JdbcSinkTask.class);

  private final LongSupplier nanoClock;

  ErrantRecordReporter reporter;
  DatabaseDialect dialect;
  JdbcSinkConfig config;
  JdbcDbWriter writer;
  ReceiptWriteFence writeFence;
  int remainingRetries;

  public JdbcSinkTask() {
    this(System::nanoTime);
  }

  JdbcSinkTask(LongSupplier nanoClock) {
    this.nanoClock = nanoClock;
  }

  @Override
  public void start(final Map<String, String> props) {
    log.info("Starting JDBC Sink task");
    config = new JdbcSinkConfig(props);
    writeFence = new ReceiptWriteFence(config, nanoClock);
    initWriter();
    remainingRetries = config.maxRetries;
    try {
      reporter = context.errantRecordReporter();
    } catch (NoSuchMethodError | NoClassDefFoundError e) {
      // Will occur in Connect runtimes earlier than 2.6
      reporter = null;
    }
  }

  void initWriter() {
    log.info("Initializing JDBC writer");
    if (config.dialectName != null && !config.dialectName.trim().isEmpty()) {
      dialect = DatabaseDialects.create(config.dialectName, config);
    } else {
      dialect = DatabaseDialects.findBestFor(config.connectionUrl, config);
    }
    final DbStructure dbStructure = new DbStructure(dialect);
    log.info("Initializing writer using SQL dialect: {}", dialect.getClass().getSimpleName());
    writer = new JdbcDbWriter(config, dialect, dbStructure, writeFence);
    validateWriteFenceEligibility();
    log.info("JDBC writer initialized");
  }

  private void validateWriteFenceEligibility() {
    if (!writeFence.enabled()) {
      return;
    }
    if (!(dialect instanceof PostgreSqlDatabaseDialect)
        && !(dialect instanceof SqliteDatabaseDialect)) {
      throw new ConfigException(
          JdbcSinkConfig.WRITE_FENCE_TIMEOUT_MS,
          config.writeFenceTimeoutMs,
          "JDBC write fence is supported only for PostgreSQL and SQLite dialects"
      );
    }
    validateWriteFenceSchemaSettings();
    if (!config.tableTypes.equals(EnumSet.of(TableType.TABLE))) {
      throw new ConfigException(
          JdbcSinkConfig.WRITE_FENCE_TIMEOUT_MS,
          config.writeFenceTimeoutMs,
          "JDBC write fence requires table.types=table"
      );
    }
    if (config.insertMode != JdbcSinkConfig.InsertMode.UPSERT
        && config.insertMode != JdbcSinkConfig.InsertMode.UPDATE) {
      throw new ConfigException(
          JdbcSinkConfig.WRITE_FENCE_TIMEOUT_MS,
          config.writeFenceTimeoutMs,
          "JDBC write fence requires insert.mode=upsert or insert.mode=update"
      );
    }
    if (config.pkMode == JdbcSinkConfig.PrimaryKeyMode.NONE) {
      throw new ConfigException(
          JdbcSinkConfig.WRITE_FENCE_TIMEOUT_MS,
          config.writeFenceTimeoutMs,
          "JDBC write fence requires a usable primary key"
      );
    }
  }

  private void validateWriteFenceSchemaSettings() {
    if (config.autoCreate || config.autoEvolve) {
      throw new ConfigException(
          JdbcSinkConfig.WRITE_FENCE_TIMEOUT_MS,
          config.writeFenceTimeoutMs,
          "JDBC write fence requires auto.create=false and auto.evolve=false"
      );
    }
  }

  @Override
  public void put(Collection<SinkRecord> records) {
    if (records.isEmpty()) {
      return;
    }
    try {
      writeFence.recordPutDelivery(records);
      final SinkRecord first = records.iterator().next();
      final int recordsCount = records.size();
      log.debug(
          "Received {} records. First record kafka coordinates:({}-{}-{}). Writing them to the "
          + "database...",
          recordsCount, first.topic(), first.kafkaPartition(), first.kafkaOffset()
      );
      writeFence.check(records);
      writer.write(records);
      writeFence.complete(records);
      log.info("Successfully wrote {} records.", recordsCount);
    } catch (JdbcWriteFenceException fence) {
      writer.closeQuietly();
      throw fence;
    } catch (TableAlterOrCreateException tace) {
      checkFenceBeforeRecovery(records);
      if (reporter != null) {
        unrollAndRetry(records);
      } else {
        log.error(tace.toString());
        throw tace;
      }
    } catch (SQLException sqle) {
      SQLException loggedException = redactSensitiveDataIfEnabled(sqle);
      log.warn(
          "Write of {} records failed, remainingRetries={}",
          records.size(),
          remainingRetries,
          loggedException
      );
      int totalExceptions = 0;
      for (Throwable e :sqle) {
        totalExceptions++;
      }
      SQLException sqlAllMessagesException = getAllMessagesException(loggedException);
      if (remainingRetries > 0) {
        checkFenceBeforeRecovery(records);
        writer.closeQuietly();
        checkFenceBeforeRecovery(records);
        initWriter();
        checkFenceBeforeRecovery(records);
        remainingRetries--;
        context.timeout(config.retryBackoffMs);
        log.debug(sqlAllMessagesException.toString());
        throw new RetriableException(sqlAllMessagesException);
      } else {
        if (reporter != null) {
          checkFenceBeforeRecovery(records);
          unrollAndRetry(records);
        } else {
          log.error(
              "Failing task after exhausting retries; "
                  + "encountered {} exceptions on last write attempt. "
                  + "For complete details on each exception, please enable DEBUG logging.",
              totalExceptions);
          int exceptionCount = 1;
          for (Throwable e : loggedException) {
            log.debug("Exception {}:", exceptionCount++, e);
          }
          throw new ConnectException(sqlAllMessagesException);
        }
      }
    }
    remainingRetries = config.maxRetries;
  }

  private void unrollAndRetry(Collection<SinkRecord> records) {
    writer.closeQuietly();
    initWriter();
    log.warn("Retrying write operation for {} records.", records.size());
    for (SinkRecord record : records) {
      Collection<SinkRecord> recordAsCollection = Collections.singletonList(record);
      try {
        writeFence.check(recordAsCollection);
        writer.write(recordAsCollection);
        writeFence.complete(recordAsCollection);
      } catch (JdbcWriteFenceException fence) {
        writer.closeQuietly();
        throw fence;
      } catch (TableAlterOrCreateException tace) {
        log.debug(tace.toString());
        checkFenceBeforeRecovery(recordAsCollection);
        reporter.report(record, tace);
        writeFence.complete(recordAsCollection);
        writer.closeQuietly();
      } catch (SQLException sqle) {
        SQLException sqlAllMessagesException =
            getAllMessagesException(redactSensitiveDataIfEnabled(sqle));
        log.debug(sqlAllMessagesException.toString());
        checkFenceBeforeRecovery(recordAsCollection);
        reporter.report(record, sqlAllMessagesException);
        writeFence.complete(recordAsCollection);
        writer.closeQuietly();
      }
    }
  }

  private void checkFenceBeforeRecovery(Collection<SinkRecord> records) {
    try {
      writeFence.check(records);
    } catch (JdbcWriteFenceException fence) {
      writer.closeQuietly();
      throw fence;
    }
  }

  private SQLException getAllMessagesException(SQLException sqle) {
    String sqleAllMessages = "Exception chain:" + System.lineSeparator();
    for (Throwable e : sqle) {
      sqleAllMessages += e + System.lineSeparator();
    }
    SQLException sqlAllMessagesException = new SQLException(sqleAllMessages);
    sqlAllMessagesException.setNextException(sqle);
    return sqlAllMessagesException;
  }

  private SQLException redactSensitiveDataIfEnabled(SQLException exception) {
    return config.trimSensitiveLogsEnabled
        ? LogUtil.redactSensitiveData(exception)
        : exception;
  }

  @Override
  public void flush(Map<TopicPartition, OffsetAndMetadata> map) {
    // Not necessary
  }

  @Override
  public void open(Collection<TopicPartition> partitions) {
    if (writeFence != null) {
      writeFence.open(partitions);
    }
  }

  @Override
  public void close(Collection<TopicPartition> partitions) {
    // Pending first-receipt timestamps deliberately survive close callbacks.
  }

  public void stop() {
    log.info("Stopping task");
    try {
      writer.closeQuietly();
    } finally {
      try {
        if (dialect != null) {
          dialect.close();
        }
      } catch (Throwable t) {
        log.warn("Error while closing the {} dialect: ", dialect.name(), t);
      } finally {
        dialect = null;
      }
    }
  }

  @Override
  public String version() {
    return Version.getVersion();
  }

}
