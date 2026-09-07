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
  int remainingRetries;
  private WriteFenceTimeoutException terminalFenceException;

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
    terminalFenceException = null;
    initWriter();
    validateWriteFenceEligibility();
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
    writer = new JdbcDbWriter(config, dialect, dbStructure);
    log.info("JDBC writer initialized");
  }

  @Override
  public void put(Collection<SinkRecord> records) {
    if (terminalFenceException != null) {
      throw terminalFenceException;
    }
    if (records.isEmpty()) {
      return;
    }
    WriteFence fence = WriteFence.start(nanoClock, config.writeFenceTimeoutNanos);
    final SinkRecord first = records.iterator().next();
    final int recordsCount = records.size();
    log.debug(
        "Received {} records. First record kafka coordinates:({}-{}-{}). Writing them to the "
        + "database...",
        recordsCount, first.topic(), first.kafkaPartition(), first.kafkaOffset()
    );
    try {
      writeRecords(records, fence);
      log.info("Successfully wrote {} records.", recordsCount);
    } catch (WriteFenceTimeoutException e) {
      throw latchTerminalFence(e);
    } catch (TableAlterOrCreateException tace) {
      if (reporter != null) {
        checkFence(fence, "after table create or alter failure");
        unrollAndRetry(records, fence);
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
      checkFence(fence, "after SQL write failure");
      if (remainingRetries > 0) {
        writer.closeQuietly();
        checkFence(fence, "after JDBC writer cleanup");
        initWriter();
        checkFence(fence, "after JDBC writer reinitialization");
        remainingRetries--;
        context.timeout(config.retryBackoffMs);
        log.debug(sqlAllMessagesException.toString());
        throw new RetriableException(sqlAllMessagesException);
      } else {
        if (reporter != null) {
          unrollAndRetry(records, fence);
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
    } catch (ConnectException e) {
      checkFence(fence, "after connector write failure");
      throw e;
    }
    remainingRetries = config.maxRetries;
  }

  private void writeRecords(Collection<SinkRecord> records, WriteFence fence)
      throws SQLException, TableAlterOrCreateException {
    if (fence.enabled()) {
      writer.write(records, fence);
    } else {
      writer.write(records);
    }
  }

  private void unrollAndRetry(Collection<SinkRecord> records, WriteFence fence) {
    checkFence(fence, "before unrolled retry cleanup");
    writer.closeQuietly();
    checkFence(fence, "after unrolled retry cleanup");
    initWriter();
    checkFence(fence, "after unrolled retry writer reinitialization");
    log.warn("Retrying write operation for {} records.", records.size());
    for (SinkRecord record : records) {
      try {
        writeRecords(Collections.singletonList(record), fence);
      } catch (WriteFenceTimeoutException e) {
        throw latchTerminalFence(e);
      } catch (TableAlterOrCreateException tace) {
        checkFence(fence, "after unrolled table create or alter failure");
        log.debug(tace.toString());
        reporter.report(record, tace);
        writer.closeQuietly();
        checkFence(fence, "after unrolled table create or alter cleanup");
      } catch (SQLException sqle) {
        SQLException sqlAllMessagesException =
            getAllMessagesException(redactSensitiveDataIfEnabled(sqle));
        checkFence(fence, "after unrolled SQL write failure");
        log.debug(sqlAllMessagesException.toString());
        reporter.report(record, sqlAllMessagesException);
        writer.closeQuietly();
        checkFence(fence, "after unrolled SQL cleanup");
      }
    }
  }

  private void checkFence(WriteFence fence, String boundary) {
    try {
      fence.check(boundary);
    } catch (WriteFenceTimeoutException e) {
      throw latchTerminalFence(e);
    }
  }

  private WriteFenceTimeoutException latchTerminalFence(WriteFenceTimeoutException e) {
    if (terminalFenceException == null) {
      terminalFenceException = e;
      if (writer != null) {
        writer.closeQuietly();
      }
    }
    return terminalFenceException;
  }

  private void validateWriteFenceEligibility() {
    if (config.writeFenceTimeoutNanos == 0) {
      return;
    }
    if (!(dialect instanceof PostgreSqlDatabaseDialect)
        && !(dialect instanceof SqliteDatabaseDialect)) {
      throw new ConfigException(
          JdbcSinkConfig.WRITE_FENCE_TIMEOUT_MS,
          config.writeFenceTimeoutMs,
          "requires PostgreSQL or SQLite dialect"
      );
    }
    validateWriteFenceSchemaSettings();
    if (config.insertMode != JdbcSinkConfig.InsertMode.UPSERT
        && config.insertMode != JdbcSinkConfig.InsertMode.UPDATE) {
      throw new ConfigException(
          JdbcSinkConfig.INSERT_MODE,
          config.insertMode,
          "must be upsert or update when " + JdbcSinkConfig.WRITE_FENCE_TIMEOUT_MS
              + " is enabled"
      );
    }
    if (config.pkMode == JdbcSinkConfig.PrimaryKeyMode.NONE) {
      throw new ConfigException(
          JdbcSinkConfig.PK_MODE,
          config.pkMode,
          "must identify a primary key when " + JdbcSinkConfig.WRITE_FENCE_TIMEOUT_MS
              + " is enabled"
      );
    }
    if (!config.tableTypes.equals(EnumSet.of(TableType.TABLE))) {
      throw new ConfigException(
          JdbcSinkConfig.TABLE_TYPES_CONFIG,
          config.tableTypes,
          "must target ordinary tables when " + JdbcSinkConfig.WRITE_FENCE_TIMEOUT_MS
              + " is enabled"
      );
    }
  }

  private void validateWriteFenceSchemaSettings() {
    if (config.autoCreate) {
      throw new ConfigException(
          JdbcSinkConfig.AUTO_CREATE,
          config.autoCreate,
          "must be false when " + JdbcSinkConfig.WRITE_FENCE_TIMEOUT_MS + " is enabled"
      );
    }
    if (config.autoEvolve) {
      throw new ConfigException(
          JdbcSinkConfig.AUTO_EVOLVE,
          config.autoEvolve,
          "must be false when " + JdbcSinkConfig.WRITE_FENCE_TIMEOUT_MS + " is enabled"
      );
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
