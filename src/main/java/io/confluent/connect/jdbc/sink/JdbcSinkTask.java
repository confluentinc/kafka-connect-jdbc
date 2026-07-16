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
import java.util.Map;

import io.confluent.connect.jdbc.dialect.DatabaseDialect;
import io.confluent.connect.jdbc.dialect.DatabaseDialects;
import io.confluent.connect.jdbc.util.Version;

public class JdbcSinkTask extends SinkTask {
  private static final Logger log = LoggerFactory.getLogger(JdbcSinkTask.class);

  ErrantRecordReporter reporter;
  DatabaseDialect dialect;
  JdbcSinkConfig config;
  JdbcDbWriter writer;
  int remainingRetries;

  boolean shouldSanitizeSensitiveLogs;

  @Override
  public void start(final Map<String, String> props) {
    log.info("Starting JDBC Sink task");
    config = new JdbcSinkConfig(props);
    initWriter();
    remainingRetries = config.maxRetries;
    shouldSanitizeSensitiveLogs = config.trimSensitiveLogsEnabled;
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
    if (records.isEmpty()) {
      return;
    }
    final SinkRecord first = records.iterator().next();
    final int recordsCount = records.size();
    log.debug(
        "Received {} records. First record kafka coordinates:({}-{}-{}). Writing them to the "
        + "database...",
        recordsCount, first.topic(), first.kafkaPartition(), first.kafkaOffset()
    );
    try {
      writer.write(records);
      log.info("Successfully wrote {} records.", recordsCount);
    } catch (TableAlterOrCreateException tace) {
      if (reporter != null) {
        unrollAndRetry(records);
      } else {
        log.error(tace.toString());
        throw tace;
      }
    } catch (SQLException sqle) {
      SQLException sanitizedException = sanitize(sqle);
      log.warn(
          "Write of {} records failed, remainingRetries={}",
          records.size(),
          remainingRetries,
          sanitizedException
      );
      int totalExceptions = 0;
      for (Throwable exception : sqle) {
        totalExceptions++;
      }
      SQLException allMessagesException = getAllMessagesException(sanitizedException);
      if (remainingRetries > 0) {
        writer.closeQuietly();
        initWriter();
        remainingRetries--;
        context.timeout(config.retryBackoffMs);
        log.debug(allMessagesException.toString());
        throw new RetriableException(allMessagesException);
      } else {
        if (reporter != null) {
          unrollAndRetry(records);
        } else {
          log.error(
              "Failing task after exhausting retries; "
                  + "encountered {} exceptions on last write attempt. "
                  + "For complete details on each exception, please enable DEBUG logging.",
              totalExceptions);
          int exceptionCount = 1;
          for (Throwable exception : sanitizedException) {
            log.debug("Exception {}:", exceptionCount++, exception);
          }
          throw new ConnectException(allMessagesException);
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
      try {
        writer.write(Collections.singletonList(record));
      } catch (TableAlterOrCreateException tace) {
        log.debug(tace.toString());
        reporter.report(record, tace);
        writer.closeQuietly();
      } catch (SQLException sqle) {
        SQLException sanitizedException = sanitize(sqle);
        SQLException allMessagesException = getAllMessagesException(sanitizedException);
        log.debug(allMessagesException.toString());
        reporter.report(record, allMessagesException);
        writer.closeQuietly();
      }
    }
  }

  private SQLException sanitize(SQLException exception) {
    return shouldSanitizeSensitiveLogs
        ? LogUtil.sanitizeSensitiveData(exception)
        : exception;
  }

  private SQLException getAllMessagesException(SQLException exception) {
    StringBuilder allMessages = new StringBuilder("Exception chain:")
        .append(System.lineSeparator());
    for (Throwable current : exception) {
      allMessages.append(current).append(System.lineSeparator());
    }
    SQLException allMessagesException = new SQLException(allMessages.toString());
    allMessagesException.setNextException(exception);
    return allMessagesException;
  }

  @Override
  public void flush(Map<TopicPartition, OffsetAndMetadata> map) {
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
