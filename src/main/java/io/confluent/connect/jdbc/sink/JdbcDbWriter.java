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

import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.SinkRecord;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import io.confluent.connect.jdbc.dialect.DatabaseDialect;
import io.confluent.connect.jdbc.util.CachedConnectionProvider;
import io.confluent.connect.jdbc.util.TableId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class JdbcDbWriter {
  private static final Logger log = LoggerFactory.getLogger(JdbcDbWriter.class);

  private final JdbcSinkConfig config;
  private final DatabaseDialect dbDialect;
  private final DbStructure dbStructure;
  final CachedConnectionProvider cachedConnectionProvider;

  JdbcDbWriter(final JdbcSinkConfig config, DatabaseDialect dbDialect, DbStructure dbStructure) {
    this.config = config;
    this.dbDialect = dbDialect;
    this.dbStructure = dbStructure;

    this.cachedConnectionProvider = connectionProvider(
        config.connectionAttempts,
        config.connectionBackoffMs
    );
  }

  protected CachedConnectionProvider connectionProvider(int maxConnAttempts, long retryBackoff) {
    return new CachedConnectionProvider(this.dbDialect, maxConnAttempts, retryBackoff) {
      @Override
      protected void onConnect(final Connection connection) throws SQLException {
        log.info("JdbcDbWriter Connected");
        connection.setAutoCommit(false);
      }
    };
  }

  void write(final Collection<SinkRecord> records)
      throws SQLException, TableAlterOrCreateException {
    final Connection connection = cachedConnectionProvider.getConnection();
    String schemaName = getSchemaSafe(connection).orElse(null);
    String catalogName = getCatalogSafe(connection).orElse(null);
    try {
      final Map<TableId, BufferedRecords> bufferByTable = new HashMap<>();
      for (SinkRecord record : records) {
        final TableId tableId = destinationTable(record.topic(), schemaName, catalogName);
        BufferedRecords buffer = bufferByTable.get(tableId);
        log.info("The table ID for the buffer is: { }" + tableId);
        if (buffer == null) {
          buffer = new BufferedRecords(config, tableId, dbDialect, dbStructure, connection);
          bufferByTable.put(tableId, buffer);
        }
        log.info("Adding the record to the buffer: { }" + record);
        buffer.add(record);
        log.info("The record added in the buffer: { }" + record);
      }
      for (Map.Entry<TableId, BufferedRecords> entry : bufferByTable.entrySet()) {
        TableId tableId = entry.getKey();
        BufferedRecords buffer = entry.getValue();
        log.debug("Flushing records in JDBC Writer for table ID: {}", tableId);
        buffer.flush();
        buffer.close();
      }
      log.trace("Committing transaction");
      connection.commit();
    } catch (SQLException | TableAlterOrCreateException e) {
      log.error("Error during write operation. Attempting rollback.", e);
      try {
        connection.rollback();
        log.info("Successfully rolled back transaction");
      } catch (SQLException sqle) {
        log.error("Failed to rollback transaction", sqle);
        e.addSuppressed(sqle);
      } finally {
        throw e;
      }
    }
    log.info("Completed write operation for {} records to the database", records.size());
  }

  void closeQuietly() {
    cachedConnectionProvider.close();
  }

  TableId destinationTable(String topic, String schemaName, String catalogName) {
    final String tableName = config.tableNameFormat.replace("${topic}", topic);
    if (tableName.isEmpty()) {
      throw new ConnectException(String.format(
          "Destination table name for topic '%s' is empty using the format string '%s'",
          topic,
          config.tableNameFormat
      ));
    }
    TableId parsedTableId = dbDialect.parseTableIdentifier(tableName);
    String finalCatalogName =
            (parsedTableId.catalogName() != null) ? parsedTableId.catalogName() : catalogName;
    String finalSchemaName =
            (parsedTableId.schemaName() != null) ? parsedTableId.schemaName() : schemaName;


    return new TableId(finalCatalogName, finalSchemaName, parsedTableId.tableName());
  }

  private Optional<String> getSchemaSafe(Connection connection) {
    try {
      return Optional.ofNullable(connection.getSchema());
    } catch (AbstractMethodError | SQLException e) {
      log.warn("Failed to get schema: {}", e.getMessage());
      return Optional.empty();
    }
  }

  private Optional<String> getCatalogSafe(Connection connection) {
    try {
      return Optional.ofNullable(connection.getCatalog());
    } catch (AbstractMethodError | SQLException e) {
      log.warn("Failed to get catalog: {}", e.getMessage());
      return Optional.empty();
    }
  }
}
