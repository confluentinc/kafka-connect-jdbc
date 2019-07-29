/*
 * Copyright 2018 Confluent Inc.
 * Copyright 2019 Nike, Inc.
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

package io.confluent.connect.jdbc.sink.bufferedrecords;

import io.confluent.connect.jdbc.dialect.DatabaseDialect;
import io.confluent.connect.jdbc.sink.DbStructure;
import io.confluent.connect.jdbc.sink.JdbcSinkConfig;
import io.confluent.connect.jdbc.sink.metadata.FieldsMetadata;
import io.confluent.connect.jdbc.sink.metadata.SchemaPair;
import io.confluent.connect.jdbc.util.ColumnId;
import io.confluent.connect.jdbc.util.TableId;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

import static java.util.Objects.isNull;
import static java.util.Objects.nonNull;

/**
 * Contains common logic for several {@link BufferedRecords} components.
 */
public abstract class BaseBufferedRecords implements BufferedRecords {
  protected final Logger log = LoggerFactory.getLogger(getClass());

  protected final TableId tableId;
  protected final JdbcSinkConfig config;
  protected final DatabaseDialect dbDialect;
  protected final DbStructure dbStructure;
  protected final Connection connection;

  protected List<SinkRecord> records = new ArrayList<>();
  protected Schema keySchema;
  protected Schema valueSchema;
  protected FieldsMetadata fieldsMetadata;
  protected PreparedStatement updatePreparedStatement;
  protected PreparedStatement deletePreparedStatement;
  protected DatabaseDialect.StatementBinder updateStatementBinder;
  protected DatabaseDialect.StatementBinder deleteStatementBinder;
  protected boolean deletesInBatch = false;

  public BaseBufferedRecords(
      JdbcSinkConfig config,
      TableId tableId,
      DatabaseDialect dbDialect,
      DbStructure dbStructure,
      Connection connection
  ) {
    this.tableId = tableId;
    this.config = config;
    this.dbDialect = dbDialect;
    this.dbStructure = dbStructure;
    this.connection = connection;
  }

  public void close() throws SQLException {
    log.info(
        "Closing BatchedBufferedRecords with updatePreparedStatement:"
            + " {} deletePreparedStatement: {}",
        updatePreparedStatement,
        deletePreparedStatement
    );
    if (nonNull(updatePreparedStatement)) {
      updatePreparedStatement.close();
      updatePreparedStatement = null;
    }
    if (nonNull(deletePreparedStatement)) {
      deletePreparedStatement.close();
      deletePreparedStatement = null;
    }
  }

  /**
   * Determine if SinkRecord key or value schema has changed.
   * @param record current SinkRecord.
   * @return true if SinkRecord's key or value schema as changed, otherwise false.
   */
  protected boolean hasSchemaChanged(SinkRecord record) {
    boolean schemaChanged = false;

    if (!Objects.equals(keySchema, record.keySchema())) {
      schemaChanged = true;
    }

    if (nonNull(record.valueSchema()) && !Objects.equals(valueSchema, record.valueSchema())) {
      schemaChanged = true;
    }

    return schemaChanged;
  }

  /**
   * Determine if pending deletes should be flushed.
   * @param record current SinkRecord.
   * @return true if pending deletes should be flushed, otherwise false.
   */
  protected boolean shouldFlushPendingDeletes(SinkRecord record) {
    if (!config.deleteEnabled) {
      return false;
    }
    if (isNull(record.valueSchema())) {
      // For deletes, both the value and value schema come in as null.
      // We don't want to treat this as a schema change if key schemas is the same
      // otherwise we flush unnecessarily.
      deletesInBatch = true;
    } else if (deletesInBatch && Objects.equals(valueSchema, record.valueSchema())) {
      // flush so an insert after a delete of same record isn't lost
      return true;
    }
    return false;
  }

  /**
   * Initializes JDBC prepared statement for deletes.
   *
   * @param deleteSql delete sql query.
   * @param schemaPair SinkRecord key/value schema pair.
   * @throws SQLException if an error occurs with JDBC.
   */
  protected void initDeletePreparedStatement(String deleteSql, SchemaPair schemaPair)
      throws SQLException {
    deletePreparedStatement = dbDialect.createPreparedStatement(connection, deleteSql);
    deleteStatementBinder = dbDialect.statementBinder(
        deletePreparedStatement,
        config.pkMode,
        schemaPair,
        fieldsMetadata,
        config.insertMode
    );
  }

  /**
   * Initializes JDBC prepared statement for inserts.
   *
   * @param insertSql insert sql query.
   * @param schemaPair SinkRecord key/value schema pair.
   * @throws SQLException if an error occurs with JDBC.
   */
  protected void initUpdatePreparedStatement(String insertSql, SchemaPair schemaPair)
      throws SQLException {
    updatePreparedStatement = dbDialect.createPreparedStatement(connection, insertSql);
    updateStatementBinder = dbDialect.statementBinder(
        updatePreparedStatement,
        config.pkMode,
        schemaPair,
        fieldsMetadata,
        config.insertMode
    );
  }

  /**
   * Execute delete prepared statement.
   * @return number of deleted rows.
   * @throws SQLException if an error occurs with JDBC.
   */
  protected long executeDeletes() throws SQLException {
    long totalDeleteCount = 0;
    if (nonNull(deletePreparedStatement)) {
      for (int updateCount : deletePreparedStatement.executeBatch()) {
        if (updateCount != Statement.SUCCESS_NO_INFO) {
          totalDeleteCount += updateCount;
        }
      }
    }
    return totalDeleteCount;
  }

  /**
   * Generates insert sql query statement.
   *
   * @param insertMode insert mode type.
   * @param insertIntoTable target table to insert into.
   * @return generated insert sql query statement.
   */
  protected String getInsertSql(JdbcSinkConfig.InsertMode insertMode, TableId insertIntoTable) {
    switch (insertMode) {
      case INSERT:
        return dbDialect.buildInsertStatement(
            insertIntoTable,
            asColumns(fieldsMetadata.keyFieldNames),
            asColumns(fieldsMetadata.nonKeyFieldNames)
        );
      case UPSERT:
        if (fieldsMetadata.keyFieldNames.isEmpty()) {
          throw new ConnectException(String.format(
              "Write to table '%s' in UPSERT mode requires key field names to be known, "
                  + "check the primary key configuration",
              insertIntoTable
          ));
        }
        try {
          return dbDialect.buildUpsertQueryStatement(
              insertIntoTable,
              asColumns(fieldsMetadata.keyFieldNames),
              asColumns(fieldsMetadata.nonKeyFieldNames)
          );
        } catch (UnsupportedOperationException e) {
          throw new ConnectException(String.format(
              "Write to table '%s' in UPSERT mode is not supported with the %s dialect.",
              insertIntoTable,
              dbDialect.name()
          ));
        }
      case UPDATE:
        return dbDialect.buildUpdateStatement(
            insertIntoTable,
            asColumns(fieldsMetadata.keyFieldNames),
            asColumns(fieldsMetadata.nonKeyFieldNames)
        );
      default:
        throw new ConnectException("Invalid insert mode");
    }
  }

  /**
   * Generates delete sql query statement.
   *
   * @return generated delete sql query statement.
   */
  protected String getDeleteSql() {
    String sql = null;
    if (config.deleteEnabled) {
      switch (config.pkMode) {
        case RECORD_KEY:
          if (fieldsMetadata.keyFieldNames.isEmpty()) {
            throw new ConnectException("Require primary keys to support delete");
          }
          try {
            sql = dbDialect.buildDeleteStatement(
                tableId,
                asColumns(fieldsMetadata.keyFieldNames)
            );
          } catch (UnsupportedOperationException e) {
            throw new ConnectException(String.format(
                "Deletes to table '%s' are not supported with the %s dialect.",
                tableId,
                dbDialect.name()
            ));
          }
          break;

        default:
          throw new ConnectException("Deletes are only supported for pk.mode record_key");
      }
    }
    return sql;
  }

  /**
   * Converts collection of names to collection of ColumnIds
   * @param names collection of names.
   * @return collection of columnIds.
   */
  protected Collection<ColumnId> asColumns(Collection<String> names) {
    return names.stream()
        .map(name -> new ColumnId(tableId, name))
        .collect(Collectors.toList());
  }
}
