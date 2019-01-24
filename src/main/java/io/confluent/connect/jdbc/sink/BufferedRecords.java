/*
 * Copyright 2016 Confluent Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.confluent.connect.jdbc.sink;

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

import io.confluent.connect.jdbc.dialect.DatabaseDialect;
import io.confluent.connect.jdbc.dialect.DatabaseDialect.StatementBinder;
import io.confluent.connect.jdbc.sink.metadata.FieldsMetadata;
import io.confluent.connect.jdbc.sink.metadata.SchemaPair;
import io.confluent.connect.jdbc.util.ColumnId;
import io.confluent.connect.jdbc.util.TableId;

public class BufferedRecords {
  private static final Logger log = LoggerFactory.getLogger(BufferedRecords.class);

  private final TableId tableId;
  private final JdbcSinkConfig config;
  private final DatabaseDialect dbDialect;
  private final DbStructure dbStructure;
  private final Connection connection;

  private List<SinkRecord> records = new ArrayList<>();
  private FieldsMetadata fieldsMetadata;
  private PreparedStatement preparedStatement;
  private StatementBinder preparedStatementBinder;

  private Schema keySchema;
  private Schema valueSchema;
  private boolean batchContainsDeletes = false;
  private PreparedStatement deletePreparedStatement;

  public BufferedRecords(
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

  public List<SinkRecord> add(SinkRecord record) throws SQLException {
    final SchemaPair schemaPair = new SchemaPair(
        record.keySchema(),
        record.valueSchema()
    );

    List<SinkRecord> flushed = new ArrayList<>();

    boolean schemaChanged = false;

    if (!Objects.equals(keySchema, record.keySchema())) {
      keySchema = record.keySchema();
      schemaChanged = true;
    }

    if (schemaPair.valueSchema == null) {
      // For deletes, both the value and value schema come in as null.
      // We don't want to treat this as a schema change if key schemas is the same
      // otherwise we flush unnecessarily.
      if (config.deleteEnabled) {
        batchContainsDeletes = true;
      }
    } else if (Objects.equals(valueSchema, record.valueSchema())) {
      if (config.deleteEnabled && batchContainsDeletes) {
        // flush so an insert after a delete of same record isn't lost
        flushed.addAll(flush());
      }
    } else {
      valueSchema = record.valueSchema();
      schemaChanged = true;
    }

    if (schemaChanged) {
      // Each batch needs to have the same schemas, so get the buffered records out
      flushed.addAll(flush());

      onSchemaChanged(schemaPair);
    }

    records.add(record);
    if (records.size() >= config.batchSize) {
      log.debug("Flushing buffered records after exceeding configured batch size {}.",
          config.batchSize);
      flushed.addAll(flush());
    }

    return flushed;
  }

  private void onSchemaChanged(SchemaPair schemaPair) throws SQLException {
    // re-initialize everything that depends on the record schema
    fieldsMetadata = FieldsMetadata.extract(
        tableId.tableName(),
        config.pkMode,
        config.pkFields,
        config.fieldsWhitelist,
        schemaPair
    );
    dbStructure.createOrAmendIfNecessary(
        config,
        connection,
        tableId,
        fieldsMetadata
    );

    final String sql = getInsertSql();
    final String deleteSql = getDeleteSql();
    log.debug(
        "{} sql: {}, DELETE sql: {}",
        config.insertMode,
        sql,
        deleteSql
    );
    close();
    preparedStatement = connection.prepareStatement(sql);
    deletePreparedStatement = config.deleteEnabled ? connection.prepareStatement(deleteSql) : null;
    preparedStatementBinder = dbDialect.statementBinder(
        preparedStatement,
        deletePreparedStatement,
        config.pkMode,
        schemaPair,
        fieldsMetadata,
        config.insertMode,
        config
    );
  }

  public List<SinkRecord> flush() throws SQLException {
    if (records.isEmpty()) {
      return new ArrayList<>();
    }
    for (SinkRecord record : records) {
      preparedStatementBinder.bindRecord(record);
    }
    int totalUpdateCount = 0;
    boolean successNoInfo = false;
    for (int updateCount : preparedStatement.executeBatch()) {
      if (updateCount == Statement.SUCCESS_NO_INFO) {
        successNoInfo = true;
        continue;
      }
      totalUpdateCount += updateCount;
    }
    int totalDeleteCount = 0;
    if (deletePreparedStatement != null) {
      for (int updateCount : deletePreparedStatement.executeBatch()) {
        if (updateCount != Statement.SUCCESS_NO_INFO) {
          totalDeleteCount += updateCount;
        }
      }
    }

    checkAffectedRowCount(totalUpdateCount + totalDeleteCount, successNoInfo);

    final List<SinkRecord> flushedRecords = records;
    records = new ArrayList<>();
    batchContainsDeletes = false;
    return flushedRecords;
  }

  private void checkAffectedRowCount(int totalCount, boolean successNoInfo) {
    if (totalCount != records.size() && !successNoInfo) {
      switch (config.insertMode) {
        case INSERT:
          throw new ConnectException(String.format(
              "Row count (%d) did not sum up to total number of records inserted/deleted (%d)",
              totalCount,
              records.size()
          ));
        case UPSERT:
        case UPDATE:
          log.debug(
              "{}/deleted records:{} resulting in in totalUpdateCount:{}",
              config.insertMode,
              records.size(),
              totalCount
          );
          break;
        default:
          throw new ConnectException("Unknown insert mode: " + config.insertMode);
      }
    }
    if (successNoInfo) {
      log.info(
          "{} records:{} , but no count of the number of rows it affected is available",
          config.insertMode,
          records.size()
      );
    }
  }

  public void close() throws SQLException {
    log.info("Closing BufferedRecords with preparedStatement: {} deletePreparedStatement: {}",
        preparedStatement,
        deletePreparedStatement);
    if (preparedStatement != null) {
      preparedStatement.close();
      preparedStatement = null;
    }
    if (deletePreparedStatement != null) {
      deletePreparedStatement.close();
      deletePreparedStatement = null;
    }
  }

  private String getInsertSql() {
    switch (config.insertMode) {
      case INSERT:
        return dbDialect.buildInsertStatement(
            tableId,
            asColumns(fieldsMetadata.keyFieldNames),
            asColumns(fieldsMetadata.nonKeyFieldNames)
        );
      case UPSERT:
        if (fieldsMetadata.keyFieldNames.isEmpty()) {
          throw new ConnectException(String.format(
              "Write to table '%s' in UPSERT mode requires key field names to be known, check the"
              + " primary key configuration",
              tableId
          ));
        }
        try {
          return dbDialect.buildUpsertQueryStatement(
              tableId,
              asColumns(fieldsMetadata.keyFieldNames),
              asColumns(fieldsMetadata.nonKeyFieldNames)
          );
        } catch (UnsupportedOperationException e) {
          throw new ConnectException(String.format(
              "Write to table '%s' in UPSERT mode is not supported with the %s dialect.",
              tableId,
              dbDialect.name()
          ));
        }
      case UPDATE:
        return dbDialect.buildUpdateStatement(
            tableId,
            asColumns(fieldsMetadata.keyFieldNames),
            asColumns(fieldsMetadata.nonKeyFieldNames)
        );
      default:
        throw new ConnectException("Invalid insert mode");
    }
  }

  private String getDeleteSql() {
    String sql = null;
    if (config.deleteEnabled) {
      switch (config.pkMode) {
        case NONE:
        case KAFKA:
        case RECORD_VALUE:
          throw new ConnectException("Deletes are only supported for pk.mode record_key");
        case RECORD_KEY:
          if (fieldsMetadata.keyFieldNames.isEmpty()) {
            throw new ConnectException("Require primary keys to support delete");
          }
          sql = dbDialect.buildDeleteStatement(
              tableId,
              asColumns(fieldsMetadata.keyFieldNames)
          );
          break;
        default:
          break;
      }
    }
    return sql;
  }

  private Collection<ColumnId> asColumns(Collection<String> names) {
    return names.stream()
                .map(name -> new ColumnId(tableId, name))
                .collect(Collectors.toList());
  }
}
