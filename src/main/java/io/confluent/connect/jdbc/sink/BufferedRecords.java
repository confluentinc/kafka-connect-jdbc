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

import io.confluent.connect.jdbc.gpload.GPloadConfigObj;
import io.confluent.connect.jdbc.gpload.YAMLConfig;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.BatchUpdateException;
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

import static java.util.Objects.isNull;
import static java.util.Objects.nonNull;

public class BufferedRecords {
  protected static final Logger log = LoggerFactory.getLogger(BufferedRecords.class);

  protected final TableId tableId;
  protected final JdbcSinkConfig config;
  protected final DatabaseDialect dbDialect;
  protected final DbStructure dbStructure;
  protected final Connection connection;

  protected List<SinkRecord> records = new ArrayList<>();
  protected Schema keySchema;
  protected Schema valueSchema;
  protected RecordValidator recordValidator;
  protected FieldsMetadata fieldsMetadata;
  private PreparedStatement updatePreparedStatement;
  protected PreparedStatement deletePreparedStatement;
  private StatementBinder updateStatementBinder;
  protected StatementBinder deleteStatementBinder;
  protected boolean deletesInBatch = false;

  private long lastFlushTime = 0;

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
    this.recordValidator = RecordValidator.create(config);
  }

  public List<SinkRecord> add(SinkRecord record) throws SQLException, TableAlterOrCreateException {
    recordValidator.validate(record);
    final List<SinkRecord> flushed = new ArrayList<>();

    boolean schemaChanged = false;
    if (!Objects.equals(keySchema, record.keySchema())) {
      keySchema = record.keySchema();
      schemaChanged = true;
    }
    if (isNull(record.valueSchema())) {
      // For deletes, value and optionally value schema come in as null.
      // We don't want to treat this as a schema change if key schemas is the same
      // otherwise we flush unnecessarily.
      if (config.deleteEnabled) {
        deletesInBatch = true;
      }
    } else if (Objects.equals(valueSchema, record.valueSchema())) {
      if (config.deleteEnabled && deletesInBatch) {
        // flush so an insert after a delete of same record isn't lost
        flushed.addAll(flush());
      }
    } else {
      // value schema is not null and has changed. This is a real schema change.
      valueSchema = record.valueSchema();
      schemaChanged = true;
    }
    if (schemaChanged || updateStatementBinder == null) {
      // Each batch needs to have the same schemas, so get the buffered records out
      flushed.addAll(flush());

      // re-initialize everything that depends on the record schema
      final SchemaPair schemaPair = new SchemaPair(
          record.keySchema(),
          record.valueSchema()
      );
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
      final String insertSql = getInsertSql();
      final String deleteSql = getDeleteSql();
      log.debug(
          "{} sql: {} deleteSql: {} meta: {}",
          config.insertMode,
          insertSql,
          deleteSql,
          fieldsMetadata
      );
      close();
      updatePreparedStatement = dbDialect.createPreparedStatement(connection, insertSql);
      updateStatementBinder = dbDialect.statementBinder(
          updatePreparedStatement,
          config.pkMode,
          schemaPair,
          fieldsMetadata,
          dbStructure.tableDefinition(connection, tableId),
          config.insertMode
      );
      if (config.deleteEnabled && nonNull(deleteSql)) {
        deletePreparedStatement = dbDialect.createPreparedStatement(connection, deleteSql);
        deleteStatementBinder = dbDialect.statementBinder(
            deletePreparedStatement,
            config.pkMode,
            schemaPair,
            fieldsMetadata,
            dbStructure.tableDefinition(connection, tableId),
            config.insertMode
        );
      }
    }
    
    // set deletesInBatch if schema value is not null
    if (isNull(record.value()) && config.deleteEnabled) {
      deletesInBatch = true;
    }

    records.add(record);

    if (records.size() >= config.batchSize) {
      flushed.addAll(flush());
    }
    return flushed;
  }

  public List<SinkRecord> flush() throws SQLException { // work to be done here - bach insert
    if (records.isEmpty()) {
      log.debug("Records is empty");
      return new ArrayList<>();
    }
    log.debug("Flushing {} buffered records", records.size());
    for (SinkRecord record : records) {
      if (isNull(record.value()) && nonNull(deleteStatementBinder)) {
        deleteStatementBinder.bindRecord(record);
      } else {
        updateStatementBinder.bindRecord(record);
      }
    }
    executeUpdates();
    executeDeletes();

    final List<SinkRecord> flushedRecords = records;
    records = new ArrayList<>();
    deletesInBatch = false;
    return flushedRecords;
  }

  protected void executeUpdates() throws SQLException {

    if(config.batchInsertMode == JdbcSinkConfig.BatchInsertMode.GPLOAD) {
      executeUpdatesUsingGpLoad();
    } else {

      int[] batchStatus = updatePreparedStatement.executeBatch();
      for (int updateCount : batchStatus) {
        if (updateCount == Statement.EXECUTE_FAILED) {
          throw new BatchUpdateException(
                  "Execution failed for part of the batch update", batchStatus);
        }
      }
    }
  }

  protected void executeUpdatesUsingGpLoad() {
    // TODO add check while configuring that dialct should be Postgress as it also handle

    try {
//    PgPreparedStatement preparedStatement = (PgPreparedStatement) updatePreparedStatement;
      GPloadConfigObj gPloadConfig = new GPloadConfigObj();
      gPloadConfig.setUrl(config.connectionUrl);
      gPloadConfig.setTable(tableId.tableName());





      //CSVWriterUtil.writeDataToCSV();


      String dataFile = "";


      gPloadConfig.setFormat("csv");
      gPloadConfig.setInput(dataFile);



      YAMLConfig yamlConfig = new YAMLConfig(gPloadConfig);
      yamlConfig.create();




    }catch (Exception e){
      e.printStackTrace();
    }

  }

  protected void executeDeletes() throws SQLException {
    if (nonNull(deletePreparedStatement)) {
      int[] batchStatus = deletePreparedStatement.executeBatch();
      for (int updateCount : batchStatus) {
        if (updateCount == Statement.EXECUTE_FAILED) {
          throw new BatchUpdateException(
                  "Execution failed for part of the batch delete", batchStatus);
        }
      }
    }
  }

  public void close() throws SQLException {
    log.debug(
        "Closing BufferedRecords with updatePreparedStatement: {} deletePreparedStatement: {}",
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

  protected String getInsertSql() throws SQLException {
    switch (config.insertMode) {
      case INSERT:case MERGE:
        return dbDialect.buildInsertStatement(
            tableId,
            asColumns(fieldsMetadata.keyFieldNames),
            asColumns(fieldsMetadata.nonKeyFieldNames),
            dbStructure.tableDefinition(connection, tableId)
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
              asColumns(fieldsMetadata.nonKeyFieldNames),
              dbStructure.tableDefinition(connection, tableId)
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
            asColumns(fieldsMetadata.nonKeyFieldNames),
            dbStructure.tableDefinition(connection, tableId)
        );
      default:
        throw new ConnectException("Invalid insert mode");
    }
  }

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

  protected Collection<ColumnId> asColumns(Collection<String> names) {
    return names.stream()
        .map(name -> new ColumnId(tableId, name))
        .collect(Collectors.toList());
  }

  public long getLastFlushTime() {
    return lastFlushTime;
  }

  public void setLastFlushTime(long lastFlushTime) {
    this.lastFlushTime = lastFlushTime;
  }
}
