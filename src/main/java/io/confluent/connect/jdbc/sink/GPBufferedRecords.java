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

import io.confluent.connect.jdbc.dialect.DatabaseDialect;
import io.confluent.connect.jdbc.sink.metadata.FieldsMetadata;
import io.confluent.connect.jdbc.sink.metadata.SchemaPair;
import io.confluent.connect.jdbc.util.TableId;
import org.apache.kafka.connect.sink.SinkRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.util.*;

import static java.util.Objects.isNull;
import static java.util.Objects.nonNull;

public class GPBufferedRecords extends BufferedRecords{
  private static final Logger log = LoggerFactory.getLogger(GPBufferedRecords.class);
  private GPBinder updateStatementBinder;
  public GPBufferedRecords(JdbcSinkConfig config, TableId tableId, DatabaseDialect dbDialect, DbStructure dbStructure, Connection connection) {
    super(config, tableId, dbDialect, dbStructure, connection);
  }


  @Override
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
      final String insertSql =  "via gpload";//  getInsertSql();
      final String deleteSql = getDeleteSql();
      log.debug(
              "{} sql: {} deleteSql: {} meta: {}",
              config.insertMode,
              insertSql,
              deleteSql,
              fieldsMetadata
      );
      close();

      updateStatementBinder = new GPBinder(dbDialect,
              config.pkMode,
              schemaPair,
              fieldsMetadata,
              dbStructure.tableDefinition(connection, tableId),
              config.insertMode, config);

      if (config.deleteEnabled && nonNull(deleteSql)) {
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

  protected void executeUpdates()  {
      updateStatementBinder.flush();
  }



}
