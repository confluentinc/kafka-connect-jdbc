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
import java.util.List;
import java.util.Objects;

import io.confluent.connect.jdbc.sink.dialect.DbDialect;
import io.confluent.connect.jdbc.sink.metadata.FieldsMetadata;

import static io.confluent.connect.jdbc.sink.JdbcSinkConfig.InsertMode.INSERT;

public class BufferedRecords {
  private static final Logger log = LoggerFactory.getLogger(BufferedRecords.class);

  private final String tableName;
  private final JdbcSinkConfig config;
  private final DbDialect dbDialect;
  private final DbStructure dbStructure;
  private final Connection connection;

  private List<SinkRecord> records = new ArrayList<>();
  private Schema keySchema;
  private Schema valueSchema;
  private FieldsMetadata fieldsMetadata;
  private PreparedStatement updatePreparedStatement;
  private PreparedStatement deletePreparedStatement;
  private PreparedStatementsBinder preparedStatementsBinder;
  private boolean deletesInBatch = false;

  public BufferedRecords(JdbcSinkConfig config, String tableName, DbDialect dbDialect, DbStructure dbStructure, Connection connection) {
    this.tableName = tableName;
    this.config = config;
    this.dbDialect = dbDialect;
    this.dbStructure = dbStructure;
    this.connection = connection;
    preparedStatementsBinder = new PreparedStatementsBinder();
  }

  public List<SinkRecord> add(SinkRecord record) throws SQLException {
    final List<SinkRecord> flushed = new ArrayList<>();
    boolean schemaChanged = false;
    if (!Objects.equals(keySchema, record.keySchema())) {
      keySchema = record.keySchema();
      schemaChanged = true;
    }
    if (record.valueSchema() == null) {
      // For deletes, both the value and value schema come in as null.
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

    if (schemaChanged) {
      // Each batch needs to have the same schemas, so get the buffered records out
      flushed.addAll(flush());

      // re-initialize everything that depends on the record schema
      fieldsMetadata = FieldsMetadata.extract(tableName, config.pkMode, config.pkFields, config.fieldsWhitelist, record.keySchema(), record.valueSchema());
      dbStructure.createOrAmendIfNecessary(config, connection, tableName, fieldsMetadata);
      final String insertSql = getInsertSql();
      final String deleteSql = getDeleteSql();
      log.debug("{} sql: {} deleteSql: {} meta: {}", config.insertMode, insertSql, deleteSql, fieldsMetadata);
      close();
      updatePreparedStatement = connection.prepareStatement(insertSql);
      if (config.deleteEnabled && deleteSql != null) {
        deletePreparedStatement = connection.prepareStatement(deleteSql);
      }
    }
    records.add(record);
    if (records.size() >= config.batchSize) {
      flushed.addAll(flush());
    }
    return flushed;
  }

  public List<SinkRecord> flush() throws SQLException {
    if (records.isEmpty()) {
      return new ArrayList<>();
    }
    for (SinkRecord record : records) {
      preparedStatementsBinder.bindRecord(config, updatePreparedStatement, deletePreparedStatement, fieldsMetadata, record);
    }
    int totalUpdateCount = 0;
    boolean successNoInfo = false;
    for (int updateCount : updatePreparedStatement.executeBatch()) {
      if (updateCount == Statement.SUCCESS_NO_INFO) {
        successNoInfo = true;
      } else {
        totalUpdateCount += updateCount;
      }
    }
    int totalDeleteCount = 0;
    if (deletePreparedStatement != null) {
      for (int updateCount : deletePreparedStatement.executeBatch()) {
        if (updateCount != Statement.SUCCESS_NO_INFO) {
          totalDeleteCount += updateCount;
        }
      }
    }

    final int expectedCount = updateRecordCount();
    log.trace("{} records:{} resulting in totalUpdateCount:{} totalDeleteCount:{}",
        config.insertMode, records.size(), totalUpdateCount, totalDeleteCount);
    if (totalUpdateCount != expectedCount && !successNoInfo && config.insertMode == INSERT) {
      throw new ConnectException(String.format("Update count (%d) did not sum up to total number of records inserted (%d)",
                                               totalUpdateCount, expectedCount));
    }
    if (successNoInfo) {
      log.info("{} records:{} , but no count of the number of rows it affected is available",
          config.insertMode, records.size());
    }

    final List<SinkRecord> flushedRecords = records;
    records = new ArrayList<>();
    deletesInBatch = false;
    return flushedRecords;
  }

  private int updateRecordCount() {
    int count = 0;
    for (SinkRecord record : records) {
      // ignore deletes
      if (record.value() != null || !config.deleteEnabled) {
        count++;
      }
    }
    return count;
  }

  public void close() throws SQLException {
    if (updatePreparedStatement != null) {
      updatePreparedStatement.close();
      updatePreparedStatement = null;
    }
    if (deletePreparedStatement != null) {
      deletePreparedStatement.close();
      deletePreparedStatement = null;
    }
  }

  private String getInsertSql() {
    switch (config.insertMode) {
      case INSERT:
        return dbDialect.getInsert(tableName, fieldsMetadata.keyFieldNames, fieldsMetadata.nonKeyFieldNames);
      case UPSERT:
        if (fieldsMetadata.keyFieldNames.isEmpty()) {
          throw new ConnectException(String.format(
              "Write to table '%s' in UPSERT mode requires key field names to be known, check the primary key configuration", tableName
          ));
        }
        return dbDialect.getUpsertQuery(tableName, fieldsMetadata.keyFieldNames, fieldsMetadata.nonKeyFieldNames);
      case UPDATE:
        return dbDialect.getUpdate(tableName, fieldsMetadata.keyFieldNames, fieldsMetadata.nonKeyFieldNames);
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
          sql = dbDialect.getDelete(tableName, fieldsMetadata.keyFieldNames);
          break;
      }
    }
    return sql;
  }


}
