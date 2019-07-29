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

package io.confluent.connect.jdbc.sink.bufferedrecords;

import io.confluent.connect.jdbc.dialect.DatabaseDialect;
import io.confluent.connect.jdbc.sink.DbStructure;
import io.confluent.connect.jdbc.sink.JdbcSinkConfig;
import io.confluent.connect.jdbc.sink.metadata.FieldsMetadata;
import io.confluent.connect.jdbc.sink.metadata.SchemaPair;
import io.confluent.connect.jdbc.util.TableId;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.SinkRecord;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import static io.confluent.connect.jdbc.sink.JdbcSinkConfig.InsertMode.INSERT;
import static java.util.Objects.isNull;
import static java.util.Objects.nonNull;

/**
 * Batched buffered records perform JDBC transactions through batching database operations.
 *
 * <p>As SinkRecords are added, their schema is examined to detect changes in structure and the
 * appropriate database operation to perform.
 *
 * <p>Operations are divided into two types; deletes and non-deletes (insert, update, and upsert).
 * SinkRecords of both operation types will be batched along side each other until certain
 * criteria is met in which case a flush is automatically performed an processing begins again.
 *
 * <p>A schema change will flush all pending records then attempt to alter the target table
 * structure (if set in the sink's config).
 *
 * <p>A record without a value (set to null) will be flagged for deletion. Additional records
 * added that match deletion criteria will to accumulate until a non deleted record is detected,
 * in which case all previous processed records (all pending deletes and any non-deletes
 * preceding them) are flushed.
 *
 * <p>A flush is also triggered when enough SinkRecords have been added (when the batch limit
 * has been reached).
 */
public class BatchedBufferedRecords extends BaseBufferedRecords {

  public BatchedBufferedRecords(
      JdbcSinkConfig config,
      TableId tableId,
      DatabaseDialect dbDialect,
      DbStructure dbStructure,
      Connection connection) {
    super(config, tableId, dbDialect, dbStructure, connection);
  }

  public List<SinkRecord> add(SinkRecord record) throws SQLException {
    final List<SinkRecord> flushed = new ArrayList<>();

    boolean schemaChanged = hasSchemaChanged(record);
    boolean shouldFlushDeletes = shouldFlushPendingDeletes(record);

    if (schemaChanged || shouldFlushDeletes) {
      flushed.addAll(flush());
    }

    if (schemaChanged) {
      keySchema = record.keySchema();
      valueSchema = record.valueSchema();

      // re-initialize everything that depends on the record schema
      final SchemaPair schemaPair = new SchemaPair(
          keySchema,
          valueSchema
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
      final String insertSql = getInsertSql(config.insertMode, tableId);
      final String deleteSql = getDeleteSql();
      log.debug(
              "{} sql: {} deleteSql: {} meta: {}",
              config.insertMode,
              insertSql,
              deleteSql,
              fieldsMetadata
      );
      close();

      initUpdatePreparedStatement(insertSql, schemaPair);
      if (config.deleteEnabled && nonNull(deleteSql)) {
        initDeletePreparedStatement(deleteSql, schemaPair);
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

    final long expectedCount = updateRecordCount();
    Optional<Long> totalUpdateCount;
    if (expectedCount > 0) {
      totalUpdateCount = executeUpdates();
    } else {
      totalUpdateCount = Optional.empty();
    }

    long totalDeleteCount = executeDeletes();

    log.trace("{} records:{} resulting in totalUpdateCount:{} totalDeleteCount:{}",
            config.insertMode, records.size(), totalUpdateCount, totalDeleteCount
    );
    if (totalUpdateCount.filter(total -> total != expectedCount).isPresent()
            && config.insertMode == INSERT) {
      throw new ConnectException(String.format(
              "Update count (%d) did not sum up to total number of records inserted (%d)",
              totalUpdateCount.get(),
              expectedCount
      ));
    }
    if (!totalUpdateCount.isPresent()) {
      log.info(
              "{} records:{} , but no count of the number of rows it affected is available",
              config.insertMode,
              records.size()
      );
    }

    final List<SinkRecord> flushedRecords = records;
    records = new ArrayList<>();
    deletesInBatch = false;
    return flushedRecords;
  }

  /**
   * @return an optional count of all updated rows or an empty optional if no info is available
   */
  private Optional<Long> executeUpdates() throws SQLException {
    Optional<Long> count = Optional.empty();
    for (int updateCount : updatePreparedStatement.executeBatch()) {
      if (updateCount != Statement.SUCCESS_NO_INFO) {
        count = count.isPresent()
                ? count.map(total -> total + updateCount)
                : Optional.of((long) updateCount);
      }
    }
    return count;
  }

  protected long updateRecordCount() {
    return records
        .stream()
        // ignore deletes
        .filter(record -> nonNull(record.value()) || !config.deleteEnabled)
        .count();
  }
}
