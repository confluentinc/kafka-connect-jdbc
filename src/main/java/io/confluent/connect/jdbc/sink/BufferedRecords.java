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
import io.confluent.connect.jdbc.util.LogUtil;
import io.confluent.connect.jdbc.util.SafeSqlException;
import io.confluent.connect.jdbc.util.TableDefinition;
import io.confluent.connect.jdbc.util.TableId;

import static java.util.Objects.isNull;
import static java.util.Objects.nonNull;

public class BufferedRecords {
  private static final Logger log = LoggerFactory.getLogger(BufferedRecords.class);
  private static final Logger SENSITIVE =
      LoggerFactory.getLogger("io.confluent.connect.jdbc.sink.Sensitive");

  private final TableId tableId;
  private final JdbcSinkConfig config;
  private final DatabaseDialect dbDialect;
  private final DbStructure dbStructure;
  private final Connection connection;

  private List<SinkRecord> records = new ArrayList<>();
  private Schema keySchema;
  private Schema valueSchema;
  private RecordValidator recordValidator;
  private FieldsMetadata fieldsMetadata;
  private PreparedStatement updatePreparedStatement;
  private PreparedStatement deletePreparedStatement;
  private StatementBinder updateStatementBinder;
  private StatementBinder deleteStatementBinder;
  private SafeSqlContext updateContext;
  private SafeSqlContext deleteContext;
  private boolean deletesInBatch = false;

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
          config.stringOutputValueColumnName,
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
          config.insertMode,
          config.replaceNullWithDefault
      );
      if (config.deleteEnabled && nonNull(deleteSql)) {
        deletePreparedStatement = dbDialect.createPreparedStatement(connection, deleteSql);
        deleteStatementBinder = dbDialect.statementBinder(
            deletePreparedStatement,
            config.pkMode,
            schemaPair,
            fieldsMetadata,
            dbStructure.tableDefinition(connection, tableId),
            config.insertMode,
            config.replaceNullWithDefault
        );
      }
      buildSafeContexts();
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
    executeUpdates();
    executeDeletes();

    final List<SinkRecord> flushedRecords = records;
    records = new ArrayList<>();
    deletesInBatch = false;
    return flushedRecords;
  }

  private void executeUpdates() throws SQLException {
    try {
      int[] batchStatus = updatePreparedStatement.executeBatch();
      for (int updateCount : batchStatus) {
        if (updateCount == Statement.EXECUTE_FAILED) {
          throw new BatchUpdateException(
                  "Execution failed for part of the batch update", batchStatus);
        }
      }
    } catch (SQLException e) {
      throw failSafely(e, updateContext);
    }
  }

  private void executeDeletes() throws SQLException {
    try {
      if (nonNull(deletePreparedStatement)) {
        int[] batchStatus = deletePreparedStatement.executeBatch();
        for (int updateCount : batchStatus) {
          if (updateCount == Statement.EXECUTE_FAILED) {
            throw new BatchUpdateException(
                    "Execution failed for part of the batch delete", batchStatus);
          }
        }
      }
    } catch (SQLException e) {
      throw failSafely(e, deleteContext);
    }
  }

  // Precomputes the value-free reconstructed statements for this batch's update and delete once,
  // from the destination table's own columns, so a later failure can be described without the driver text.
  private void buildSafeContexts() {
    updateContext = null;
    deleteContext = null;

    // With auto.create or auto.evolve on, a producer field can still become a real column, so the
    // live schema is not a trustworthy identifier source yet; leave the contexts null to fall back to the skeleton.
    if (config.autoCreate || config.autoEvolve) {
      return;
    }
    TableDefinition tableDef;
    try {
      tableDef = dbStructure.tableDefinition(connection, tableId);
    } catch (SQLException e) {
      return;
    }
    if (tableDef == null) {
      return;
    }
    java.util.Set<String> dbColumns = tableDef.columnNames();
    String vendorPrefix =
        dbDialect.getClass().getSimpleName().contains("Oracle") ? "ORA" : null;
    java.util.List<ColumnId> keyCols =
        resolveCanonical(fieldsMetadata.keyFieldNames, dbColumns);
    java.util.List<ColumnId> nonKeyCols =
        resolveCanonical(fieldsMetadata.nonKeyFieldNames, dbColumns);
    if (keyCols != null && nonKeyCols != null) {
      updateContext = SafeSqlContext.create(
          dbDialect,
          tableId,
          keyCols,
          nonKeyCols,
          operationFor(config.insertMode),
          vendorPrefix
      ).orElse(null);
    }
    if (config.deleteEnabled && keyCols != null && !keyCols.isEmpty()) {
      deleteContext = SafeSqlContext.create(
          dbDialect,
          tableId,
          keyCols,
          java.util.Collections.emptyList(),
          SafeSqlContext.Operation.DELETE,
          vendorPrefix
      ).orElse(null);
    }
  }

  private java.util.List<ColumnId> resolveCanonical(
      java.util.Collection<String> fieldNames,
      java.util.Set<String> dbColumns
  ) {
    java.util.List<ColumnId> resolved = new java.util.ArrayList<>();
    for (String field : fieldNames) {
      String canonical = canonicalColumn(field, dbColumns);
      if (canonical == null) {
        return null;
      }
      resolved.add(new ColumnId(tableId, canonical));
    }
    return resolved;
  }

  private static String canonicalColumn(String field, java.util.Set<String> dbColumns) {
    if (field == null) {
      return null;
    }
    String canonical = null;
    if (dbColumns.contains(field)) {
      canonical = field;
    } else {
      for (String column : dbColumns) {
        if (column.equalsIgnoreCase(field)) {
          if (canonical != null) {
            return null;
          }
          canonical = column;
        }
      }
    }
    if (canonical == null
        || canonical.indexOf('?') >= 0
        || canonical.indexOf('<') >= 0
        || canonical.indexOf('>') >= 0) {
      return null;
    }
    return canonical;
  }

  private static SafeSqlContext.Operation operationFor(JdbcSinkConfig.InsertMode mode) {
    switch (mode) {
      case UPSERT:
        return SafeSqlContext.Operation.UPSERT;
      case UPDATE:
        return SafeSqlContext.Operation.UPDATE;
      default:
        return SafeSqlContext.Operation.INSERT;
    }
  }

  private SafeSqlException failSafely(SQLException raw, SafeSqlContext ctx) {
    SafeSqlException safe = ctx == null
        ? LogUtil.sanitize(raw)
        : LogUtil.sanitizeReconstructed(raw, ctx.safeStatement(), ctx.vendorPrefix());
    if (config.sensitiveTraceEnabled && SENSITIVE.isTraceEnabled()) {
      SENSITIVE.trace("Raw SQL failure for {}",
          ctx == null ? "unknown statement" : ctx.qualifiedTable(), raw);
    }
    return safe;
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

  private String getInsertSql() throws SQLException {
    switch (config.insertMode) {
      case INSERT:
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

  private String getDeleteSql() {
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

  private Collection<ColumnId> asColumns(Collection<String> names) {
    return names.stream()
        .map(name -> new ColumnId(tableId, name))
        .collect(Collectors.toList());
  }
}
