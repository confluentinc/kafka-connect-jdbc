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
import io.confluent.connect.jdbc.util.TableId;
import org.apache.kafka.connect.data.Decimal;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.data.Time;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.SinkRecord;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

import static io.confluent.connect.jdbc.sink.JdbcSinkConfig.InsertMode.INSERT;
import static io.confluent.connect.jdbc.sink.JdbcSinkConfig.PrimaryKeyMode.KAFKA;
import static java.util.Objects.isNull;
import static java.util.Objects.nonNull;

/**
 * Load buffered records perform JDBC transactions through batching database operations similar
 * to {@link BatchedBufferedRecords} but with some key differences. All non delete operations are
 * treated as upserts (no explicit updates or inserts).
 *
 * <p>As SinkRecords are added, their schema is examined to detect changes in structure and the
 * appropriate database operation to perform.
 *
 * <p>Operations are divided into two types; deletes and non deletes (upserts).
 * SinkRecords of both operation types will be batched along side each other until certain
 * criteria is met in which case a flush is automatically performed an processing begins again.
 *
 * <p>A schema change will flush all pending records then attempt to alter the target table
 * structure (if set in the sink's config).
 *
 * <p>A record without a value schema (set to null) will be flagged for deletion. Records pending
 * deletion will accumulate until a non deleted record is deleted in which case all previous
 * processed records are flushed.
 *
 * <p>A flush is also triggered when enough SinkRecords have been added (when the batch limit
 * has been reached).
 *
 * <p>Each flush will result in the creation of a temporary staging table where all pending records
 * will be inserted into followed by a single table merge (from temp table to target table).
 */
public class LoadMergeBufferedRecords extends BaseBufferedRecords {

  public LoadMergeBufferedRecords(
      JdbcSinkConfig config,
      TableId tableId,
      DatabaseDialect dbDialect,
      DbStructure dbStructure,
      Connection connection) {
    super(config, tableId, dbDialect, dbStructure, connection);
  }

  @Override
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

      close();
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
    bindAndExecutePreparedStatements(records);

    final List<SinkRecord> flushedRecords = records;
    records = new ArrayList<>();
    deletesInBatch = false;
    return flushedRecords;
  }

  private void bindAndExecutePreparedStatements(Collection<SinkRecord> records)
      throws SQLException {
    SchemaPair schemaPair = new SchemaPair(this.keySchema, this.valueSchema);

    final String deleteSql = getDeleteSql();
    if (nonNull(deleteSql)) {
      initDeletePreparedStatement(deleteSql, schemaPair);
    }
    Collection<SinkRecord> updatedRecords = new ArrayList<>();
    for (SinkRecord record : records) {
      if (isNull(record.value()) && nonNull(deleteStatementBinder)) {
        deleteStatementBinder.bindRecord(record);
      } else {
        updatedRecords.add(record);
      }
    }

    Optional<Long> totalUpdateCount = executeMerge(updatedRecords, schemaPair);
    long totalDeleteCount = executeDeletes();

    if (log.isTraceEnabled()) {
      log.trace("merged records:{} resulting in totalUpdateCount:{} totalDeleteCount:{}",
          records.size(), totalUpdateCount, totalDeleteCount
      );
    }

    // validate the number of entries changed reported from JDBC.
    assertModifiedCount(updatedRecords.size(), totalUpdateCount);
  }

  private Optional<Long> executeMerge(Collection<SinkRecord> records, SchemaPair schemaPair)
      throws SQLException {
    log.info("Binding records");
    if (records.size() == 0) {
      log.info("No records to bind, skipping inserts");
      return Optional.empty();
    }

    TableId tempTableId = createTempTable();
    final String insertSql = getInsertSql(INSERT, tempTableId);

    initUpdatePreparedStatement(insertSql, schemaPair);
    records = removeDuplicates(records, fieldsMetadata);

    for (SinkRecord record : records) {
      log.info("Binding record " + record);
      updateStatementBinder.bindRecord(record);
    }

    Optional<Long> count = Optional.empty();
    for (int updateCount : updatePreparedStatement.executeBatch()) {
      if (updateCount != Statement.SUCCESS_NO_INFO) {
        count = count.isPresent()
            ? count.map(total -> total + updateCount)
            : Optional.of((long) updateCount);
      }
    }

    final long expectedCount = records.size();
    assertModifiedCount(expectedCount, count);

    long mergedCount = executeMergeFromTempTableStatement(tempTableId);
    count = Optional.of(mergedCount);

    return count;
  }

  protected void assertModifiedCount(long expected, Optional<Long> actual) {
    if (actual.filter(total -> total != expected).isPresent()
        && config.insertMode == INSERT) {
      throw new ConnectException(String.format(
          "Update count (%d) did not sum up to total number of records inserted (%d)",
          actual.get(),
          expected
      ));
    }
    if (!actual.isPresent()) {
      log.info(
          "{} records:{} , but no count of the number of rows it affected is available",
          config.insertMode,
          records.size()
      );
    }
  }

  private long executeMergeFromTempTableStatement(TableId tempTable)
      throws SQLException {
    String sql = dbDialect.buildTempTableMergeQueryStatement(
        tableId,
        tempTable,
        asColumns(fieldsMetadata.keyFieldNames),
        asColumns(fieldsMetadata.nonKeyFieldNames));

    PreparedStatement tempTableMergePreparedStatement = connection.prepareStatement(sql);
    return tempTableMergePreparedStatement.executeUpdate();
  }

  private TableId createTempTable() throws SQLException {
    final TableId id = createTempTableId();
    final String sql = getCreateTempTableSql(id);

    log.info("Creating table with sql: {}", sql);
    dbDialect.applyDdlStatements(connection, Collections.singletonList(sql));
    return id;
  }

  private TableId createTempTableId() {
    return dbDialect.parseTableIdentifier(tableId.tableName() + "_temp_" + newTableNameSuffix());
  }

  private String getCreateTempTableSql(TableId tempTable) {
    return dbDialect.buildCreateTempTableStatement(tempTable, fieldsMetadata.allFields.values());
  }

  private static String newTableNameSuffix() {
    return UUID.randomUUID()
        .toString()
        .substring(0, 12);
  }

  /**
   * Remove records from the List of SinkRecord's that have the same key values (are duplicates).
   * <p>
   * Use a HashMap&ltString, SinkRecord&gt keyed by the SinkRecord keys, always put to the HashMap
   * without checking for previous existence of key
   * which will cause duplicates to overlay one another. When the HashMap values are
   * returned the list will contain no duplicates. The key fields of the record are converted to
   * Strings and concatenated to produce a unique value of a single type (String) for the keys.
   * </p>
   *
   * @param recordList     list of SinkRecord that may have duplicate keys.
   * @param fieldsMetadata field metadata.
   * @return Collection of SinkRecords containing records that each have unique key(s).
   */
  public Collection<SinkRecord> removeDuplicates(
      Collection<SinkRecord> recordList,
      FieldsMetadata fieldsMetadata) {
    if (config.pkMode == KAFKA) {
      return recordList; //record rows should already be unique.
    }

    if (log.isDebugEnabled()) {
      log.debug("event=removeDuplicates orignalNumber=" + recordList.size());
    }
    HashMap<List<String>, SinkRecord> uniqueRecordMap = new HashMap<>(recordList.size());
    for (SinkRecord sr : recordList) {
      List<String> mashedKeys = mashUpKeys(sr, fieldsMetadata);
      SinkRecord rv = uniqueRecordMap.put(mashedKeys, sr);
      if (rv != null) {
        if (log.isDebugEnabled()) {
          log.debug("event=removeDuplicates duplicateFound=" + keysToString(mashedKeys));
          log.debug("event=duprecord rv=" + "rv=" + rv);
        }
      }
    }
    if (log.isDebugEnabled()) {
      log.debug("event=removeDuplicates returning={} from records={}",
          uniqueRecordMap.size(), recordList.size());
    }
    return new ArrayList<>(uniqueRecordMap.values());
  }

  /**
   * For debugging log messages, produce nicely formatted picture of key list.
   *
   * @param keyList collection of mashed up keys.
   * @return String of key elements delimited by "|"
   */
  private String keysToString(List<String> keyList) {
    StringBuilder sb = new StringBuilder("|");
    for (String s : keyList) {
      sb.append(s).append("|");
    }
    return sb.toString();
  }

  /**
   * For a SinkRecord, create an unmodifiable List of String representations of the keys.
   *
   * @param sr             A SinkRecord
   * @param fieldsMetadata field metadata.
   * @return String containing all the specified keys converted to a string and concatenated.
   */
  private List<String> mashUpKeys(SinkRecord sr, FieldsMetadata fieldsMetadata) {
    List<String> listOfKeys;
    switch (config.pkMode) {
      case RECORD_KEY:
        listOfKeys = retrieveKeysFromKeySchema(sr, fieldsMetadata);
        break;

      case RECORD_VALUE:
        listOfKeys = retrieveKeysFromValueSchema(sr, fieldsMetadata);
        break;

      default:
        throw new RuntimeException(" pk mode not supported: " + config.pkMode);
    }

    if (log.isTraceEnabled()) {
      log.trace("event={} mashedKey={} pkmode={}", "mashUpKeys",
          keysToString(listOfKeys), config.pkMode);
    }
    return Collections.unmodifiableList(listOfKeys);
  }

  /**
   * Retrieve primary key values from record value schema.
   *
   * @param sr             sink record.
   * @param fieldsMetadata field metadata.
   * @return collection of primary keys as string.
   */
  private List<String> retrieveKeysFromValueSchema(SinkRecord sr, FieldsMetadata fieldsMetadata) {
    SchemaPair schemaPair = new SchemaPair(sr.keySchema(), sr.valueSchema());
    ArrayList<String> listOfKeys = new ArrayList<>();
    for (String fieldName : fieldsMetadata.keyFieldNames) {
      final Field field = schemaPair.valueSchema.field(fieldName);
      Object value = ((Struct) sr.value()).get(field);
      String key = keyFieldToString(field.schema(), value);
      listOfKeys.add(key);
    }

    return listOfKeys;
  }

  /**
   * Retrieve primary key values from record key schema.
   *
   * @param sr             sink records.
   * @param fieldsMetadata field metadata.
   * @return collection of primary keys as string.
   */
  private List<String> retrieveKeysFromKeySchema(SinkRecord sr, FieldsMetadata fieldsMetadata) {
    ArrayList<String> listOfKeys = new ArrayList<>();
    SchemaPair schemaPair = new SchemaPair(sr.keySchema(), sr.valueSchema());

    if (schemaPair.keySchema.type().isPrimitive()) {
      listOfKeys.add(primitiveValueToString(schemaPair.keySchema, sr.key()));
    } else {
      for (String fieldName : fieldsMetadata.keyFieldNames) {
        final Field field = schemaPair.keySchema.field(fieldName);
        listOfKeys.add(keyFieldToString(field.schema(), ((Struct) sr.key()).get(field)));
      }
    }

    return listOfKeys;
  }

  /**
   * If the field is a primitive type return the String representation of that type.
   *
   * @param schema schema for the field.
   * @param value  value for the field.
   * @return String representation of the value Object for the primitive type.
   */
  public static String primitiveValueToString(Schema schema, Object value) {
    switch (schema.type()) {
      case INT8:
        return ((Byte) value).toString();
      case INT16:
        return ((Short) value).toString();
      case INT32:
        return ((Integer) value).toString();
      case INT64:
        return ((Long) value).toString();
      case FLOAT32:
        return ((Float) value).toString();
      case FLOAT64:
        return ((Double) value).toString();
      case BOOLEAN:
        return ((Boolean) value).toString();
      case STRING:
        return (String) value;
      case BYTES:
        final byte[] bytes;
        if (value instanceof ByteBuffer) {
          final ByteBuffer buffer = ((ByteBuffer) value).slice();
          bytes = new byte[buffer.remaining()];
          buffer.get(bytes);
        } else {
          bytes = (byte[]) value;
        }
        return Arrays.toString(bytes);
      default:
        return "null";
    }
  }

  /**
   * Convert an Object to a representative String using schema to determine conversion.
   *
   * @param schema schema to use for determining field value
   * @param value  actual value object for the field
   * @return String of key converted to reasonable string
   */
  public String keyFieldToString(Schema schema, Object value) {
    if (value == null) {
      if (log.isDebugEnabled()) {
        log.debug("event=keyFieldToString value=null");
      }
      return "<null>";
    }

    String keyAsString = null;
    if (keyAsString == null) {
      keyAsString = isLogicalType(schema, value);
      if (log.isInfoEnabled()) {
        log.info("event=keyFieldToString isLogicalType=" + keyAsString);
      }
    }
    if (keyAsString == null) {
      keyAsString = primitiveValueToString(schema, value);
      if (log.isInfoEnabled()) {
        log.info("event=keyFieldToString primitiveValueToString=" + keyAsString);
      }
    }

    return keyAsString;
  }

  /**
   * If field in the schema is an avro logical type, convert to string.
   *
   * @param schema the schema for the record values.
   * @param value  the Object value from a SinkRecord that accompanied the schema.
   * @return
   * String containing the logical type's value converted to a String or null if not a logical type.
   */
  private String isLogicalType(Schema schema, Object value) {
    if (schema.name() != null) {
      switch (schema.name()) {
        case org.apache.kafka.connect.data.Date.LOGICAL_NAME:
        case Time.LOGICAL_NAME:
        case org.apache.kafka.connect.data.Timestamp.LOGICAL_NAME:
          return ((java.util.Date) value).toInstant().toString();
        case Decimal.LOGICAL_NAME:
          return ((BigDecimal) value).toString();
        default:
          return null;
      }
    }
    return null;
  }

}
