/**
 * Copyright 2015 Confluent Inc.
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 **/

package io.confluent.connect.jdbc.source;

import static io.confluent.connect.jdbc.util.DateTimeUtils.min;

import io.confluent.connect.jdbc.source.JdbcSourceConnectorConfig.NumericMapping;
import io.confluent.connect.jdbc.util.DateTimeUtils;
import io.confluent.connect.jdbc.util.JdbcUtils;
import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.Collections;
import java.util.Map;

import org.apache.kafka.connect.data.Decimal;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.source.SourceRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * <p>
 * TimestampIncrementingTableQuerier performs incremental loading of data using two mechanisms: a
 * timestamp column provides monotonically incrementing values that can be used to detect new or
 * modified rows and a strictly incrementing (e.g. auto increment) column allows detecting new
 * rows or combined with the timestamp provide a unique identifier for each update to the row.
 * </p>
 * <p>
 * At least one of the two columns must be specified (or left as "" for the incrementing column
 * to indicate use of an auto-increment column). If both columns are provided, they are both
 * used to ensure only new or updated rows are reported and to totally order updates so
 * recovery can occur no matter when offsets were committed. If only the incrementing fields is
 * provided, new rows will be detected but not updates. If only the timestamp field is
 * provided, both new and updated rows will be detected, but stream offsets will not be unique
 * so failures may cause duplicates or losses.
 * </p>
 */
public class TimestampIncrementingTableQuerier extends TableQuerier {
  private static final Logger log = LoggerFactory.getLogger(
      TimestampIncrementingTableQuerier.class
  );

  private static final BigDecimal LONG_MAX_VALUE_AS_BIGDEC = new BigDecimal(Long.MAX_VALUE);

  private ColumnName timestampColumn;
  private ColumnName incrementingColumn;
  private long timestampDelay;
  private TimestampIncrementingOffset offset;
  private final boolean requireNonZeroScaleIncrementer;


  public TimestampIncrementingTableQuerier(QueryMode mode, String name, String topicPrefix,
                                           String timestampColumn, String incrementingColumn,
                                           Map<String, Object> offsetMap, Long timestampDelay, String schemaPattern, NumericMapping mapNumerics) {
    this(mode, name, topicPrefix, new ColumnName(timestampColumn, null),
        new ColumnName(incrementingColumn, null), offsetMap, timestampDelay, schemaPattern,
        mapNumerics, true);
  }

  public TimestampIncrementingTableQuerier(QueryMode mode, String name, String topicPrefix,
                                           ColumnName timestampColumn, final ColumnName incrementingColumn,
                                           Map<String, Object> offsetMap, Long timestampDelay, String schemaPattern, NumericMapping mapNumerics, final boolean requireNonZeroScaleIncrementer) {
    super(mode, name, topicPrefix, schemaPattern, mapNumerics);
    this.timestampColumn = timestampColumn == null ? ColumnName.empty() : timestampColumn;
    this.incrementingColumn = incrementingColumn == null ? ColumnName.empty() : incrementingColumn;
    this.timestampDelay = timestampDelay;
    this.offset = TimestampIncrementingOffset.fromMap(offsetMap);
    this.requireNonZeroScaleIncrementer = requireNonZeroScaleIncrementer;
  }

  @Override
  protected void createPreparedStatement(Connection db) throws SQLException {
    // Default when unspecified uses an autoincrementing column
    if (!incrementingColumn.isEmpty() && incrementingColumn.isEmpty()) {
      incrementingColumn = new ColumnName(JdbcUtils.getAutoincrementColumn(db, schemaPattern, name),
          null);
    }

    String quoteString = JdbcUtils.getIdentifierQuoteString(db);

    StringBuilder builder = new StringBuilder();

    switch (mode) {
      case TABLE:
        builder.append("SELECT * FROM ");
        builder.append(JdbcUtils.quoteString(name, quoteString));
        break;
      case QUERY:
        builder.append(query);
        break;
      default:
        throw new ConnectException("Unknown mode encountered when preparing query: " + mode);
    }

    if (!(incrementingColumn.isEmpty() || timestampColumn.isEmpty())) {
      timestampIncrementingWhereClause(builder, quoteString);
    } else if (!incrementingColumn.isEmpty()) {
      incrementingWhereClause(builder, quoteString);
    } else if (!timestampColumn.isEmpty()) {
      timestampWhereClause(builder, quoteString);
    }
    String queryString = builder.toString();
    log.debug("{} prepared SQL query: {}", this, queryString);
    stmt = db.prepareStatement(queryString, ResultSet.TYPE_FORWARD_ONLY,
        ResultSet.CONCUR_READ_ONLY);
  }

  private void timestampIncrementingWhereClause(StringBuilder builder, String quoteString) {
    // This version combines two possible conditions.
    //
    // The first check ensures new rows are captured where the timestamp hasn't moved forward since
    // the last poll. It checks timestamp == last timestamp and incrementing > last incrementing.
    // The timestamp alone would include duplicates, but adding the incrementing condition ensures
    // no duplicates.
    // e.g. you would get only the row with id = 23 in the following scenario:
    //  timestamp 1234, id 22 <- last
    //  timestamp 1234, id 23

    // The second check only uses the timestamp > last timestamp. This covers everything new,
    // even if it is an update of the existing row. If we previously had:
    //  timestamp 1234, id 22 <- last
    // and then these rows were written:
    //  timestamp 1235, id 22
    //  timestamp 1236, id 23
    // We should capture both id = 22 (an update) and id = 23 (a new row)

    // note that both checks are limited by the max increment and max timestamp days span settings.

    builder.append(" WHERE ");

    String qualifiedIncrementingColumn = this.incrementingColumn.getQuotedQualifiedName(
        quoteString);

    String qualifiedTimestampColumn = this.timestampColumn.getQuotedQualifiedName(quoteString);

    builder.append(qualifiedTimestampColumn);
    builder.append(" < ? AND ((");
    builder.append(qualifiedTimestampColumn);
    builder.append(" = ? AND ");
    builder.append(qualifiedIncrementingColumn);
    builder.append(" > ? AND ");
    builder.append(qualifiedIncrementingColumn);

    // limiting the increment span to prevent single step overload
    builder.append(" < ?) OR ");
    builder.append(qualifiedTimestampColumn);
    builder.append(" > ?)");
    builder.append(" ORDER BY ");
    builder.append(qualifiedTimestampColumn);
    builder.append(",");
    builder.append(qualifiedIncrementingColumn);
    builder.append(" ASC");
  }

  private void incrementingWhereClause(StringBuilder builder, String quoteString) {
    builder.append(" WHERE ");

    String qualifiedIncrementingColumn = this.incrementingColumn.getQuotedQualifiedName(
        quoteString);

    builder.append(qualifiedIncrementingColumn);
    builder.append(" > ? AND ");
    builder.append(qualifiedIncrementingColumn);
    builder.append(" < ? ORDER BY ");
    builder.append(qualifiedIncrementingColumn);
    builder.append(" ASC");
  }


  private void timestampWhereClause(StringBuilder builder, String quoteString) {
    builder.append(" WHERE ");

    String qualifiedTimestampColumn = this.timestampColumn.getQuotedQualifiedName(quoteString);

    builder.append(qualifiedTimestampColumn);
    builder.append(" > ? AND ");
    builder.append(qualifiedTimestampColumn);
    builder.append(" < ? ORDER BY ");
    builder.append(qualifiedTimestampColumn);
    builder.append(" ASC");
  }

  @Override
  protected ResultSet executeQuery(int fetchSize) throws SQLException {
    if (!(incrementingColumn.isEmpty() || timestampColumn.isEmpty())) {
      Timestamp tsOffset = offset.getTimestampOffset();
      Timestamp tsEndOffset = offset.getTimestampSpanEndOffset();
      Long incOffset = offset.getIncrementingOffset();
      Long incEndOffset = offset.getIncrementingSpanEndOffset();
      final long currentDbTime = JdbcUtils.getCurrentTimeOnDB(
          stmt.getConnection(),
          DateTimeUtils.UTC_CALENDAR.get()
      ).getTime();
      Timestamp endTime = new Timestamp(currentDbTime - timestampDelay);
      final Timestamp adjustedEndTime = min(endTime, tsEndOffset);
      stmt.setTimestamp(1, adjustedEndTime, DateTimeUtils.UTC_CALENDAR.get());
      stmt.setTimestamp(2, tsOffset, DateTimeUtils.UTC_CALENDAR.get());
      stmt.setLong(3, incOffset);
      stmt.setLong(4, incEndOffset);
      stmt.setTimestamp(5, tsOffset, DateTimeUtils.UTC_CALENDAR.get());
      log.debug(
          "Executing prepared statement with start time > {} and end time <= {} and incrementing"
              + " value > {} and < {}",
          DateTimeUtils.formatUtcTimestamp(tsOffset),
          DateTimeUtils.formatUtcTimestamp(adjustedEndTime),
          incOffset, incEndOffset
      );
    } else if (!incrementingColumn.isEmpty()) {
      Long incOffset = offset.getIncrementingOffset();
      Long incEndOffset = offset.getIncrementingSpanEndOffset();
      stmt.setLong(1, incOffset);
      stmt.setLong(2, incEndOffset);
      log.debug("Executing prepared statement with incrementing value > {} and < {}",
          incOffset, incEndOffset);
    } else if (!timestampColumn.isEmpty()) {
      Timestamp tsOffset = offset.getTimestampOffset();
      Timestamp tsSpanEndOffset = offset.getTimestampSpanEndOffset();
      final long currentDbTime = JdbcUtils.getCurrentTimeOnDB(
          stmt.getConnection(),
          DateTimeUtils.UTC_CALENDAR.get()
      ).getTime();
      Timestamp endTime = new Timestamp(currentDbTime - timestampDelay);
      stmt.setTimestamp(1, tsOffset, DateTimeUtils.UTC_CALENDAR.get());
      final Timestamp adjustedEndTime = min(endTime, tsSpanEndOffset);
      stmt.setTimestamp(2, adjustedEndTime, DateTimeUtils.UTC_CALENDAR.get());
      log.debug("Executing prepared statement with timestamp value > {} end time < {}",
          DateTimeUtils.formatUtcTimestamp(tsOffset),
          DateTimeUtils.formatUtcTimestamp(adjustedEndTime));
    }
    stmt.setFetchSize(fetchSize);
    return stmt.executeQuery();
  }

  @Override
  public SourceRecord extractRecord() throws SQLException {
    final Struct record = DataConverter.convertRecord(schema, resultSet, mapNumerics);
    offset = extractOffset(schema, record);
    // TODO: Key?
    final String topic;
    final Map<String, String> partition;
    switch (mode) {
      case TABLE:
        partition = Collections.singletonMap(JdbcSourceConnectorConstants.TABLE_NAME_KEY, name);
        topic = topicPrefix + name;
        break;
      case QUERY:
        partition = Collections.singletonMap(JdbcSourceConnectorConstants.QUERY_NAME_KEY,
            JdbcSourceConnectorConstants.QUERY_NAME_VALUE);
        topic = topicPrefix;
        break;
      default:
        throw new ConnectException("Unexpected query mode: " + mode);
    }
    return new SourceRecord(partition, offset.toMap(), topic, record.schema(), record);
  }

  /**
   * Reset (frame-shift) the offsets based on the given record.
   * <p/>
   * Visible for testing
   */
  TimestampIncrementingOffset extractOffset(Schema schema, Struct record) {
    final Timestamp extractedTimestamp;
    if (!timestampColumn.isEmpty()) {
      extractedTimestamp = (Timestamp) record.get(timestampColumn.getName());
      Timestamp timestampOffset = offset.getTimestampOffset();
      assert timestampOffset != null && timestampOffset.compareTo(extractedTimestamp) <= 0;
    } else {
      extractedTimestamp = null;
    }

    final Long extractedId;
    if (!incrementingColumn.isEmpty()) {
      final Schema incrementingColumnSchema = schema.field(incrementingColumn.getName()).schema();
      final Object incrementingColumnValue = record.get(incrementingColumn.getName());
      if (incrementingColumnValue == null) {
        throw new ConnectException(
            "Null value for incrementing column of type: " + incrementingColumnSchema.type()
        );
      } else if (isIntegralPrimitiveType(incrementingColumnValue)) {
        extractedId = ((Number) incrementingColumnValue).longValue();
      } else if (incrementingColumnSchema.name() != null
          && incrementingColumnSchema.name().equals(Decimal.LOGICAL_NAME)) {
        extractedId = extractDecimalId(incrementingColumnValue);
      } else {
        throw new ConnectException(
            "Invalid type for incrementing column: " + incrementingColumnSchema.type()
        );
      }

      // If we are only using an incrementing column, then this must be incrementing.
      // If we are also using a timestamp, then we may see updates to older rows.
      Long incrementingOffset = offset.getIncrementingOffset();
      assert incrementingOffset == -1L
          || extractedId > incrementingOffset
          || !timestampColumn.isEmpty();
    } else {
      extractedId = null;
    }

    return offset.copy(extractedTimestamp, extractedId);
  }

  private Long extractDecimalId(Object incrementingColumnValue) {
    final BigDecimal decimal = ((BigDecimal) incrementingColumnValue);
    if (decimal.compareTo(LONG_MAX_VALUE_AS_BIGDEC) > 0) {
      throw new ConnectException(
          "Decimal value for incrementing column exceeded Long.MAX_VALUE"
      );
    }

    if (requireNonZeroScaleIncrementer && decimal.scale() != 0) {
      throw new ConnectException("Scale of Decimal value for incrementing column must be 0");
    }

    return decimal.longValue();
  }

  private boolean isIntegralPrimitiveType(Object incrementingColumnValue) {
    return incrementingColumnValue instanceof Long
        || incrementingColumnValue instanceof Integer
        || incrementingColumnValue instanceof Short
        || incrementingColumnValue instanceof Byte;
  }

  @Override
  public String toString() {
    return "TimestampIncrementingTableQuerier{"
        + "name='" + name + '\''
        + ", query='" + query + '\''
        + ", topicPrefix='" + topicPrefix + '\''
        + ", timestampColumn=" + timestampColumn
        + ", incrementingColumn=" + incrementingColumn
        + '}';
  }
}
