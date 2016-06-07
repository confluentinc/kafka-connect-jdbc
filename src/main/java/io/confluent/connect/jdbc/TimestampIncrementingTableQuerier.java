/**
 * Copyright 2015 Confluent Inc.
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
 **/

package io.confluent.connect.jdbc;

import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.source.SourceRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.Calendar;
import java.util.Collections;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.HashMap;
import java.util.Map;
import java.util.TimeZone;

/**
 * <p>
 *   TimestampIncrementingTableQuerier performs incremental loading of data using two mechanisms: a
 *   timestamp column provides monotonically incrementing values that can be used to detect new or
 *   modified rows and a strictly incrementing (e.g. auto increment) column allows detecting new rows
 *   or combined with the timestamp provide a unique identifier for each update to the row.
 * </p>
 * <p>
 *   At least one of the two columns must be specified (or left as "" for the incrementing column
 *   to indicate use of an auto-increment column). If both columns are provided, they are both
 *   used to ensure only new or updated rows are reported and to totally order updates so
 *   recovery can occur no matter when offsets were committed. If only the incrementing fields is
 *   provided, new rows will be detected but not updates. If only the timestamp field is
 *   provided, both new and updated rows will be detected, but stream offsets will not be unique
 *   so failures may cause duplicates or losses.
 * </p>
 */
public class TimestampIncrementingTableQuerier extends TableQuerier {
  private static final Logger log = LoggerFactory.getLogger(TimestampIncrementingTableQuerier.class);

  private static final Calendar UTC_CALENDAR = new GregorianCalendar(TimeZone.getTimeZone("UTC"));

  private String timestampColumn;
  private Long timestampOffset;
  private String incrementingColumn;
  private boolean incrementingColumnUsePrimaryKey;
  private Long incrementingOffset = null;
  private long timestampDelay;

  public TimestampIncrementingTableQuerier(QueryMode mode, String name, String topicPrefix, String keyColumnName,
                                           String timestampColumn, Long timestampOffset,
                                           String incrementingColumn, boolean incrementingColumnUsePrimaryKey, Long incrementingOffset, Long timestampDelay) {
    super(mode, name, topicPrefix, keyColumnName);
    this.timestampColumn = timestampColumn;
    this.timestampOffset = timestampOffset;
    this.incrementingOffset = incrementingOffset;
    this.timestampDelay = timestampDelay;
    this.incrementingColumn = incrementingColumn;
    this.incrementingColumnUsePrimaryKey = incrementingColumnUsePrimaryKey;
  }

  @Override
  protected void createPreparedStatement(Connection db) throws SQLException {
    // Use primary key if one exists and use.primary.key is set to true.
    // Default when unspecified uses an autoincrementing column.
    if (incrementingColumnUsePrimaryKey && keyColumn != null) {
      incrementingColumn = keyColumn;
      // TODO verify type of key column to be possible to be autoincrementing?
    } else if (incrementingColumn == null || incrementingColumn.isEmpty()) {
      incrementingColumn = JdbcUtils.getAutoincrementColumn(db, name);
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
        throw new ConnectException("Unknown mode encountered when preparing query: " + mode.toString());
    }

    if (incrementingColumn != null && timestampColumn != null) {
      // This version combines two possible conditions. The first checks timestamp == last
      // timestamp and incrementing > last incrementing. The timestamp alone would include
      // duplicates, but adding the incrementing condition ensures no duplicates, e.g. you would
      // get only the row with id = 23:
      //  timestamp 1234, id 22 <- last
      //  timestamp 1234, id 23
      // The second check only uses the timestamp >= last timestamp. This covers everything new,
      // even if it is an update of the existing row. If we previously had:
      //  timestamp 1234, id 22 <- last
      // and then these rows were written:
      //  timestamp 1235, id 22
      //  timestamp 1236, id 23
      // We should capture both id = 22 (an update) and id = 23 (a new row)
      builder.append(" WHERE ");
      builder.append(JdbcUtils.quoteString(timestampColumn, quoteString));
      builder.append(" < ? AND ((");
      builder.append(JdbcUtils.quoteString(timestampColumn, quoteString));
      builder.append(" = ? AND ");
      builder.append(JdbcUtils.quoteString(incrementingColumn, quoteString));
      builder.append(" > ?");
      builder.append(") OR ");
      builder.append(JdbcUtils.quoteString(timestampColumn, quoteString));
      builder.append(" > ?)");
      builder.append(" ORDER BY ");
      builder.append(JdbcUtils.quoteString(timestampColumn, quoteString));
      builder.append(",");
      builder.append(JdbcUtils.quoteString(incrementingColumn, quoteString));
      builder.append(" ASC");
    } else if (incrementingColumn != null) {
      builder.append(" WHERE ");
      builder.append(JdbcUtils.quoteString(incrementingColumn, quoteString));
      builder.append(" > ?");
      builder.append(" ORDER BY ");
      builder.append(JdbcUtils.quoteString(incrementingColumn, quoteString));
      builder.append(" ASC");
    } else if (timestampColumn != null) {
      builder.append(" WHERE ");
      builder.append(JdbcUtils.quoteString(timestampColumn, quoteString));
      builder.append(" > ? AND ");
      builder.append(JdbcUtils.quoteString(timestampColumn, quoteString));
      builder.append(" < ? ORDER BY ");
      builder.append(JdbcUtils.quoteString(timestampColumn, quoteString));
      builder.append(" ASC");
    }
    String queryString = builder.toString();
    log.debug("{} prepared SQL query: {}", this, queryString);
    stmt = db.prepareStatement(queryString);
  }



  @Override
  protected ResultSet executeQuery() throws SQLException {
    if (incrementingColumn != null && timestampColumn != null) {
      Timestamp startTime = new Timestamp(timestampOffset == null ? 0 : timestampOffset);
      Timestamp endTime = new Timestamp(JdbcUtils.getCurrentTimeOnDB(stmt.getConnection(), UTC_CALENDAR).getTime() - timestampDelay);
      stmt.setTimestamp(1, endTime, UTC_CALENDAR);
      stmt.setTimestamp(2, startTime, UTC_CALENDAR);
      stmt.setLong(3, (incrementingOffset == null ? -1 : incrementingOffset));
      stmt.setTimestamp(4, startTime, UTC_CALENDAR);
      log.debug("Executing prepared statement with start time value = " + timestampOffset + " (" + startTime.toString() + ") "
              + " end time " + endTime.toString()
              + " and incrementing value = " + incrementingOffset);
    } else if (incrementingColumn != null) {
      stmt.setLong(1, (incrementingOffset == null ? -1 : incrementingOffset));
      log.debug("Executing prepared statement with incrementing value = " + incrementingOffset);
    } else if (timestampColumn != null) {
      Timestamp startTime = new Timestamp(timestampOffset == null ? 0 : timestampOffset);
      Timestamp endTime = new Timestamp(JdbcUtils.getCurrentTimeOnDB(stmt.getConnection(), UTC_CALENDAR).getTime() - timestampDelay);
      stmt.setTimestamp(1, startTime, UTC_CALENDAR);
      stmt.setTimestamp(2, endTime, UTC_CALENDAR);
      log.debug("Executing prepared statement with timestamp value = " + timestampOffset + " (" + JdbcUtils.formatUTC(startTime) + ") "
              + " end time " + JdbcUtils.formatUTC(endTime));
    }
    return stmt.executeQuery();
  }

  @Override
  public SourceRecord extractRecord() throws SQLException {
    Struct record = DataConverter.convertRecord(valueSchema, resultSet);
    Map<String, Long> offset = new HashMap<>();
    if (incrementingColumn != null) {
      Long id;
      switch (valueSchema.field(incrementingColumn).schema().type()) {
        case INT32:
          id = (long) (Integer) record.get(incrementingColumn);
          break;
        case INT64:
          id = (Long) record.get(incrementingColumn);
          break;
        default:
          throw new ConnectException("Invalid type for incrementing column: "
                                            + valueSchema.field(incrementingColumn).schema().type());
      }

      // If we are only using an incrementing column, then this must be incrementing. If we are also
      // using a timestamp, then we may see updates to older rows.
      assert (incrementingOffset == null || id > incrementingOffset) || timestampColumn != null;
      incrementingOffset = id;

      offset.put(JdbcSourceTask.INCREMENTING_FIELD, id);
    }


    if (timestampColumn != null) {
      Date timestamp = (Date) record.get(timestampColumn);
      assert timestampOffset == null || timestamp.getTime() >= timestampOffset;
      timestampOffset = timestamp.getTime();
      offset.put(JdbcSourceTask.TIMESTAMP_FIELD, timestampOffset);
    }

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

    if (keyColumn != null && keySchema != null) {
      Object key = record.get(keyColumn);

      if (key != null) {
        return new SourceRecord(partition, offset, topic, keySchema, key, record.schema(), record);
      }
    }

    return new SourceRecord(partition, offset, topic, record.schema(), record);
  }

  @Override
  public String toString() {
    return "TimestampIncrementingTableQuerier{" +
           "name='" + name + '\'' +
           ", query='" + query + '\'' +
           ", topicPrefix='" + topicPrefix + '\'' +
           ", timestampColumn='" + timestampColumn + '\'' +
           ", incrementingColumn='" + incrementingColumn + '\'' +
           '}';
  }
}
