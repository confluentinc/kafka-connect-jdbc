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

package io.confluent.connect.jdbc.source;

import org.apache.kafka.common.errors.InvalidTimestampException;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.source.SourceRecord;

import java.sql.*;
import java.util.Collections;
import java.util.Map;

/**
 * TableQuerier executes queries against a specific table. Implementations handle different types
 * of queries: periodic bulk loading, incremental loads using auto incrementing IDs, incremental
 * loads using timestamps, etc.
 */
abstract class TableQuerier implements Comparable<TableQuerier> {
  public enum QueryMode {
    TABLE, // Copying whole tables, with queries constructed automatically
    QUERY // User-specified query
  }

  protected final QueryMode mode;
  protected final String schemaPattern;
  private String kafkaKeyColumn;
  private String kafkaTimestampColumn;
  protected final String name;
  protected final String query;
  protected final String topicPrefix;

  // Mutable state

  protected final boolean mapNumerics;
  protected long lastUpdate;
  protected PreparedStatement stmt;
  protected ResultSet resultSet;
  protected Schema valueSchema;
  protected Schema keySchema;

  public TableQuerier(QueryMode mode, String nameOrQuery, String topicPrefix,
                      String schemaPattern, boolean mapNumerics,
                      String kafkaKeyColumn, String kafkaTimestampColumn) {
    this.mode = mode;
    this.schemaPattern = schemaPattern;
    this.kafkaKeyColumn = kafkaKeyColumn;
    this.kafkaTimestampColumn = kafkaTimestampColumn;
    this.name = mode.equals(QueryMode.TABLE) ? nameOrQuery : null;
    this.query = mode.equals(QueryMode.QUERY) ? nameOrQuery : null;
    this.topicPrefix = topicPrefix;
    this.mapNumerics = mapNumerics;
    this.lastUpdate = 0;
  }

  public long getLastUpdate() {
    return lastUpdate;
  }

  public PreparedStatement getOrCreatePreparedStatement(Connection db) throws SQLException {
    if (stmt != null) {
      return stmt;
    }
    createPreparedStatement(db);
    return stmt;
  }

  protected abstract void createPreparedStatement(Connection db) throws SQLException;

  public boolean querying() {
    return resultSet != null;
  }

  public void maybeStartQuery(Connection db) throws SQLException {
    if (resultSet == null) {
      stmt = getOrCreatePreparedStatement(db);
      resultSet = executeQuery();
      ResultSetMetaData metadata = resultSet.getMetaData();
      valueSchema = DataConverter.convertSchema(name, metadata, mapNumerics);
      if(kafkaKeyColumn != null && !kafkaKeyColumn.isEmpty()) {
        keySchema = DataConverter.convertSchema(kafkaKeyColumn, metadata, mapNumerics);
      }
    }
  }

  protected abstract ResultSet executeQuery() throws SQLException;

  public boolean next() throws SQLException {
    return resultSet.next();
  }

  public abstract SourceRecord extractRecord() throws SQLException;

  protected SourceRecord extractRecordCore(Struct record, Map<String, ?> sourceOffset) throws SQLException {
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
    Object kafkaKey = null;
    if (kafkaKeyColumn != null && !kafkaKeyColumn.isEmpty()) {
      kafkaKey = record.get(kafkaKeyColumn);
    }

    Long kafkaTimestamp = null;
    if (kafkaTimestampColumn != null && !kafkaTimestampColumn.isEmpty()) {
      Object timestamp = record.get(kafkaTimestampColumn);
      if(timestamp instanceof Date){
        kafkaTimestamp = ((Date)timestamp).getTime();
      }
      else if(timestamp instanceof Timestamp) {
        kafkaTimestamp = ((Timestamp)timestamp).getTime();
      }
      else if(timestamp instanceof Long){
        kafkaTimestamp = (Long)timestamp;
      }
      else {
        throw new InvalidTimestampException(String.format("Type %s cannot be used as a Kafka Timestamp. " +
                "Valid types are Date, Timestamp and Long", timestamp.getClass().getName()));
      }
    }
    return new SourceRecord(partition, sourceOffset, topic, null, keySchema, kafkaKey, record.schema(), record, kafkaTimestamp);
  }

  public void reset(long now) {
    closeResultSetQuietly();
    closeStatementQuietly();
    // TODO: Can we cache this and quickly check that it's identical for the next query
    // instead of constructing from scratch since it's almost always the same
    valueSchema = null;
    keySchema = null;
    lastUpdate = now;
  }

  private void closeStatementQuietly() {
    if (stmt != null) {
      try {
        stmt.close();
      } catch (SQLException ignored) {
      }
    }
    stmt = null;
  }

  private void closeResultSetQuietly() {
    if (resultSet != null) {
      try {
        resultSet.close();
      } catch (SQLException ignored) {
      }
    }
    resultSet = null;
  }

  @Override
  public int compareTo(TableQuerier other) {
    if (this.lastUpdate < other.lastUpdate) {
      return -1;
    } else if (this.lastUpdate > other.lastUpdate) {
      return 1;
    } else {
      return this.name.compareTo(other.name);
    }
  }
}
