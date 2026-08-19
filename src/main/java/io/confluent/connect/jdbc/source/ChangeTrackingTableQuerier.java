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

package io.confluent.connect.jdbc.source;

import io.confluent.connect.jdbc.dialect.DatabaseDialect;
import io.confluent.connect.jdbc.source.SchemaMapping.FieldSetter;
import io.confluent.connect.jdbc.util.ColumnDefinition;
import io.confluent.connect.jdbc.util.ColumnId;
import io.confluent.connect.jdbc.util.ExpressionBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.source.SourceRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * ChangeTrackingTableQuerier always returns the latest changed rows after a specific
 * change_version.
 */
public class ChangeTrackingTableQuerier extends TableQuerier {
  private static final Logger log = LoggerFactory.getLogger(ChangeTrackingTableQuerier.class);
  protected final Map<String, String> partition;
  protected final String topic;
  private List<ColumnId> columns;
  private ColumnId primaryKeyColumn;
  protected ChangeTrackingOffset committedOffset;
  protected ChangeTrackingOffset offset;
  private static final String CHANGE_TRACKING_SQL =
      "SELECT CT.%s,%s,CT.SYS_CHANGE_OPERATION AS operation_ind,"
      + "CT.SYS_CHANGE_VERSION as %s FROM %s.%s "
      + "RIGHT OUTER JOIN CHANGETABLE(CHANGES %s.%s, %s) AS CT "
      + "ON %s.%s.%s = CT.%s ORDER BY CT.SYS_CHANGE_VERSION";

  public ChangeTrackingTableQuerier(
      DatabaseDialect dialect,
      QueryMode mode,
      String name,
      String topicPrefix,
      Map<String, Object> offsetMap,
      String suffix
  ) {
    super(dialect, mode, name, topicPrefix, suffix, false);
    ChangeTrackingOffset initialOffset = ChangeTrackingOffset.fromMap(offsetMap);
    this.committedOffset = initialOffset;
    this.offset = initialOffset;
    String tableName = tableId.tableName();
    topic = topicPrefix + tableName; // backward compatible
    partition = OffsetProtocols.sourcePartitionForProtocolV1(tableId);
  }

  @Override
  protected void createPreparedStatement(Connection db) throws SQLException {
    findAllNonPKColumns(db);
    findPrimaryKeyColumn(db);

    String schemaName = tableId.schemaName();
    String tableName = tableId.tableName();

    ExpressionBuilder builder = dialect.expressionBuilder();
    builder.append(CHANGE_TRACKING_SQL);
    String columnsString = columns.stream()
        .map(columnId -> schemaName + '.' + tableName + "." + columnId.name())
        .collect(Collectors.joining(","));
    long changeVersionOffset = offset.getChangeVersionOffset(dialect, db, tableId);
    String queryString = String.format(builder.toString(),
        primaryKeyColumn.name(),
        columnsString,
        ChangeTrackingOffset.CHANGE_TRACKING_OFFSET_FIELD,
        schemaName,
        tableName,
        schemaName,
        tableName,
        changeVersionOffset,
        schemaName,
        tableName,
        primaryKeyColumn.name(),
        primaryKeyColumn.name());
    recordQuery(queryString);
    log.trace("{} prepared SQL query: {}", this, queryString);
    stmt = dialect.createPreparedStatement(db, queryString);
  }

  @Override
  protected ResultSet executeQuery() throws SQLException {
    return stmt.executeQuery();
  }

  @Override
  public SourceRecord extractRecord() throws SQLException {
    Struct record = new Struct(schemaMapping.schema());
    for (FieldSetter setter : schemaMapping.fieldSetters()) {
      try {
        setter.setField(record, resultSet);
      } catch (IOException e) {
        log.warn("Error mapping fields into Connect record", e);
        throw new ConnectException(e);
      } catch (SQLException e) {
        log.warn("SQL error mapping fields into Connect record", e);
        throw new DataException(e);
      }
    }
    offset = extractOffset();
    return new SourceRecord(partition, offset.toMap(), topic, record.schema(), record);
  }

  @Override
  public void reset(long now, boolean resetOffset) {
    // the task is being reset, any uncommitted offset needs to be reset as well
    // use the previous committedOffset to set the running offset
    if (resetOffset) {
      this.offset = this.committedOffset;
    }
    super.reset(now, resetOffset);
  }

  @Override
  public String toString() {
    return "ChangeTrackingTableQuerier{" + "table='" + tableId + '\'' + ", query='" + query + '\''
           + ", topicPrefix='" + topicPrefix + '\'' + '}';
  }

  @Override
  public void maybeStartQuery(Connection db) throws SQLException, ConnectException {
    if (resultSet == null) {
      this.db = db;
      stmt = getOrCreatePreparedStatement(db);
      resultSet = executeQuery();
      String schemaName = tableId != null ? tableId.tableName() : null;
      ResultSetMetaData metadata = resultSet.getMetaData();
      dialect.validateSpecificColumnTypes(metadata, columns);
      schemaMapping = SchemaMapping.create(schemaName, metadata, dialect);
    } else {
      log.trace("Current ResultSet {} isn't null. Continuing to seek.", resultSet.hashCode());
    }

    // This is called everytime during poll() before extracting records,
    // to ensure that the previous run succeeded, allowing us to move the committedOffset forward.
    // This action is a no-op for the first poll()
    this.committedOffset = this.offset;
    log.trace("Set the committed offset: {}",
        committedOffset.getChangeVersionOffset(dialect, db, tableId));
  }

  private void findPrimaryKeyColumn(Connection db) throws SQLException {
    for (ColumnDefinition defn : dialect.describeColumns(
            db,
            tableId.catalogName(),
            tableId.schemaName(),
            tableId.tableName(),
            null).values()) {
      if (defn.isPrimaryKey()) {
        primaryKeyColumn = defn.id();
        break;
      }
    }
  }

  private void findAllNonPKColumns(Connection db) throws SQLException {
    columns = new ArrayList<>();
    for (ColumnDefinition defn : dialect.describeColumns(
            db,
            tableId.catalogName(),
            tableId.schemaName(),
            tableId.tableName(),
            null).values()) {
      if (!defn.isPrimaryKey()) {
        columns.add(defn.id());
      }
    }
  }

  private ChangeTrackingOffset extractOffset() throws SQLException {
    return new ChangeTrackingOffset(
        resultSet.getLong(ChangeTrackingOffset.CHANGE_TRACKING_OFFSET_FIELD));
  }
}
