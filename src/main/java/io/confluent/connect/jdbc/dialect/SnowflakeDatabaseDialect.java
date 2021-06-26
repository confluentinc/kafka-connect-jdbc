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

package io.confluent.connect.jdbc.dialect;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Collection;

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.connect.data.Date;
import org.apache.kafka.connect.data.Decimal;
import org.apache.kafka.connect.data.Time;
import org.apache.kafka.connect.data.Timestamp;

import io.confluent.connect.jdbc.dialect.DatabaseDialectProvider.SubprotocolBasedProvider;
import io.confluent.connect.jdbc.sink.metadata.SinkRecordField;
import io.confluent.connect.jdbc.util.ColumnId;
import io.confluent.connect.jdbc.util.ExpressionBuilder;
import io.confluent.connect.jdbc.util.ExpressionBuilder.Transform;
import io.confluent.connect.jdbc.util.IdentifierRules;
import io.confluent.connect.jdbc.util.TableId;

/**
 * A {@link DatabaseDialect} for Snowflake.
 */
public class SnowflakeDatabaseDialect extends GenericDatabaseDialect {

  /**
   * The provider for {@link SnowflakeDatabaseDialect}.
   */
  public static class Provider extends SubprotocolBasedProvider {
    public Provider() {
      super(SnowflakeDatabaseDialect.class.getSimpleName(), "snowflake");
    }

    @Override
    public DatabaseDialect create(AbstractConfig config) {
      return new SnowflakeDatabaseDialect(config);
    }
  }

  /**
   * Create a new dialect instance with the given connector configuration.
   *
   * @param config the connector configuration; may not be null
   */
  public SnowflakeDatabaseDialect(AbstractConfig config) {
    super(config, new IdentifierRules(".", "\"", "\""));
  }

  /**
   * Perform any operations on a {@link PreparedStatement} before it is used. This is called from
   * the {@link #createPreparedStatement(Connection, String)} method after the statement is
   * created but before it is returned/used.
   *
   * <p>This method sets the {@link PreparedStatement#setFetchDirection(int) fetch direction}
   * to {@link ResultSet#FETCH_FORWARD forward} as an optimization for the driver to allow it to
   * scroll more efficiently through the result set and prevent out of memory errors.
   *
   * @param stmt the prepared statement; never null
   * @throws SQLException the error that might result from initialization
   */
  @Override
  protected void initializePreparedStatement(PreparedStatement stmt) throws SQLException {
    log.trace("Initializing PreparedStatement fetch direction to FETCH_FORWARD for '{}'", stmt);
    stmt.setFetchDirection(ResultSet.FETCH_FORWARD);
  }

  @Override
  protected String getSqlType(SinkRecordField field) {
    if (field.schemaName() != null) {
      switch (field.schemaName()) {
        case Decimal.LOGICAL_NAME:
          return "DECIMAL";
        case Date.LOGICAL_NAME:
          return "DATE";
        case Time.LOGICAL_NAME:
          return "TIME";
        case Timestamp.LOGICAL_NAME:
          return "TIMESTAMP";
        default:
          // fall through to normal types
      }
    }
    switch (field.schemaType()) {
      case INT8:
      case INT16:
        return "SMALLINT";
      case INT32:
        return "INT";
      case INT64:
        return "BIGINT";
      case FLOAT32:
        return "REAL";
      case FLOAT64:
        return "DOUBLE PRECISION";
      case BOOLEAN:
        return "BOOLEAN";
      case STRING:
        return "TEXT";
      case BYTES:
        return "ARRAY";
      default:
        return super.getSqlType(field);
    }
  }

  @Override
  public String buildUpsertQueryStatement(final TableId table, Collection<ColumnId> keyColumns,
                      Collection<ColumnId> nonKeyColumns) {
    final Transform<ColumnId> transform = (builder, col) -> builder.append(table)
        .append(".")
        .appendColumnName(col.name())
        .append("=INCOMING.")
        .appendColumnName(col.name());

    ExpressionBuilder builder = expressionBuilder();
    builder.append("MERGE INTO ");
    builder.append(table);
    builder.append(" USING (SELECT ");
    builder.appendList()
        .delimitedBy(", ")
        .transformedBy(ExpressionBuilder.columnNamesWithPrefix("? "))
        .of(keyColumns, nonKeyColumns);
    builder.append(" FROM dual) INCOMING ON ");
    builder.appendList()
        .delimitedBy(" AND ")
        .transformedBy(transform)
        .of(keyColumns);
    if (nonKeyColumns != null && !nonKeyColumns.isEmpty()) {
      builder.append(" WHEN MATCHED THEN UPDATE SET ");
      builder.appendList()
          .delimitedBy(",")
          .transformedBy(transform)
          .of(nonKeyColumns);
    }

    builder.append(" WHEN NOT MATCHED THEN INSERT(");
    builder.appendList()
        .delimitedBy(",")
        .of(nonKeyColumns, keyColumns);
    builder.append(") VALUES (");
    builder.appendList()
        .delimitedBy(",")
        .transformedBy(ExpressionBuilder.columnNamesWithPrefix("INCOMING."))
        .of(nonKeyColumns, keyColumns);
    builder.append(")");
    return builder.toString();
  }

}
