/*
 * Copyright 2021 Confluent Inc.
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

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.connect.data.Date;
import org.apache.kafka.connect.data.Decimal;
import org.apache.kafka.connect.data.Time;
import org.apache.kafka.connect.data.Timestamp;

import java.util.Collection;

import io.confluent.connect.jdbc.sink.metadata.SinkRecordField;
import io.confluent.connect.jdbc.util.ColumnId;
import io.confluent.connect.jdbc.util.ExpressionBuilder;
import io.confluent.connect.jdbc.util.IdentifierRules;
import io.confluent.connect.jdbc.util.TableId;

/**
 * A {@link DatabaseDialect} for Firebird.
 *
 * @author <a href="mailto:vasiliy.yashkov@red-soft.ru">Vasiliy Yashkov</a>
 */
public class FirebirdDatabaseDialect extends GenericDatabaseDialect {

  /**
   * The provider for {@link FirebirdDatabaseDialect}.
   */
  public static class Provider extends DatabaseDialectProvider.SubprotocolBasedProvider {
    public Provider() {
      super(FirebirdDatabaseDialect.class.getSimpleName(), "firebird");
    }

    @Override
    public DatabaseDialect create(AbstractConfig config) {
      return new FirebirdDatabaseDialect(config);
    }
  }

  /**
   * Create a new dialect instance with the given connector configuration.
   *
   * @param config the connector configuration; may not be null
   */
  public FirebirdDatabaseDialect(AbstractConfig config) {
    super(config, new IdentifierRules(".", "\"", "\""));
  }

  /**
   * CURRENT_TIMESTAMP returns the current server date and time in the session time zone.
   * The default is 3 decimals, i.e. milliseconds precision.
   *
   * @return the query string
   */
  @Override
  protected String currentTimestampDatabaseQuery() {
    return "SELECT CURRENT_TIMESTAMP FROM RDB$DATABASE";
  }

  @Override
  protected String checkConnectionQuery() {
    return "SELECT 1 FROM RDB$DATABASE";
  }

  @Override
  protected String getSqlType(SinkRecordField field) {
    if (field.schemaName() != null) {
      switch (field.schemaName()) {
        case Decimal.LOGICAL_NAME:
          return "DECIMAL(18," + field.schemaParameters().get(Decimal.SCALE_FIELD) + ")";
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
        return "FLOAT";
      case FLOAT64:
        return "DOUBLE PRECISION";
      case BOOLEAN:
        return "BOOLEAN";
      case STRING:
        return "BLOB SUB_TYPE TEXT";
      case BYTES:
        return "BLOB SUB_TYPE BINARY";
      default:
        return super.getSqlType(field);
    }
  }

  @Override
  public String buildUpsertQueryStatement(
      final TableId table,
      Collection<ColumnId> keyColumns,
      Collection<ColumnId> nonKeyColumns
  ) {
    final ExpressionBuilder.Transform<ColumnId> transform = (builder, col) -> {
      builder.append(table)
          .append(".")
          .appendColumnName(col.name())
          .append("=SOURCE.")
          .appendColumnName(col.name());
    };

    ExpressionBuilder builder = expressionBuilder();
    builder.append("MERGE INTO ");
    builder.append(table);
    builder.append(" USING (SELECT ");
    builder.appendList()
        .delimitedBy(", ")
        .transformedBy(ExpressionBuilder.columnNamesWithPrefix("? "))
        .of(keyColumns, nonKeyColumns);
    builder.append(" FROM RDB$DATABASE) SOURCE ON(");
    builder.appendList()
        .delimitedBy(" AND ")
        .transformedBy(transform)
        .of(keyColumns);
    builder.append(")");
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
    builder.append(") VALUES(");
    builder.appendList()
        .delimitedBy(",")
        .transformedBy(ExpressionBuilder.columnNamesWithPrefix("SOURCE."))
        .of(nonKeyColumns, keyColumns);
    builder.append(")");
    return builder.toString();
  }
}
