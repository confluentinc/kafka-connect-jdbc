/*
 * Copyright 2018 Confluent Inc.
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

package io.confluent.connect.jdbc.dialect;

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.connect.data.Date;
import org.apache.kafka.connect.data.Decimal;
import org.apache.kafka.connect.data.Timestamp;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import io.confluent.connect.jdbc.dialect.DatabaseDialectProvider.SubprotocolBasedProvider;
import io.confluent.connect.jdbc.sink.metadata.SinkRecordField;
import io.confluent.connect.jdbc.util.ColumnId;
import io.confluent.connect.jdbc.util.ExpressionBuilder;
import io.confluent.connect.jdbc.util.IdentifierRules;
import io.confluent.connect.jdbc.util.TableId;

/**
 * A {@link DatabaseDialect} for Exasol.
 */
public class ExasolDatabaseDialect extends GenericDatabaseDialect {

  /**
   * The provider for {@link ExasolDatabaseDialect}.
   */
  public static class Provider extends SubprotocolBasedProvider {
    public Provider() {
      super(ExasolDatabaseDialect.class.getSimpleName(), "exa");
    }

    @Override
    public DatabaseDialect create(AbstractConfig config) {
      return new ExasolDatabaseDialect(config);
    }
  }

  /**
   * Create a new dialect instance with the given connector configuration.
   *
   * @param config the connector configuration; may not be null
   */
  public ExasolDatabaseDialect(AbstractConfig config) {
    super(config, new IdentifierRules(".", "\"", "\""));
  }

  @Override
  protected String getSqlType(SinkRecordField field) {
    if (field.schemaName() != null) {
      switch (field.schemaName()) {
        case Decimal.LOGICAL_NAME:
          return "DECIMAL(36," + field.schemaParameters().get(Decimal.SCALE_FIELD) + ")";
        case Date.LOGICAL_NAME:
          return "DATE";
        // case Time.LOGICAL_NAME:
        // TIME is not supported (-> INT32)
        case Timestamp.LOGICAL_NAME:
          return "TIMESTAMP";
        default:
          // fall through to normal types
      }
    }
    switch (field.schemaType()) {
      case INT8:
        return "DECIMAL(3,0)";
      case INT16:
        return "DECIMAL(5,0)";
      case INT32:
        return "DECIMAL(10,0)";
      case INT64:
        return "DECIMAL(19,0)";
      case FLOAT32:
        return "FLOAT";
      case FLOAT64:
        return "DOUBLE";
      case BOOLEAN:
        return "BOOLEAN";
      case STRING:
        return "CLOB";
      // case BYTES:
      // BLOB is not supported
      default:
        return super.getSqlType(field);
    }
  }

  @Override
  public String buildDropTableStatement(
      TableId table,
      DropOptions options
  ) {
    ExpressionBuilder builder = expressionBuilder();

    builder.append("DROP TABLE");
    if (options.ifExists()) {
      builder.append(" IF EXISTS");
    }
    builder.append(" " + table);
    if (options.cascade()) {
      builder.append(" CASCADE CONSTRAINTS");
    }
    return builder.toString();
  }

  @Override
  public List<String> buildAlterTable(
      TableId table,
      Collection<SinkRecordField> fields
  ) {
    final List<String> queries = new ArrayList<>(fields.size());
    for (SinkRecordField field : fields) {
      queries.addAll(super.buildAlterTable(table, Collections.singleton(field)));
    }
    return queries;
  }

  @Override
  public String buildUpsertQueryStatement(
      TableId table,
      Collection<ColumnId> keyColumns,
      Collection<ColumnId> nonKeyColumns
  ) {
    ExpressionBuilder builder = expressionBuilder();
    builder.append("MERGE INTO ");
    builder.append(table);
    builder.append(" AS target USING (SELECT ");
    builder.appendList()
           .delimitedBy(", ")
           .transformedBy(ExpressionBuilder.columnNamesWithPrefix("? AS "))
           .of(keyColumns, nonKeyColumns);
    builder.append(") AS incoming ON (");
    builder.appendList()
           .delimitedBy(" AND ")
           .transformedBy(this::transformAs)
           .of(keyColumns);
    builder.append(")");
    if (nonKeyColumns != null && !nonKeyColumns.isEmpty()) {
      builder.append(" WHEN MATCHED THEN UPDATE SET ");
      builder.appendList()
             .delimitedBy(",")
             .transformedBy(this::transformUpdate)
             .of(nonKeyColumns);
    }
    builder.append(" WHEN NOT MATCHED THEN INSERT (");
    builder.appendList()
           .delimitedBy(",")
           .transformedBy(ExpressionBuilder.columnNames())
           .of(nonKeyColumns, keyColumns);
    builder.append(") VALUES (");
    builder.appendList()
           .delimitedBy(",")
           .transformedBy(ExpressionBuilder.columnNamesWithPrefix("incoming."))
           .of(nonKeyColumns, keyColumns);
    builder.append(")");
    return builder.toString();
  }

  private void transformAs(ExpressionBuilder builder, ColumnId col) {
    builder.append("target.")
           .appendIdentifierQuoted(col.name())
           .append("=incoming.")
           .appendIdentifierQuoted(col.name());
  }

  private void transformUpdate(ExpressionBuilder builder, ColumnId col) {
    builder.appendIdentifierQuoted(col.name())
           .append("=incoming.")
           .appendIdentifierQuoted(col.name());
  }

}
