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

package io.confluent.connect.jdbc.dialect;

import io.confluent.connect.jdbc.sink.metadata.SinkRecordField;
import io.confluent.connect.jdbc.util.ColumnId;
import io.confluent.connect.jdbc.util.ExpressionBuilder;
import io.confluent.connect.jdbc.util.IdentifierRules;
import io.confluent.connect.jdbc.util.TableId;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.connect.data.Date;
import org.apache.kafka.connect.data.Decimal;
import org.apache.kafka.connect.data.Time;
import org.apache.kafka.connect.data.Timestamp;

import java.util.Collection;
import java.util.Collections;
import java.util.List;

/**
 * A {@link DatabaseDialect} for Snowflake.
 */
public class SnowflakeDatabaseDialect extends GenericDatabaseDialect {
  /**
   * The provider for {@link SnowflakeDatabaseDialect}.
   */
  public static class Provider extends DatabaseDialectProvider.SubprotocolBasedProvider {
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

  @Override
  public boolean shouldLoadMergeOnUpsert() {
    return true;
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
          return "TIMESTAMP_LTZ";
        default:
          // fall through to normal types
      }
    }
    switch (field.schemaType()) {
      case INT8:
        return "TINYINT";
      case INT16:
        return "SMALLINT";
      case INT32:
        return "INTEGER";
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
        return "BINARY";
      default:
        return super.getSqlType(field);
    }
  }

  @Override
  public String buildCreateTempTableStatement(
          TableId table,
          Collection<SinkRecordField> fields
  ) {
    ExpressionBuilder builder = expressionBuilder();

    builder.append("CREATE TEMPORARY TABLE ");
    builder.append(table);
    builder.append(" (");
    writeColumnsSpec(builder, fields);
    builder.append(")");
    return builder.toString();
  }

  @Override
  public String buildInsertStatement(
          TableId table,
          Collection<ColumnId> keyColumns,
          Collection<ColumnId> nonKeyColumns
  ) {
    ExpressionBuilder builder = expressionBuilder();
    builder.append("INSERT INTO ");
    builder.append(table);
    builder.append("(");
    builder.appendList()
            .delimitedBy(",")
            .transformedBy(ExpressionBuilder.columnNames())
            .of(keyColumns, nonKeyColumns);
    builder.append(") VALUES(");
    builder.appendMultiple(",", "?", keyColumns.size() + nonKeyColumns.size());
    builder.append(")");
    return builder.toString();
  }

  @Override
  public String buildTempTableMergeQueryStatement(
          TableId table,
          TableId tempTable,
          Collection<ColumnId> keyColumns,
          Collection<ColumnId> nonKeyColumns
  ) {
    final ExpressionBuilder.Transform<ColumnId> transform = (builder, col) -> {
      builder.append(table)
              .append(".")
              .appendColumnName(col.name())
              .append("=src.")
              .appendColumnName(col.name());
    };
    ExpressionBuilder builder = expressionBuilder();
    builder.append("merge into ");
    builder.append(table);
    builder.append(" using(");
    builder.append(tempTable);
    builder.append(")as src on(");
    builder.appendList()
            .delimitedBy(" and ")
            .transformedBy(transform)
            .of(keyColumns);
    builder.append(")");
    if (nonKeyColumns != null && !nonKeyColumns.isEmpty()) {
      builder.append("when matched then update set ");
      builder.appendList()
              .delimitedBy(",")
              .transformedBy(transform)
              .of(nonKeyColumns);
    }
    builder.append(" when not matched then insert(");
    builder.appendList()
            .delimitedBy(",")
            .of(nonKeyColumns, keyColumns);
    builder.append(")values(");
    builder.appendList()
            .delimitedBy(",")
            .transformedBy(ExpressionBuilder.columnNamesWithPrefix("src."))
            .of(nonKeyColumns, keyColumns);
    builder.append(")");
    return builder.toString();
  }

  @Override
  public List<String> buildAlterTable(
          TableId table,
          Collection<SinkRecordField> fields
  ) {
    final boolean newlines = fields.size() > 1;

    final ExpressionBuilder.Transform<SinkRecordField> transform = (builder, field) -> {
      if (newlines) {
        builder.appendNewLine();
      }
      writeColumnSpec(builder, field);
    };

    ExpressionBuilder builder = expressionBuilder();
    builder.append("ALTER TABLE ");
    builder.append(table);
    builder.append(" ADD ");
    builder.appendList()
            .delimitedBy(",")
            .transformedBy(transform)
            .of(fields);
    return Collections.singletonList(builder.toString());
  }

  @Override
  public String buildDropTableStatement(
          TableId table,
          DropOptions options
  ) {
    ExpressionBuilder builder = expressionBuilder();

    builder.append("DROP TABLE ");
    if (options.ifExists()) {
      builder.append("IF EXISTS ");
    }
    builder.append(table);
    if (options.cascade()) {
      builder.append(" CASCADE");
    }
    return builder.toString();
  }
}