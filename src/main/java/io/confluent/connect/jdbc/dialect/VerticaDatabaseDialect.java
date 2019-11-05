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

import io.confluent.connect.jdbc.util.ColumnId;
import io.confluent.connect.jdbc.util.ExpressionBuilder;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.connect.data.Date;
import org.apache.kafka.connect.data.Decimal;
import org.apache.kafka.connect.data.Time;
import org.apache.kafka.connect.data.Timestamp;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import io.confluent.connect.jdbc.dialect.DatabaseDialectProvider.SubprotocolBasedProvider;
import io.confluent.connect.jdbc.sink.metadata.SinkRecordField;
import io.confluent.connect.jdbc.util.IdentifierRules;
import io.confluent.connect.jdbc.util.TableId;

/**
 * A {@link DatabaseDialect} for Vertica.
 */
public class VerticaDatabaseDialect extends GenericDatabaseDialect {
  /**
   * The provider for {@link VerticaDatabaseDialect}.
   */
  public static class Provider extends SubprotocolBasedProvider {
    public Provider() {
      super(VerticaDatabaseDialect.class.getSimpleName(), "vertica");
    }

    @Override
    public DatabaseDialect create(AbstractConfig config) {
      return new VerticaDatabaseDialect(config);
    }
  }

  /**
   * Create a new dialect instance with the given connector configuration.
   *
   * @param config the connector configuration; may not be null
   */
  public VerticaDatabaseDialect(AbstractConfig config) {
    super(config, new IdentifierRules(".", "\"", "\""));
  }

  @Override
  protected String getSqlType(SinkRecordField field) {
    return getSqlType(field, true);
  }

  private String getSqlType(SinkRecordField field, boolean sized) {
    if (field.schemaName() != null) {
      switch (field.schemaName()) {
        case Decimal.LOGICAL_NAME:
          return "DECIMAL" + (
                    sized ? "(18," + field.schemaParameters().get(Decimal.SCALE_FIELD) + ")"
                            : ""
            );
        case Date.LOGICAL_NAME:
          return "DATE";
        case Time.LOGICAL_NAME:
          return "TIME";
        case Timestamp.LOGICAL_NAME:
          return "TIMESTAMP";
        default:
          // fall through to non-logical types
          break;
      }
    }
    switch (field.schemaType()) {
      case INT8:
        return "INT";
      case INT16:
        return "INT";
      case INT32:
        return "INT";
      case INT64:
        return "INT";
      case FLOAT32:
        return "FLOAT";
      case FLOAT64:
        return "FLOAT";
      case BOOLEAN:
        return "BOOLEAN";
      case STRING:
        return "VARCHAR" + (sized ? "(1024)" : "");
      case BYTES:
        return "VARBINARY" + (sized ? "(1024)" : "");
      default:
        return super.getSqlType(field);
    }
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
          Collection<ColumnId> nonKeyColumns,
          Map<String, SinkRecordField> allFields
  ) {
    ExpressionBuilder builder = expressionBuilder();
    builder.append("MERGE INTO ");
    builder.append(table);
    builder.append(" AS target USING (SELECT ");
    builder.appendList()
            .delimitedBy(", ")
            .transformedBy((b, input) -> transformTypedParam(b, (ColumnId) input, allFields))
            .of(keyColumns, nonKeyColumns);
    builder.append(") AS incoming ON (");
    builder.appendList()
            .delimitedBy(" AND ")
            .transformedBy(this::transformAs)
            .of(keyColumns);
    builder.append(")");
    builder.append(" WHEN MATCHED THEN UPDATE SET ");
    builder.appendList()
            .delimitedBy(",")
            .transformedBy(this::transformUpdate)
            .of(nonKeyColumns, keyColumns);
    builder.append(" WHEN NOT MATCHED THEN INSERT (");
    builder.appendList()
            .delimitedBy(", ")
            .transformedBy(ExpressionBuilder.columnNamesWithPrefix(""))
            .of(nonKeyColumns, keyColumns);
    builder.append(") VALUES (");
    builder.appendList()
            .delimitedBy(",")
            .transformedBy(ExpressionBuilder.columnNamesWithPrefix("incoming."))
            .of(nonKeyColumns, keyColumns);
    builder.append(");");
    return builder.toString();
  }

  private void transformAs(ExpressionBuilder builder, ColumnId col) {
    builder.append("target.")
            .appendColumnName(col.name())
            .append("=incoming.")
            .appendColumnName(col.name());
  }

  private void transformUpdate(ExpressionBuilder builder, ColumnId col) {
    builder.appendColumnName(col.name())
            .append("=incoming.")
            .appendColumnName(col.name());
  }

  private void transformTypedParam(
          ExpressionBuilder builder,
          ColumnId col,
          Map<String, SinkRecordField> allFields
  ) {
    SinkRecordField field = allFields.get(col.name());

    builder.append("?::")
            .append(getSqlType(field, false))
            .append(" AS ")
            .appendColumnName(col.name());
  }
}
