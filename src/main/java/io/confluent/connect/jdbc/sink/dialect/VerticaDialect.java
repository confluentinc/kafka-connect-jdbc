/*
 * Copyright 2016 Confluent Inc.
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

package io.confluent.connect.jdbc.sink.dialect;

import org.apache.kafka.connect.data.Date;
import org.apache.kafka.connect.data.Decimal;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Time;
import org.apache.kafka.connect.data.Timestamp;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import io.confluent.connect.jdbc.sink.metadata.SinkRecordField;

import static io.confluent.connect.jdbc.sink.dialect.StringBuilderUtil.joinToBuilder;

public class VerticaDialect extends DbDialect {
  public VerticaDialect() {
    super("\"", "\"");
  }

  @Override
  protected String getSqlType(String schemaName, Map<String, String> parameters, Schema.Type type) {
    return getSqlType(schemaName, parameters, type, true);
  }

  private String getSqlType(
      String schemaName,
      Map<String, String> parameters,
      Schema.Type type,
      boolean sized
  ) {
    if (schemaName != null) {
      switch (schemaName) {
        case Decimal.LOGICAL_NAME:
          return "DECIMAL" + (sized ? "(18," + parameters.get(Decimal.SCALE_FIELD) + ")" : "");
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
    switch (type) {
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
        return super.getSqlType(schemaName, parameters, type);
    }
  }

  @Override
  public List<String> getAlterTable(String tableName, Collection<SinkRecordField> fields) {
    final List<String> queries = new ArrayList<>(fields.size());
    for (SinkRecordField field : fields) {
      queries.addAll(super.getAlterTable(tableName, Collections.singleton(field)));
    }
    return queries;
  }

  @Override
  public String getUpsertQuery(
      final String table,
      final Collection<String> keyCols,
      final Collection<String> cols,
      final Map<String, SinkRecordField> allFields
  ) {
    final StringBuilder builder = new StringBuilder();
    builder.append("MERGE INTO ");
    String tableName = escaped(table);
    builder.append(tableName);
    builder.append(" AS target USING (SELECT ");
    joinToBuilder(builder, ", ", keyCols, cols, new StringBuilderUtil.Transform<String>() {
      @Override
      public void apply(StringBuilder builder, String col) {
        SinkRecordField field = allFields.get(col);
        String colType = getSqlType(
            field.schemaName(),
            field.schemaParameters(),
            field.schemaType(),
            false
        );
        builder.append("?::").append(colType).append(" AS ").append(escaped(col));
      }
    });
    builder.append(") AS incoming ON (");
    joinToBuilder(builder, " AND ", keyCols, new StringBuilderUtil.Transform<String>() {
      @Override
      public void apply(StringBuilder builder, String col) {
        builder.append("target.").append(escaped(col)).append("=incoming.").append(escaped(col));
      }
    });
    builder.append(")");
    builder.append(" WHEN MATCHED THEN UPDATE SET ");
    joinToBuilder(builder, ",", cols, keyCols, new StringBuilderUtil.Transform<String>() {
      @Override
      public void apply(StringBuilder builder, String col) {
        builder.append(escaped(col)).append("=incoming.").append(escaped(col));
      }
    });
    builder.append(" WHEN NOT MATCHED THEN INSERT (");
    joinToBuilder(builder, ", ", cols, keyCols, escaper());
    builder.append(") VALUES (");
    joinToBuilder(builder, ",", cols, keyCols, prefixedEscaper("incoming."));
    builder.append(");");
    return builder.toString();
  }
}
