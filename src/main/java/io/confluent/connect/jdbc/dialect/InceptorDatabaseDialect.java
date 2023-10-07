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

import io.confluent.connect.jdbc.sink.metadata.SinkRecordField;
import io.confluent.connect.jdbc.util.*;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.connect.data.Date;
import org.apache.kafka.connect.data.Decimal;
import org.apache.kafka.connect.data.Time;
import org.apache.kafka.connect.data.Timestamp;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;

public class InceptorDatabaseDialect extends GenericDatabaseDialect {
  private static final Logger log = LoggerFactory.getLogger(InceptorDatabaseDialect.class);

  public static class Provider extends DatabaseDialectProvider.SubprotocolBasedProvider {
    public Provider() {
      super(InceptorDatabaseDialect.class.getSimpleName(), "hive2");
    }

    @Override
    public DatabaseDialect create(AbstractConfig config) {
      return new InceptorDatabaseDialect(config);
    }
  }

  /**
   * dialect like mysql
   *
   * @param config just config
   */
  public InceptorDatabaseDialect(AbstractConfig config) {
    super(config, new IdentifierRules(".", "", ""));
  }

  @Override
  protected String currentTimestampDatabaseQuery() {
    return "select CURRENT_TIMESTAMP from system.dual";
  }

  @Override
  protected String checkConnectionQuery() {
    return "SELECT 1 FROM system.dual";
  }

  @Override
  protected String getSqlType(SinkRecordField field) {
    if (field.schemaName() != null) {
      switch (field.schemaName()) {
        case Decimal.LOGICAL_NAME:
          // Maximum precision supported by Inceptor is 38
          int scale = Integer.parseInt(field.schemaParameters().get(Decimal.SCALE_FIELD));
          return "DECIMAL(38," + scale + ")";
        case Date.LOGICAL_NAME:
          return "DATE";
        case Time.LOGICAL_NAME:
          return "TIME";
        case Timestamp.LOGICAL_NAME:
          return "TIMESTAMP";
        default:
          // pass through to primitive types
      }
    }
    switch (field.schemaType()) {
      case INT8:
        return "TINYINT";
      case INT16:
        return "SMALLINT";
      case INT32:
        return "INT";
      case INT64:
        return "BIGINT";
      case FLOAT32:
        return "FLOAT";
      case FLOAT64:
        return "DOUBLE";
      case BOOLEAN:
        return "BOOLEAN";
      case STRING:
        return "STRING";
      case BYTES:
        return "VARBINARY(1024)";
      default:
        return super.getSqlType(field);
    }
  }

  @Override
  public String buildInsertStatement(
          TableId table,
          String pkName,
          Collection<ColumnId> keyColumns,
          Collection<ColumnId> nonKeyColumns
  ) {
    ExpressionBuilder builder = new ExpressionBuilder(new IdentifierRules(".", "", ""));
    builder.append("INSERT INTO ");
    builder.append(table,QuoteMethod.NEVER);
    builder.append("(");
    builder.append(pkName + ",");
    builder.appendList()
            .delimitedBy(",")
            .transformedBy(ExpressionBuilder.columnNames())
            .of(keyColumns, nonKeyColumns);
    builder.append(") VALUES(");
    builder.appendMultiple(",", "?", 1 + keyColumns.size() + nonKeyColumns.size());
    builder.append(")");
    return builder.toString();
  }

  @Override
  public String buildInsertStatement(
          TableId table,
          Collection<ColumnId> keyColumns,
          Collection<ColumnId> nonKeyColumns
  ) {
    ExpressionBuilder builder = new ExpressionBuilder(new IdentifierRules(".", "", ""));
    builder.append("INSERT INTO ");
    builder.append(table,QuoteMethod.NEVER);
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
  public String buildDesertStatement(
          TableId table,
          String pkName
  ) {
    ExpressionBuilder builder = new ExpressionBuilder(new IdentifierRules(".", "", ""));
    builder.append("DELETE FROM ");
    builder.append(table,QuoteMethod.NEVER);
    builder.append(" WHERE ");
    builder.append(pkName);
    builder.append(" in (?)");
    return builder.toString();
  }

}
