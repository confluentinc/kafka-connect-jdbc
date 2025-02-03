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

import io.confluent.connect.jdbc.dialect.DatabaseDialectProvider.SubprotocolBasedProvider;
//import io.confluent.connect.jdbc.sink.metadata.SinkRecordField;
//import io.confluent.connect.jdbc.source.ColumnMapping;
//import io.confluent.connect.jdbc.util.ColumnDefinition;
import io.confluent.connect.jdbc.util.ColumnId;
import io.confluent.connect.jdbc.util.ExpressionBuilder;
import io.confluent.connect.jdbc.util.ExpressionBuilder.Transform;
//import io.confluent.connect.jdbc.util.IdentifierRules;
//import io.confluent.connect.jdbc.util.QuoteMethod;
import io.confluent.connect.jdbc.util.TableDefinition;
import io.confluent.connect.jdbc.util.TableId;
//import io.confluent.connect.jdbc.util.ExpressionBuilder.Transform;
import org.apache.kafka.common.config.AbstractConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.Collection;


/**
 * A {@link DatabaseDialect} for PostgreSQL.
 */
public class RedshiftDatabaseDialect extends PostgreSqlDatabaseDialect {

  private static final Logger log = LoggerFactory.getLogger(RedshiftDatabaseDialect.class);

  /**
   * The provider for {@link RedshiftDatabaseDialect}.
   */
  public static class Provider extends SubprotocolBasedProvider {
    public Provider() {
      super(RedshiftDatabaseDialect.class.getSimpleName(), "redshift");
    }

    @Override
    public DatabaseDialect create(AbstractConfig config) {
      return new RedshiftDatabaseDialect(config);
    }
  }

  /**
   * Create a new dialect instance with the given connector configuration.
   *
   * @param config the connector configuration; may not be null
   */
  public RedshiftDatabaseDialect(AbstractConfig config) {
    super(config);
  }

  @Override
  public String buildUpsertQueryStatement(
      TableId table,
      Collection<ColumnId> keyColumns,
      Collection<ColumnId> nonKeyColumns,
      TableDefinition definition
  ) {
    final Transform<ColumnId> transform = (builder, col) -> {
      builder.appendColumnName(col.name())
             .append("=EXCLUDED.")
             .appendColumnName(col.name());
    };

    ExpressionBuilder builder = expressionBuilder();
    builder.append("INSERT INTO ");
    builder.append(table);
    builder.append(" (");
    builder.appendList()
           .delimitedBy(",")
           .transformedBy(ExpressionBuilder.columnNames())
           .of(keyColumns, nonKeyColumns);
    builder.append(") VALUES (");
    builder.appendList()
           .delimitedBy(",")
           .transformedBy(this.columnValueVariables(definition))
           .of(keyColumns, nonKeyColumns);
    builder.append(") ON CONFLICT (");
    builder.appendList()
           .delimitedBy(",")
           .transformedBy(ExpressionBuilder.columnNames())
           .of(keyColumns);
    if (nonKeyColumns.isEmpty()) {
      builder.append(") DO NOTHING");
    } else {
      builder.append(") DO UPDATE SET ");
      builder.appendList()
              .delimitedBy(",")
              .transformedBy(transform)
              .of(nonKeyColumns);
    }
    return builder.toString();
  }
}
