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
import io.confluent.connect.jdbc.util.TableId;
import org.apache.kafka.common.config.AbstractConfig;

import java.util.Collection;

public class NetezzaDatabaseDialect extends GenericDatabaseDialect {

  /**
   * The provider for {@link NetezzaDatabaseDialect}.
   */
  public static class Provider extends DatabaseDialectProvider.SubprotocolBasedProvider {
    public Provider() {
      super(NetezzaDatabaseDialect.class.getSimpleName(), "ibm", "netezza");
    }

    @Override
    public DatabaseDialect create(AbstractConfig config) {
      return new NetezzaDatabaseDialect(config);
    }
  }

  public NetezzaDatabaseDialect(AbstractConfig config) {
    super(config);
  }

  @Override
  public String buildUpsertQueryStatement(
          final TableId table,
        Collection<ColumnId> keyColumns,
        Collection<ColumnId> nonKeyColumns
  ) {
    ExpressionBuilder builder = expressionBuilder();
    builder.append("merge into ");
    builder.append(table);
    builder.append(" AS target using (select ");
    builder.appendList()
            .delimitedBy(", ")
            .transformedBy(ExpressionBuilder.columnNamesWithPrefix("? "))
            .of(keyColumns, nonKeyColumns);
    builder.append(") AS incoming on(");
    builder.appendList()
            .delimitedBy(" and ")
            .transformedBy(this::transformAsUpdate)
            .of(keyColumns);
    builder.append(")");
    if (nonKeyColumns != null && !nonKeyColumns.isEmpty()) {
      builder.append(" when matched then update set ");
      builder.appendList()
                .delimitedBy(",")
                .transformedBy(this::transformAsUpdate)
                .of(nonKeyColumns);
    }
    builder.append(" when not matched then insert(");
    builder.appendList()
            .delimitedBy(",")
            .transformedBy(ExpressionBuilder.columnNames())
            .of(nonKeyColumns, keyColumns);
    builder.append(") values(");
    builder.appendList()
            .delimitedBy(",")
            .transformedBy(ExpressionBuilder.columnNamesWithPrefix("incoming."))
            .of(nonKeyColumns, keyColumns);
    builder.append(")");
    return builder.toString();
  }

  private void transformAsUpdate(ExpressionBuilder builder, ColumnId col) {
    builder.append("target.")
            .appendColumnName(col.name())
            .append("=incoming.")
            .appendColumnName(col.name());
  }
}