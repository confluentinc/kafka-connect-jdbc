/*
 * Copyright 2026 Confluent Inc.
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

package io.confluent.connect.jdbc.sink;

import java.util.Collection;
import java.util.Optional;

import io.confluent.connect.jdbc.dialect.DatabaseDialect;
import io.confluent.connect.jdbc.util.ColumnId;
import io.confluent.connect.jdbc.util.SafeText;
import io.confluent.connect.jdbc.util.TableId;

public final class SafeSqlContext {

  public enum Operation {
    INSERT,
    UPSERT,
    UPDATE,
    DELETE
  }

  private final String safeStatement;
  private final String qualifiedTable;
  private final String vendorPrefix;

  private SafeSqlContext(String safeStatement, String qualifiedTable, String vendorPrefix) {
    this.safeStatement = safeStatement;
    this.qualifiedTable = qualifiedTable;
    this.vendorPrefix = vendorPrefix;
  }

  public String safeStatement() {
    return safeStatement;
  }

  public String qualifiedTable() {
    return qualifiedTable;
  }

  public String vendorPrefix() {
    return vendorPrefix;
  }

  public static Optional<SafeSqlContext> create(
      DatabaseDialect dialect,
      TableId tableId,
      Collection<ColumnId> keyColumns,
      Collection<ColumnId> nonKeyColumns,
      Operation op,
      String vendorPrefix
  ) {
    final String statement;
    try {
      switch (op) {
        case INSERT:
          statement = dialect.buildInsertStatement(tableId, keyColumns, nonKeyColumns);
          break;
        case UPSERT:
          statement = dialect.buildUpsertQueryStatement(tableId, keyColumns, nonKeyColumns);
          break;
        case UPDATE:
          statement = dialect.buildUpdateStatement(tableId, keyColumns, nonKeyColumns);
          break;
        case DELETE:
          statement = dialect.buildDeleteStatement(tableId, keyColumns);
          break;
        default:
          return Optional.empty();
      }
    } catch (RuntimeException ex) {
      return Optional.empty();
    }
    if (statement == null) {
      return Optional.empty();
    }
    String rendered = statement.replace("?", "<redacted>")
        .replaceAll("<redacted>(, <redacted>)+", "<redacted>");
    if (!SafeText.isSafe(rendered)) {
      return Optional.empty();
    }
    return Optional.of(new SafeSqlContext(rendered, tableId.toString(), vendorPrefix));
  }
}
