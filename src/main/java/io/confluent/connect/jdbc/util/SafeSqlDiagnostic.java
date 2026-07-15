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

package io.confluent.connect.jdbc.util;

public final class SafeSqlDiagnostic {

  public enum Kind {
    SQL_EXCEPTION,
    BATCH_UPDATE_EXCEPTION,
    OTHER
  }

  public final String message;
  public final String canonicalSqlState;

  private SafeSqlDiagnostic(String message, String canonicalSqlState) {
    this.message = message;
    this.canonicalSqlState = canonicalSqlState;
  }

  public static SafeSqlDiagnostic skeleton(
      Kind kind,
      DiagnosticCategory category,
      String canonicalSqlState
  ) {
    String message = "<redacted> [type=" + wire(kind) + ", category=" + category.label()
        + (canonicalSqlState != null ? ", sqlState=" + canonicalSqlState : "") + "]";
    return new SafeSqlDiagnostic(message, canonicalSqlState);
  }

  public static SafeSqlDiagnostic reconstructed(
      String safeStatement,
      DiagnosticCategory category,
      String canonicalSqlState,
      String oracleTag
  ) {
    String message = safeStatement + ": ERROR: " + category.label()
        + (canonicalSqlState != null ? " [sqlState=" + canonicalSqlState + "]" : "")
        + (oracleTag != null ? " [" + oracleTag + "]" : "");
    if (!SafeText.isSafe(message)) {
      return skeleton(Kind.SQL_EXCEPTION, category, canonicalSqlState);
    }
    return new SafeSqlDiagnostic(message, canonicalSqlState);
  }

  private static String wire(Kind kind) {
    switch (kind) {
      case SQL_EXCEPTION:
        return "SQLException";
      case BATCH_UPDATE_EXCEPTION:
        return "BatchUpdateException";
      case OTHER:
        return "Throwable";
      default:
        throw new AssertionError("Unknown diagnostic kind: " + kind);
    }
  }
}
