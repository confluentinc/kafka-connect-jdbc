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

import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.regex.Pattern;

public enum DiagnosticCategory {
  UNIQUE_VIOLATION,
  NOT_NULL_VIOLATION,
  FOREIGN_KEY_VIOLATION,
  CHECK_VIOLATION,
  STRING_DATA_RIGHT_TRUNCATION,
  INVALID_TEXT_REPRESENTATION,
  INTEGRITY_CONSTRAINT_VIOLATION,
  CONNECTION_EXCEPTION,
  RAISE_EXCEPTION,
  SQL_ERROR;

  private static final Pattern SQL_STATE_PATTERN = Pattern.compile("^[0-9A-Z]{5}$");
  private static final Map<String, DiagnosticCategory> EXACT_CATEGORIES = new HashMap<>();
  private static final Map<String, DiagnosticCategory> CLASS_CATEGORIES = new HashMap<>();

  static {
    EXACT_CATEGORIES.put("23000", INTEGRITY_CONSTRAINT_VIOLATION);
    EXACT_CATEGORIES.put("23001", INTEGRITY_CONSTRAINT_VIOLATION);
    EXACT_CATEGORIES.put("23502", NOT_NULL_VIOLATION);
    EXACT_CATEGORIES.put("23503", FOREIGN_KEY_VIOLATION);
    EXACT_CATEGORIES.put("23505", UNIQUE_VIOLATION);
    EXACT_CATEGORIES.put("23514", CHECK_VIOLATION);
    EXACT_CATEGORIES.put("08000", CONNECTION_EXCEPTION);
    EXACT_CATEGORIES.put("08001", CONNECTION_EXCEPTION);
    EXACT_CATEGORIES.put("08003", CONNECTION_EXCEPTION);
    EXACT_CATEGORIES.put("08004", CONNECTION_EXCEPTION);
    EXACT_CATEGORIES.put("08006", CONNECTION_EXCEPTION);
    EXACT_CATEGORIES.put("08S01", CONNECTION_EXCEPTION);
    EXACT_CATEGORIES.put("22001", STRING_DATA_RIGHT_TRUNCATION);
    EXACT_CATEGORIES.put("22007", SQL_ERROR);
    EXACT_CATEGORIES.put("22008", SQL_ERROR);
    EXACT_CATEGORIES.put("22P02", INVALID_TEXT_REPRESENTATION);
    EXACT_CATEGORIES.put("P0001", RAISE_EXCEPTION);

    CLASS_CATEGORIES.put("23", INTEGRITY_CONSTRAINT_VIOLATION);
    CLASS_CATEGORIES.put("08", CONNECTION_EXCEPTION);
    CLASS_CATEGORIES.put("22", SQL_ERROR);
  }

  public String label() {
    return name().toLowerCase(Locale.ROOT);
  }

  public static Classification classify(String sqlState) {
    if (sqlState == null || !SQL_STATE_PATTERN.matcher(sqlState).matches()) {
      return new Classification(SQL_ERROR, null);
    }

    DiagnosticCategory exactCategory = EXACT_CATEGORIES.get(sqlState);
    if (exactCategory != null) {
      return new Classification(exactCategory, sqlState);
    }

    DiagnosticCategory classCategory = CLASS_CATEGORIES.get(sqlState.substring(0, 2));
    return new Classification(classCategory != null ? classCategory : SQL_ERROR, null);
  }

  public static final class Classification {
    public final DiagnosticCategory category;
    public final String canonicalSqlState;

    public Classification(DiagnosticCategory category, String canonicalSqlState) {
      this.category = category;
      this.canonicalSqlState = canonicalSqlState;
    }
  }
}
