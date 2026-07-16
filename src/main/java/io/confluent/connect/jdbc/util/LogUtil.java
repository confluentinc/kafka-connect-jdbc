/*
 * Copyright 2022 Confluent Inc.
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

import java.sql.BatchUpdateException;
import java.sql.SQLException;

/**
 * A stop-gap utility class to find a tradeoff between 2 things: To have reasonably good exception/
 * error information to investigate incidents while at the same time avoid logging sensitive data.
 */
public class LogUtil {
  private static final String REDACTED_VALUE = "<redacted>";

  public static SQLException trimSensitiveData(SQLException e) {
    return (SQLException) trimSensitiveData((Throwable)e);
  }

  public static Throwable trimSensitiveData(Throwable t) {
    if (!(t instanceof SQLException)) {
      // t is not a SQLException; return as-is.
      // This is also the recursion termination condition i.e. when t is null.
      return t;
    }

    if (!(t instanceof BatchUpdateException)) {
      // t is a SQLException, but not BatchUpdateException.
      SQLException oldSqe = (SQLException)t;
      SQLException newSqe = new SQLException(oldSqe.getLocalizedMessage());
      newSqe.setNextException(trimSensitiveData(oldSqe.getNextException()));
      return newSqe;
    }

    // At this point t is BatchUpdateException; return a new trimmed version of it.
    BatchUpdateException e = (BatchUpdateException)t;
    return new BatchUpdateException(getNonSensitiveErrorMessage(e.getLocalizedMessage()),
        e.getUpdateCounts());
  }

  // Structured ServerErrorMessage labels — only ever appear at field boundaries, so safe to trust.
  // Redshift's redshift-jdbc42 driver is a pgjdbc fork and emits the same shape.
  private static final String[] STRUCTURED_END_MARKERS = {
      "\n  Detail: ",
      "\n  Hint: "
  };

  // pgjdbc BatchResultHandler suffix (reused verbatim by redshift-jdbc42). Free-form sentence text,
  // so used only as a fallback when no structured label is present — a reason could plausibly
  // contain this phrase (e.g., a trigger's RAISE EXCEPTION message), in which case earliest-wins
  // across both tiers would truncate the reason mid-sentence.
  private static final String BATCH_SUFFIX_FALLBACK = "  Call getNextException ";

  public static SQLException redactSensitiveData(SQLException e) {
    return (SQLException) redactSensitiveData((Throwable) e);
  }

  public static Throwable redactSensitiveData(Throwable t) {
    if (!(t instanceof SQLException)) {
      return t;
    }

    if (!(t instanceof BatchUpdateException)) {
      // t is a SQLException, but not BatchUpdateException.
      SQLException oldSqlException = (SQLException) t;
      SQLException newSqlException =
          new SQLException(
              REDACTED_VALUE, oldSqlException.getSQLState(), oldSqlException.getErrorCode());
      newSqlException.setNextException(redactSensitiveData(oldSqlException.getNextException()));
      newSqlException.setStackTrace(oldSqlException.getStackTrace());
      return newSqlException;
    }

    // At this point t is BatchUpdateException; redact its message too.
    BatchUpdateException oldBatchUpdateException = (BatchUpdateException) t;
    BatchUpdateException newBatchUpdateException =
        new BatchUpdateException(
            REDACTED_VALUE,
            oldBatchUpdateException.getSQLState(),
            oldBatchUpdateException.getErrorCode(),
            oldBatchUpdateException.getUpdateCounts());
    newBatchUpdateException.setNextException(
        redactSensitiveData(oldBatchUpdateException.getNextException()));
    newBatchUpdateException.setStackTrace(oldBatchUpdateException.getStackTrace());
    return newBatchUpdateException;
  }

  // This implementation assumes it to be Postgres, see toString() of ServerErrorMessage.java
  // as well as the constructor of PSQLException.java with "boolean detail" flag in
  // https://github.com/pgjdbc/pgjdbc/blob/master/pgjdbc/src/main/java/org/postgresql/util/
  // Redshift's redshift-jdbc42 driver is a pgjdbc fork that emits the same message shape,
  // including the BatchResultHandler "Call getNextException" suffix used as the Tier 2 fallback.
  // For other JDBC Databases it would not fail but might return the same input string back!
  private static String getNonSensitiveErrorMessage(String errMsg) {
    final String sensitiveStartSearchText = ") VALUES (";
    final String errorStartSearchText = ": ERROR: ";

    if (errMsg == null) {
      return null;
    }

    final int trimStartIdx = 0;
    final int trimEndIdx = errMsg.indexOf(sensitiveStartSearchText);
    if (trimEndIdx < 0) {
      return errMsg;
    }

    String msg1 = errMsg.substring(trimStartIdx, trimEndIdx + 1);

    int errorStartIdx = errMsg.indexOf(errorStartSearchText);
    if (errorStartIdx < trimEndIdx) {
      return msg1;
    }

    // Tier 1: structured server-side field labels. Earliest match wins between them.
    int errorEndIdx = -1;
    for (String marker : STRUCTURED_END_MARKERS) {
      int idx = errMsg.indexOf(marker, errorStartIdx);
      if (idx > 0 && (errorEndIdx < 0 || idx < errorEndIdx)) {
        errorEndIdx = idx;
      }
    }
    // Tier 2: fall back to the BatchResultHandler suffix only if no structured marker matched.
    if (errorEndIdx < 0) {
      errorEndIdx = errMsg.indexOf(BATCH_SUFFIX_FALLBACK, errorStartIdx);
    }
    if (errorEndIdx < 0) {
      return msg1;
    }

    return msg1 + errMsg.substring(errorStartIdx, errorEndIdx);
  }

  public static String maybeRedact(boolean shouldRedactSensitiveLogs, String msg) {
    if (shouldRedactSensitiveLogs) {
      return REDACTED_VALUE;
    }
    return String.valueOf(msg);
  }
}
