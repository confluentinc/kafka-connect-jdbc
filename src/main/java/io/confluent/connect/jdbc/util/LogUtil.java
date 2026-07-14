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
import java.util.regex.Matcher;
import java.util.regex.Pattern;

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

  public static SQLException sanitizeSensitiveData(SQLException e) {
    return (SQLException) sanitizeSensitiveData((Throwable) e);
  }

  public static Throwable sanitizeSensitiveData(Throwable t) {
    if (!(t instanceof SQLException)) {
      // Also the recursion termination condition, i.e. when t is null.
      return t;
    }
    SQLException sqe = (SQLException) t;
    Throwable next = sanitizeSensitiveData(sqe.getNextException());
    SQLException out;
    if (t instanceof BatchUpdateException) {
      BatchUpdateException b = (BatchUpdateException) t;
      out = new BatchUpdateException(
          sanitizeMessage(b.getMessage()), b.getSQLState(), b.getErrorCode(),
          b.getUpdateCounts());
    } else {
      out = new SQLException(
          sanitizeMessage(sqe.getMessage()), sqe.getSQLState(), sqe.getErrorCode());
    }
    if (next instanceof SQLException) {
      out.setNextException((SQLException) next);
    }
    out.setStackTrace(sqe.getStackTrace());
    return out;
  }

  // Token that begins a value region inside a "Batch entry ... was aborted" statement.
  private static final Pattern VALUE_REGION =
      Pattern.compile("\\s(VALUES\\s*\\(|SET\\s|WHERE\\s)", Pattern.CASE_INSENSITIVE);
  private static final String ABORTED_MARKER = " was aborted";
  private static final String ERROR_MARKER = ": ERROR:";

  // Right-edge markers that bound the safe ERROR reason; earliest match across all of them wins.
  private static final Pattern[] REASON_END_MARKERS = {
      Pattern.compile("\\n\\s*Detail:"),
      Pattern.compile("\\n\\s*Hint:"),
      Pattern.compile("\\n\\s*Where:"),
      Pattern.compile("\\n\\s*Position:"),
      Pattern.compile("\\s{2}Call getNextException"),
  };

  // Value-group redaction patterns applied after the statement-trim pass; these catch unquoted
  // numerics/uuids that the single-quoted-literal pass misses because they aren't quoted.
  private static final Pattern DETAIL_FAILING_ROW =
      Pattern.compile("(Detail:\\s*Failing row contains \\()[^\\n]*?(\\)\\.?)");
  private static final Pattern DETAIL_KEY =
      Pattern.compile("(Detail:\\s*Key\\s*\\([^)]*\\)=\\()[^\\n]*?(\\))");
  private static final Pattern DUPLICATE_KEY_VALUE_IS =
      Pattern.compile("(The duplicate key value is \\()[^\\n]*?(\\)\\.?)");
  private static final Pattern EQUALS_PAREN =
      Pattern.compile("(=\\s*\\()[^)]*?(\\))");

  // Single-quoted literal redaction; driver-agnostic. Identifiers use "" / `` / [] so are
  // untouched by this pattern. Handles doubled '' escapes inside the literal.
  private static final Pattern SINGLE_QUOTED = Pattern.compile("'(?:[^']|'')*'");

  /**
   * Package-private so it can be unit-tested directly. Three cooperating passes:
   * (1) trim the pgjdbc/Redshift batch statement down to its safe head + bounded ERROR reason,
   * (2) redact unquoted value groups in known positions,
   * (3) redact single-quoted literals.
   */
  static String sanitizeMessage(String msg) {
    if (msg == null) {
      return null;
    }
    String base = trimBatchStatement(msg);
    if (base == null) {
      base = msg;
    }
    base = redactValueGroups(base);
    base = redactSingleQuoted(base);
    return base;
  }

  // Only for the pgjdbc/Redshift "Batch entry N <verb> ... was aborted" shape. Returns null
  // (no-op / fall through to raw message) if the message doesn't match that shape.
  private static String trimBatchStatement(String msg) {
    if (!msg.trim().startsWith("Batch entry")) {
      return null;
    }
    int abortedIdx = msg.indexOf(ABORTED_MARKER);
    if (abortedIdx < 0) {
      return null;
    }
    Matcher valueRegion = VALUE_REGION.matcher(msg);
    int headEnd = (valueRegion.find() && valueRegion.start() < abortedIdx)
        ? valueRegion.start() : abortedIdx;
    String head = msg.substring(0, headEnd);

    int errIdx = msg.indexOf(ERROR_MARKER, abortedIdx);
    if (errIdx < 0) {
      // Fail-closed: no reason marker found, keep only the statement head.
      return head;
    }
    return head + boundedReason(msg, errIdx);
  }

  // Reason text starting at `start` (index of ": ERROR:"), cut at the earliest right-edge marker.
  private static String boundedReason(String msg, int start) {
    int end = msg.length();
    for (Pattern pattern : REASON_END_MARKERS) {
      Matcher m = pattern.matcher(msg);
      if (m.find(start) && m.start() < end) {
        end = m.start();
      }
    }
    return msg.substring(start, end);
  }

  private static String redactValueGroups(String msg) {
    String redactedReplacement = Matcher.quoteReplacement(REDACTED_VALUE);
    msg = DETAIL_FAILING_ROW.matcher(msg).replaceAll("$1" + redactedReplacement + "$2");
    msg = DETAIL_KEY.matcher(msg).replaceAll("$1" + redactedReplacement + "$2");
    msg = DUPLICATE_KEY_VALUE_IS.matcher(msg).replaceAll("$1" + redactedReplacement + "$2");
    msg = EQUALS_PAREN.matcher(msg).replaceAll("$1" + redactedReplacement + "$2");
    return msg;
  }

  private static String redactSingleQuoted(String msg) {
    String replacement = Matcher.quoteReplacement("'" + REDACTED_VALUE + "'");
    return SINGLE_QUOTED.matcher(msg).replaceAll(replacement);
  }

  public static String maybeRedact(boolean shouldRedactSensitiveLogs, String msg) {
    if (shouldRedactSensitiveLogs) {
      return REDACTED_VALUE;
    }
    return String.valueOf(msg);
  }
}
