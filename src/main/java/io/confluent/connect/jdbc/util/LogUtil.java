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
import java.util.function.UnaryOperator;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Utility that redacts sensitive customer data (row values, literals) out of JDBC driver exception
 * messages before they are logged, while preserving the debuggability skeleton (table/column
 * names, error class text, SQLState, vendor code, batch update counts and the exception chain).
 *
 * <p>It is the durable redactor gating the JDBC sink write-failure log path. The connector config
 * {@code trim.sensitive.log} (field {@code JdbcSinkConfig.trimSensitiveLogsEnabled}) selects the
 * behaviour: when {@code true} (the default) failing exceptions are routed through
 * {@link #sanitizeSensitiveData(SQLException)} so values are replaced with {@code <redacted>};
 * when {@code false} (the operator escape hatch) the raw driver message is logged verbatim.
 */
public class LogUtil {
  private static final String REDACTED_VALUE = "<redacted>";

  public static SQLException redactSensitiveData(SQLException e) {
    return (SQLException) redactSensitiveData((Throwable) e);
  }

  public static Throwable redactSensitiveData(Throwable t) {
    // The whole message is replaced with the redaction marker; the cause is intentionally dropped
    // (preserveCause=false) to keep this path's fully-redacting behaviour unchanged.
    return rebuildChain(t, message -> REDACTED_VALUE, false);
  }

  public static SQLException sanitizeSensitiveData(SQLException e) {
    return (SQLException) sanitizeSensitiveData((Throwable) e);
  }

  private static Throwable sanitizeSensitiveData(Throwable t) {
    // Structured redaction of each message in the chain; the cause is preserved (run through the
    // same sanitize path) so it cannot reintroduce a raw value.
    return rebuildChain(t, LogUtil::sanitizeMessage, true);
  }

  /**
   * Shared skeleton for {@link #redactSensitiveData} and {@link #sanitizeSensitiveData}: walks the
   * {@link SQLException#getNextException()} chain ITERATIVELY (batch failures chain one exception
   * per failing row, so depth is input-influenced and a recursive walk risks StackOverflowError),
   * rebuilding each node with {@code transform} applied to its message and preserving SQLState,
   * vendor code, {@link BatchUpdateException} update counts, the next-exception chain and the
   * stack trace. Non-SQLExceptions (including {@code null}) are returned as-is. When
   * {@code preserveCause} is true the cause of each node is carried over via
   * {@link Throwable#initCause(Throwable)} after being run back through this same rebuild (with
   * the same transform), so a cause cannot leak a
   * raw value either.
   */
  private static Throwable rebuildChain(
      Throwable t, UnaryOperator<String> transform, boolean preserveCause) {
    if (!(t instanceof SQLException)) {
      // Also the termination condition when a next-exception is null.
      return t;
    }
    SQLException head = null;
    SQLException prev = null;
    SQLException current = (SQLException) t;
    while (current != null) {
      SQLException rebuilt = rebuildNode(current, transform);
      if (preserveCause) {
        Throwable cause = current.getCause();
        if (cause != null) {
          rebuilt.initCause(rebuildChain(cause, transform, true));
        }
      }
      if (head == null) {
        head = rebuilt;
      } else {
        prev.setNextException(rebuilt);
      }
      prev = rebuilt;
      current = current.getNextException();
    }
    return head;
  }

  // Rebuilds a single SQLException node, applying `transform` to its message and preserving the
  // SQLState, vendor error code, (for BatchUpdateException) update counts, and stack trace.
  private static SQLException rebuildNode(SQLException e, UnaryOperator<String> transform) {
    SQLException out;
    if (e instanceof BatchUpdateException) {
      BatchUpdateException b = (BatchUpdateException) e;
      out = new BatchUpdateException(
          transform.apply(b.getMessage()), b.getSQLState(), b.getErrorCode(),
          b.getUpdateCounts());
    } else {
      out = new SQLException(
          transform.apply(e.getMessage()), e.getSQLState(), e.getErrorCode());
    }
    out.setStackTrace(e.getStackTrace());
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

  // Delimiters that may legitimately precede an opening value-quote: whitespace, or one of the
  // punctuation characters that typically introduce a SQL literal (open-paren, equals, comma,
  // colon). A quote preceded by anything else (e.g. a letter, as in a prose contraction like
  // "couldn't") is treated as a stray apostrophe, not the start of a quoted value, so it can't
  // shift the pairing and hide a genuine value-quote that follows it.
  private static final String OPENING_QUOTE_DELIMITER_PUNCTUATION = "(=,:";

  private static boolean isOpeningQuoteContext(String msg, int idx) {
    if (idx == 0) {
      return true;
    }
    char prev = msg.charAt(idx - 1);
    if (Character.isWhitespace(prev)) {
      return true;
    }
    return OPENING_QUOTE_DELIMITER_PUNCTUATION.indexOf(prev) >= 0;
  }

  // Scans forward from `start` (just past an opening quote) for the matching closing quote,
  // treating a doubled '' as an escaped literal quote (not the close), mirroring the semantics
  // of the old '(?:[^']|'')*' pattern. Returns -1 if no closing quote is found.
  private static int findClosingQuote(String msg, int start) {
    int n = msg.length();
    int j = start;
    while (j < n) {
      char c = msg.charAt(j);
      if (c == '\'') {
        if (j + 1 < n && msg.charAt(j + 1) == '\'') {
          j += 2;
          continue;
        }
        return j;
      }
      j++;
    }
    return -1;
  }

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

  // Single-quoted literal redaction; driver-agnostic. Identifiers use "" / `` / [] so are
  // untouched by this scan. Handles doubled '' escapes inside the literal, and only pairs a
  // quote as an "opening" quote when it sits in a position where a real SQL literal would start
  // (see isOpeningQuoteContext) so a stray prose apostrophe (e.g. "couldn't") can't shift the
  // pairing and leave a genuine value unredacted. Fail-closed: an opening quote with no matching
  // close redacts through end-of-line/string rather than leaving the tail unredacted.
  private static String redactSingleQuoted(String msg) {
    StringBuilder sb = new StringBuilder(msg.length());
    int n = msg.length();
    int i = 0;
    while (i < n) {
      char c = msg.charAt(i);
      if (c == '\'' && isOpeningQuoteContext(msg, i)) {
        int closeIdx = findClosingQuote(msg, i + 1);
        if (closeIdx >= 0) {
          sb.append('\'').append(REDACTED_VALUE).append('\'');
          i = closeIdx + 1;
          continue;
        }
        // Fail-closed: unmatched opening quote, redact through end-of-line/string.
        int eol = msg.indexOf('\n', i);
        int end = eol < 0 ? n : eol;
        sb.append('\'').append(REDACTED_VALUE);
        i = end;
        continue;
      }
      sb.append(c);
      i++;
    }
    return sb.toString();
  }

  public static String maybeRedact(boolean shouldRedactSensitiveLogs, String msg) {
    if (shouldRedactSensitiveLogs) {
      return REDACTED_VALUE;
    }
    return String.valueOf(msg);
  }
}
