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
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.IdentityHashMap;
import java.util.function.UnaryOperator;

/**
 * Rebuilds throwable graphs with sensitive data removed before connector code logs them.
 *
 * <p>Source connector redaction replaces entire messages. Sink sanitization preserves diagnostic
 * structure and delegates message parsing to a grammar for specific pgjdbc, MySQL, and SQL Server
 * shapes. Recognized shapes with incomplete boundaries fall closed.
 */
public class LogUtil {
  private static final String REDACTED_VALUE = SqlErrorMessageSanitizer.REDACTED_VALUE;

  /**
   * @deprecated Use {@link #sanitizeSensitiveData(SQLException)}.
   */
  @Deprecated
  public static SQLException trimSensitiveData(SQLException e) {
    return sanitizeSensitiveData(e);
  }

  /**
   * @deprecated Use {@link #sanitizeSensitiveData(SQLException)} for SQL exceptions.
   */
  @Deprecated
  public static Throwable trimSensitiveData(Throwable t) {
    return t instanceof SQLException ? sanitizeSensitiveData((SQLException) t) : t;
  }

  public static SQLException redactSensitiveData(SQLException e) {
    return (SQLException) redactSensitiveData((Throwable) e);
  }

  public static Throwable redactSensitiveData(Throwable t) {
    // The whole message is replaced with the redaction marker; causes remain intentionally dropped.
    return rebuildChain(t, message -> REDACTED_VALUE, false);
  }

  /**
   * Rebuilds an exception graph with sensitive values removed from every retained message.
   *
   * <p>{@link BatchUpdateException} nodes remain {@code BatchUpdateException} instances and retain
   * their update counts. Every other {@link SQLException} subtype is represented by a plain
   * {@code SQLException}; generic subtype reconstruction is intentionally avoided because driver
   * constructors do not share a safe contract. All SQL nodes retain their SQLState, vendor code,
   * stack trace, next-exception edge, and sanitized cause edge.
   *
   * @param e the SQL exception to sanitize
   * @return a rebuilt exception graph, or {@code null} when {@code e} is {@code null}
   */
  public static SQLException sanitizeSensitiveData(SQLException e) {
    return (SQLException) sanitizeSensitiveData((Throwable) e);
  }

  private static Throwable sanitizeSensitiveData(Throwable t) {
    return rebuildChain(t, SqlErrorMessageSanitizer::sanitize, true);
  }

  /**
   * Rebuilds both next-exception and cause edges without recursion. Identity tracking bounds cyclic
   * graphs and preserves shared edges. Non-SQL roots retain the historical pass-through behavior;
   * non-SQL causes are rebuilt as generic, fully redacted throwables.
   */
  private static Throwable rebuildChain(
      Throwable t, UnaryOperator<String> transform, boolean preserveCause) {
    if (!(t instanceof SQLException)) {
      return t;
    }

    IdentityHashMap<Throwable, Throwable> rebuilt = new IdentityHashMap<>();
    Deque<Throwable> pending = new ArrayDeque<>();
    rebuilt.put(t, rebuildNode(t, transform));
    pending.push(t);

    while (!pending.isEmpty()) {
      Throwable current = pending.pop();
      Throwable rebuiltCurrent = rebuilt.get(current);

      if (current instanceof SQLException) {
        SQLException next = ((SQLException) current).getNextException();
        if (next != null) {
          Throwable rebuiltNext = rebuilt.get(next);
          if (rebuiltNext == null) {
            rebuiltNext = rebuildNode(next, transform);
            rebuilt.put(next, rebuiltNext);
            pending.push(next);
          }
          ((SQLException) rebuiltCurrent).setNextException((SQLException) rebuiltNext);
        }
      }

      if (preserveCause) {
        Throwable cause = current.getCause();
        if (cause != null) {
          Throwable rebuiltCause = rebuilt.get(cause);
          if (rebuiltCause == null) {
            rebuiltCause = rebuildNode(cause, transform);
            rebuilt.put(cause, rebuiltCause);
            pending.push(cause);
          }
          if (rebuiltCause != rebuiltCurrent) {
            rebuiltCurrent.initCause(rebuiltCause);
          }
        }
      }
    }

    return rebuilt.get(t);
  }

  private static Throwable rebuildNode(Throwable t, UnaryOperator<String> transform) {
    if (!(t instanceof SQLException)) {
      Throwable out = new Throwable(REDACTED_VALUE);
      out.setStackTrace(t.getStackTrace());
      return out;
    }

    SQLException e = (SQLException) t;
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

  public static String maybeRedact(boolean shouldRedactSensitiveLogs, String msg) {
    if (shouldRedactSensitiveLogs) {
      return REDACTED_VALUE;
    }
    return String.valueOf(msg);
  }
}
