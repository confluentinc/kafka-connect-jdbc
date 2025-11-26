/*
 * Copyright 2024 Confluent Inc.
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

import org.apache.commons.lang3.exception.ExceptionUtils;

import java.sql.SQLException;
import java.sql.SQLNonTransientException;
import java.sql.SQLRecoverableException;
import java.sql.SQLTransientException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Locale;
import java.util.Set;

/**
 * Utility class for determining whether SQL exceptions should be retried.
 */
public final class RetryUtils {

  private static final String CONNECTION_CLOSED_MSG = "CONNECTION IS CLOSED";

  private static final Set<String> RETRIABLE_SQLSTATE_PREFIXES =
      Collections.unmodifiableSet(new HashSet<>(Arrays.asList(
          "08", // connection exception
          "40", // transaction rollback
          "55"  // object not available / lock not available (PG)
      )));

  private static final Set<String> RETRIABLE_SQLSTATES =
      Collections.unmodifiableSet(new HashSet<>(Arrays.asList(
          "40001", // serialization failure
          "40P01", // deadlock detected (PostgreSQL)
          "55P03", // lock not available (PostgreSQL)
          "57014", // query canceled (PostgreSQL, DB2)
          "57033"  // connection ended (DB2)
      )));

  private static final Set<String> RETRIABLE_MESSAGE_TOKENS =
      Collections.unmodifiableSet(new HashSet<>(Arrays.asList(
          "ORA-01466", // Oracle definition changed
          "ORA-01284", // Oracle file cannot be opened
          "ORA-03113", // end-of-file communication channel
          "ORA-03114", // not connected to ORACLE
          "ORA-12541", // TNS no listener
          "ORA-12545", // TNS host or object not known
          "DEADLOCK DETECTED",
          "LOCK REQUEST TIMEOUT", // SQL Server
          "SQLCODE=-911" // DB2 deadlock/timeout
      )));

  private static final Set<Integer> RETRIABLE_ERROR_CODES =
      Collections.synchronizedSet(new HashSet<>(Arrays.asList(
          1466, 1284, 3113, 3114, 4068, // Oracle
          1205, 1213, // MySQL and SQL Server deadlock/timeouts
          -911 // DB2 deadlock/timeout when reported via error code
      )));

  public static void addRetriableErrorCodes(Collection<Integer> errorCodes) {
    if (errorCodes == null || errorCodes.isEmpty()) {
      return;
    }
    RETRIABLE_ERROR_CODES.addAll(errorCodes);
  }

  public static boolean shouldRetry(Throwable throwable) {
    if (throwable == null) {
      return false;
    }
    for (Throwable t : ExceptionUtils.getThrowableList(throwable)) {
      if (t instanceof SQLException) {
        for (SQLException se = (SQLException) t; se != null; se = se.getNextException()) {
          if (canRetry(se)) {
            return true;
          }
          if (cannotRetry(se)) {
            return false;
          }
        }
      }
    }
    return false;
  }

  private static boolean canRetry(SQLException se) {
    return isRecoverableAndNotClosed(se)
        || (se instanceof SQLTransientException)
        || hasRetriableErrorCodeOrState(se)
        || messageHasRetriableTokens(se);
  }

  private static boolean hasRetriableErrorCodeOrState(SQLException se) {
    return RETRIABLE_ERROR_CODES.contains(se.getErrorCode()) || hasRetriableSqlState(se);
  }

  private static boolean isRecoverableAndNotClosed(SQLException se) {
    return se instanceof SQLRecoverableException
        && (se.getMessage() == null
            || !se.getMessage().toUpperCase(Locale.ROOT).contains(CONNECTION_CLOSED_MSG));
  }

  private static boolean cannotRetry(SQLException se) {
    return se instanceof SQLNonTransientException;
  }

  private static boolean hasRetriableSqlState(SQLException se) {
    String sqlState = se.getSQLState();
    return sqlState != null
        && !sqlState.isEmpty()
        && (hasExactSqlStateMatch(sqlState) || hasSqlStatePrefixMatch(sqlState));
  }

  private static boolean hasExactSqlStateMatch(String sqlState) {
    return RETRIABLE_SQLSTATES.contains(sqlState.toUpperCase(Locale.ROOT));
  }

  private static boolean hasSqlStatePrefixMatch(String sqlState) {
    return sqlState.length() >= 2
        && RETRIABLE_SQLSTATE_PREFIXES.contains(
            sqlState.substring(0, 2).toUpperCase(Locale.ROOT));
  }

  private static boolean messageHasRetriableTokens(SQLException se) {
    String message = se.getMessage();
    return message != null
        && !message.isEmpty()
        && RETRIABLE_MESSAGE_TOKENS.stream()
            .anyMatch(token -> message.toUpperCase(Locale.ROOT).contains(token));
  }
}

