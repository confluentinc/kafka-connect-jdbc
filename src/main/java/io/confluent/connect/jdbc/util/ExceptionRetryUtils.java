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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLException;
import java.sql.SQLNonTransientException;
import java.sql.SQLRecoverableException;
import java.sql.SQLTransientException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

/**
 * Utility class for determining whether SQL exceptions should be retried.
 */
public final class ExceptionRetryUtils {

  private static final String CONNECTION_CLOSED_MSG = "CONNECTION IS CLOSED";
  private static final Logger log = LoggerFactory.getLogger(ExceptionRetryUtils.class);

  private static final Set<String> RETRIABLE_SQLSTATE_PREFIXES =
      Collections.unmodifiableSet(new HashSet<>(Arrays.asList(
          "08", // connection exception
          "40", // transaction rollback
          "55",  // object not available / lock not available (PG)
          "57" // process/query issues (PostgreSQL, DB2)
      )));

  // *
  private static final Set<Integer> RETRIABLE_ERROR_CODES =
      Collections.synchronizedSet(new HashSet<>(Arrays.asList(
          1466, 1284, 3113, 3114, 4068, // Oracle
          1205, 1213, // MySQL and SQL Server deadlock/timeouts
          -911 // DB2 deadlock/timeout when reported via error code
      )));

  /**
   * Ordered decision:
   * - Traverse throwable cause chain in order; for each SQLException, traverse
   *   its next-exception chain in order.
   * - The first qualifying SQL decides:
   *   - retriable (recoverable/transient/whitelisted) => retry (true)
   *   - non-transient => do not retry (false)
   * - If the first qualifying SQL seen is non-transient => do not retry (false).
   *   - Non-transient exceptions take precedence over retriable ones.
   * - If no qualifying SQL is found => do not retry (false).
   */
  public static boolean shouldRetry(Throwable throwable) {
    if (throwable == null) {
      return false;
    }
    for (Throwable t : ExceptionUtils.getThrowableList(throwable)) {
      if (t instanceof SQLException) {
        for (SQLException se = (SQLException) t; se != null; se = se.getNextException()) {
          log.info("Evaluating SQLException for retry: SQLState={}, ErrorCode={}, Message={}",
              se.getSQLState(), se.getErrorCode(), se.getMessage());
          log.info("Full SQLException: ", se);
          log.info("Next SQLException in chain: ", se.getNextException());
          if (canRetry(se)) {
            return true;
          }
          if (cannotRetry(se)) {
            return false;
          }
        }
      }
    }
    return true;
  }

  private static boolean canRetry(SQLException se) {
    return isRecoverableAndNotClosed(se)
        || (se instanceof SQLTransientException)
        || hasRetriableErrorCodeOrState(se);
  }

  private static boolean hasRetriableErrorCodeOrState(SQLException se) {
    return RETRIABLE_ERROR_CODES.contains(se.getErrorCode()) || hasRetriableSqlState(se);
  }

  private static boolean isRecoverableAndNotClosed(SQLException se) {
    return se instanceof SQLRecoverableException
        && (se.getMessage() == null
            || !se.getMessage().contains(CONNECTION_CLOSED_MSG));
  }

  private static boolean cannotRetry(SQLException se) {
    return se instanceof SQLNonTransientException;
  }

  private static boolean hasRetriableSqlState(SQLException se) {
    String sqlState = se.getSQLState();
    return sqlState != null
        && !sqlState.isEmpty()
        && (hasSqlStatePrefixMatch(sqlState));
  }

  private static boolean hasSqlStatePrefixMatch(String sqlState) {
    return sqlState.length() >= 2
        && RETRIABLE_SQLSTATE_PREFIXES.contains(
            sqlState.substring(0, 2));
  }
}
