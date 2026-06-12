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

  // Right-edge markers used to find the end of the safe ": ERROR: <reason>" segment that follows
  // a stripped "VALUES (...)" body. The list is ordered for clarity (postgres-style markers first,
  // followed by the batched-error suffix emitted by pgjdbc-derived drivers); LogUtil picks the
  // earliest marker found, so order does not affect correctness.
  //   - "\n  Detail: "             pgjdbc / redshift-jdbc when the server includes a DETAIL field
  //   - "\n  Hint: "               pgjdbc / redshift-jdbc when the server includes a HINT field
  //   - "  Call getNextException " pgjdbc BatchResultHandler suffix; redshift-jdbc reuses the same
  //                                template, so this also bounds Redshift's multi-row INSERT errors
  //                                when DETAIL/HINT are absent (the case the connector hits in
  //                                production)
  // If none of these markers appear after ": ERROR: ", we fall back to returning only the prefix
  // up to ") " — never the row-data segment.
  private static final String[] ERROR_END_MARKERS = {
      "\n  Detail: ",
      "\n  Hint: ",
      "  Call getNextException "
  };

  // This was originally written assuming the Postgres JDBC driver (pgjdbc) message shape, see
  // toString() of ServerErrorMessage.java and the constructor of PSQLException.java with the
  // "boolean detail" flag in
  // https://github.com/pgjdbc/pgjdbc/blob/master/pgjdbc/src/main/java/org/postgresql/util/.
  //
  // Redshift's redshift-jdbc42 driver is a pgjdbc fork and reuses the same BatchUpdateException
  // template ("Batch entry {0} {1} was aborted: {2}  Call getNextException ..." — see
  // amazon-redshift-jdbc-driver BatchResultHandler.java). However for multi-row INSERT failures
  // (i.e., `INSERT INTO ... VALUES (...), (...)` — the form emitted by JDBC sink connectors that
  // batch rows for throughput) the Redshift server omits the DETAIL block, leaving the original
  // single-marker scan unable to find a right-edge for the ": ERROR: <reason>" segment. To
  // restore visibility of the error reason on Redshift (and, secondarily, on Postgres when only
  // HINT is present), this method now scans for any of ERROR_END_MARKERS and uses the earliest
  // match, falling back conservatively to the prefix-only result if none match.
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

    int errorEndIdx = -1;
    for (String marker : ERROR_END_MARKERS) {
      int idx = errMsg.indexOf(marker, errorStartIdx);
      if (idx > 0 && (errorEndIdx < 0 || idx < errorEndIdx)) {
        errorEndIdx = idx;
      }
    }
    if (errorEndIdx < 0) {
      return msg1;
    }

    String msg2 = errMsg.substring(errorStartIdx, errorEndIdx);
    return msg1 + msg2;
  }
}
