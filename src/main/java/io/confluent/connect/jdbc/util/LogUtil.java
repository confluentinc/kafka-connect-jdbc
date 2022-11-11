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

  // This implementation assumes it to be Postgres, see toString() of ServerErrorMessage.java
  // as well as the constructor of PSQLException.java with "boolean detail" flag in
  // https://github.com/pgjdbc/pgjdbc/blob/master/pgjdbc/src/main/java/org/postgresql/util/
  // For other JDBC Databases it would not fail but might return the same input string back!
  private static String getNonSensitiveErrorMessage(String errMsg) {
    final String sensitiveStartSearchText = ") VALUES (";
    final String errorStartSearchText = ": ERROR: ";
    final String errorEndSearchText = "\n  Detail: ";

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
    int errorEndIdx = errMsg.indexOf(errorEndSearchText);
    if (errorStartIdx < trimEndIdx || errorEndIdx < trimEndIdx
            || errorEndIdx < errorStartIdx) {
      return msg1;
    }

    String msg2 = errMsg.substring(errorStartIdx, errorEndIdx);
    return msg1 + msg2;
  }
}
