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
import java.sql.SQLException;
import org.junit.Assert; import org.junit.Test;
public class SafeSqlExceptionTest {
  private SafeSqlDiagnostic diag() {
    return SafeSqlDiagnostic.reconstructed(
        "INSERT INTO \"t\" (\"c\") VALUES (<redacted>)",
        DiagnosticCategory.UNIQUE_VIOLATION, "23505", null);
  }
  @Test public void cleanAtConstruction() {
    SafeSqlException e = new SafeSqlException(diag());
    Assert.assertEquals(
        "INSERT INTO \"t\" (\"c\") VALUES (<redacted>): ERROR: unique_violation [sqlState=23505]",
        e.getMessage());
    Assert.assertEquals("23505", e.getSQLState());
    Assert.assertEquals(0, e.getErrorCode());
    Assert.assertEquals(0, e.getStackTrace().length);
    Assert.assertNull(e.getCause());
    Assert.assertNull(e.getNextException());
  }
  @Test public void mutatorsAreNoOps() {
    SafeSqlException e = new SafeSqlException(diag());
    e.initCause(new RuntimeException("raw-cause"));
    e.setNextException(new SQLException("raw-next"));
    e.setStackTrace(new StackTraceElement[]{ new StackTraceElement("X","m","X.java",1) });
    Assert.assertNull(e.getCause());
    Assert.assertNull(e.getNextException());
    Assert.assertEquals(0, e.getStackTrace().length);
    Assert.assertFalse(e.toString().contains("raw"));
  }
  @Test public void reSealFromDiagnosticIsFreshAndEqualMessage() {
    SafeSqlException first = new SafeSqlException(diag());
    SafeSqlException resealed = new SafeSqlException(first.diagnostic());
    Assert.assertEquals(first.getMessage(), resealed.getMessage());
    Assert.assertNotSame(first, resealed);
  }
}
