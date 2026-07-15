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
import java.sql.BatchUpdateException;
import java.sql.SQLException;
import org.junit.Assert; import org.junit.Test;
public class LogUtilSanitizeTest {
  @Test public void skeletonWhenContextless() {
    SafeSqlException out = LogUtil.sanitize(
        new BatchUpdateException("Batch entry 0 INSERT ... 'x@y.com'", "23505", 1062, new int[]{1}));
    Assert.assertEquals(
        "<redacted> [type=BatchUpdateException, category=unique_violation, sqlState=23505]",
        out.getMessage());
    Assert.assertEquals(0, out.getErrorCode());
    Assert.assertEquals(0, out.getStackTrace().length);
  }
  @Test public void reconstructedFromStatement() {
    SafeSqlException out = LogUtil.sanitizeReconstructed(
        new SQLException("ERROR: dup key (email)=(x@y.com)", "23505", 0),
        "INSERT INTO \"db\".\"orders\" (\"id\",\"email\") VALUES (<redacted>)", null);
    Assert.assertTrue(out.getMessage().startsWith(
        "INSERT INTO \"db\".\"orders\" (\"id\",\"email\") VALUES (<redacted>): ERROR: unique_violation"));
    Assert.assertFalse(out.getMessage().contains("x@y.com"));
  }
  @Test public void oracleStandardTagReconstructed() {
    SafeSqlException out = LogUtil.sanitizeReconstructed(
        sqlEx("ORA-00001: unique constraint violated", "23000", 1),
        "INSERT INTO \"S\".\"ORDERS\" (\"ID\") VALUES (<redacted>)", "ORA");
    Assert.assertTrue(out.getMessage().endsWith("[sqlState=23000] [ORA-00001]"));
  }
  @Test public void oracleCustomCollapsedReconstructed() {
    SafeSqlException out = LogUtil.sanitizeReconstructed(
        sqlEx("ORA-20001: app error", null, 20001),
        "INSERT INTO \"S\".\"ORDERS\" (\"ID\") VALUES (<redacted>)", "ORA");
    Assert.assertTrue(out.getMessage().endsWith("ERROR: sql_error [application_error]"));
    Assert.assertFalse(out.getMessage().contains("app error"));
  }
  @Test public void reSealsAlreadySafe() {
    SafeSqlException first = LogUtil.sanitizeReconstructed(
        new SQLException("raw", "23505", 0), "INSERT INTO \"t\" (\"c\") VALUES (<redacted>)", null);
    SafeSqlException resealed = LogUtil.sanitize(first);
    Assert.assertEquals(first.getMessage(), resealed.getMessage());
    Assert.assertNotSame(first, resealed);
  }
  @Test public void failClosedOnRuntime() {
    SQLException weird = new SQLException("x") {
      @Override public String getSQLState() { throw new RuntimeException(); } };
    Assert.assertEquals("<redacted> [type=SQLException, category=sql_error]",
        LogUtil.sanitize(weird).getMessage());
  }
  @Test(expected = OutOfMemoryError.class) public void rethrowsError() {
    SQLException fatal = new SQLException("x") {
      @Override public String getSQLState() { throw new OutOfMemoryError(); } };
    LogUtil.sanitize(fatal);
  }
  private static SQLException sqlEx(String msg, String state, int code) {
    return new SQLException(msg, state, code);
  }
}
