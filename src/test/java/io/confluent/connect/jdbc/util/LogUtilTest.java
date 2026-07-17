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

import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;
import org.postgresql.util.PSQLException;

import java.sql.BatchUpdateException;
import java.sql.SQLException;

import static org.junit.Assert.assertEquals;

public class LogUtilTest {
  private static final String REDACTED = "<redacted>";

  @Test
  public void testNonSqlThrowable() {
    Throwable t = new Throwable("t");
    assertEquals(t, LogUtil.trimSensitiveData(t));
  }

  @Test
  public void testSqlExceptionNoNested() {
    SQLException e = new SQLException("e");
    SQLException trimmed = LogUtil.trimSensitiveData(e);
    assertEqualsSQLException(e, trimmed);
  }

  @Test
  public void testSqlExceptionOneLevelNestedNonBatchUpdate() {
    SQLException e1 = new SQLException("e1");
    SQLException e2 = new SQLException("e2");
    e1.setNextException(e2);

    SQLException trimmed = LogUtil.trimSensitiveData(e1);
    assertEqualsSQLException(e1, trimmed);
  }

  @Test
  public void testSqlExceptionTwoLevelNestedNonBatchUpdate() {
    SQLException e1 = new SQLException("e1");
    SQLException e2 = new SQLException("e2");
    SQLException e3 = new SQLException("e3");
    e1.setNextException(e2);
    e2.setNextException(e3);

    SQLException trimmed = LogUtil.trimSensitiveData(e1);
    assertEqualsSQLException(e1, trimmed);
  }

  @Test
  public void testFirstLevelBatchUpdateNoSensitive() {
    BatchUpdateException e1 = new BatchUpdateException("Hello World", new int[0]);
    SQLException trimmed = LogUtil.trimSensitiveData(e1);
    assertEqualsSQLException(e1, trimmed);
  }

  @Test
  public void testFirstLevelBatchUpdateSensitive() {
    BatchUpdateException e1 = new BatchUpdateException("Batch entry 0 INSERT INTO \"abc\" (\"c1\",\"c2\",\"c3\",\"c4\") " +
            "VALUES ('1','2','3',NULL) was aborted: ERROR: null value in column \"c4\" violates not-null constraint\n" +
            "  Detail: Failing row contains (1, 2, 3, null).  Call getNextException to see other errors in the batch.",
            new int[0]);

    BatchUpdateException expectedTrimmed = new BatchUpdateException("Batch entry 0 INSERT INTO \"abc\" (\"c1\",\"c2\",\"c3\",\"c4\"): " +
            "ERROR: null value in column \"c4\" violates not-null constraint",
            new int[0]);

    SQLException actualTrimmed = LogUtil.trimSensitiveData(e1);
    assertEqualsSQLException(expectedTrimmed, actualTrimmed);
  }

  @Test
  public void testSecondLevelNestedBatchUpdateNoSensitive() {
    SQLException e1 = new SQLException("e1");
    BatchUpdateException e2 = new BatchUpdateException("Hello World", new int[0]);
    e1.setNextException(e2);

    SQLException trimmed = LogUtil.trimSensitiveData(e1);
    assertEqualsSQLException(e1, trimmed);
  }

  @Test
  public void testSecondLevelNestedBatchUpdateSensitive() {
    SQLException e1 = new SQLException("e1");
    BatchUpdateException e2 = new BatchUpdateException("Batch entry 0 INSERT INTO \"abc\" (\"c1\",\"c2\",\"c3\",\"c4\") " +
            "VALUES ('1','2','3',NULL) was aborted: ERROR: null value in column \"c4\" violates not-null constraint\n" +
            "  Detail: Failing row contains (1, 2, 3, null).  Call getNextException to see other errors in the batch.",
            new int[0]);
    e1.setNextException(e2);

    SQLException expectedTrimmed = new SQLException("e1");
    BatchUpdateException e3 = new BatchUpdateException("Batch entry 0 INSERT INTO \"abc\" (\"c1\",\"c2\",\"c3\",\"c4\"): " +
            "ERROR: null value in column \"c4\" violates not-null constraint",
            new int[0]);
    expectedTrimmed.setNextException(e3);

    SQLException actualTrimmed = LogUtil.trimSensitiveData(e1);
    assertEqualsSQLException(expectedTrimmed, actualTrimmed);
  }

  @Test
  public void testSecondLevelNestedBatchUpdateSensitiveNoError() {
    SQLException e1 = new SQLException("e1");
    BatchUpdateException e2 = new BatchUpdateException("Batch entry 0 INSERT INTO \"abc\" (\"c1\",\"c2\",\"c3\",\"c4\") " +
            "VALUES ('1','2','3',NULL) was aborted.",
            new int[0]);
    e1.setNextException(e2);

    SQLException expectedTrimmed = new SQLException("e1");
    BatchUpdateException e3 = new BatchUpdateException("Batch entry 0 INSERT INTO \"abc\" (\"c1\",\"c2\",\"c3\",\"c4\")",
            new int[0]);
    expectedTrimmed.setNextException(e3);

    SQLException actualTrimmed = LogUtil.trimSensitiveData(e1);
    assertEqualsSQLException(expectedTrimmed, actualTrimmed);
  }

  @Test
  public void testSensitiveLogWithTrimEnabled() {
    String sensitiveMessage = "SELECT * FROM users WHERE password='secret123'";
    String result = LogUtil.maybeRedact(true, sensitiveMessage);
    assertEquals(REDACTED, result);
  }

  @Test
  public void testSensitiveLogWithTrimDisabled() {
    String message = "SELECT * FROM users WHERE id=1";
    String result = LogUtil.maybeRedact(false, message);
    assertEquals(message, result);
  }

  @Test
  public void testRedactSensitiveDataWithNonSqlThrowable() {
    Throwable t = new RuntimeException("secret");
    Assert.assertSame(t, LogUtil.redactSensitiveData(t));
  }

  @Test
  public void testRedactSensitiveDataWithSqlExceptionChain() {
    SQLException e1 = new SQLException("sensitive-message-e1", "42000", 10);
    SQLException e2 = new SQLException("sensitive-message-e2", "42001", 20);
    e1.setNextException(e2);

    SQLException expected = new SQLException(REDACTED, "42000", 10);
    SQLException expectedChild = new SQLException(REDACTED, "42001", 20);
    expected.setNextException(expectedChild);

    SQLException redacted = LogUtil.redactSensitiveData(e1);

    assertEqualsSQLException(expected, redacted);
  }

  @Test
  public void testRedactSensitiveDataWithBatchUpdateException() {
    BatchUpdateException e1 =
        new BatchUpdateException("sensitive message-e1", "42002", 30, new int[0]);

    SQLException e2 = new SQLException("sensitive message-e2", "42003", 40);
    e1.setNextException(e2);

    BatchUpdateException expected =
        new BatchUpdateException(REDACTED, "42002", 30, new int[0]);
    SQLException expectedChild = new SQLException(REDACTED, "42003", 40);
    expected.setNextException(expectedChild);

    SQLException actual = LogUtil.redactSensitiveData(e1);
    Assert.assertTrue(actual instanceof BatchUpdateException);
    Assert.assertArrayEquals(
        expected.getUpdateCounts(), ((BatchUpdateException) actual).getUpdateCounts());

    assertEqualsSQLException(expected, actual);
  }

  @Test
  public void testSecondLevelNestedBatchUpdateSensitiveNoDetails() {
    SQLException e1 = new SQLException("e1");
    BatchUpdateException e2 = new BatchUpdateException("Batch entry 0 INSERT INTO \"abc\" (\"c1\",\"c2\",\"c3\",\"c4\") " +
            "VALUES ('1','2','3',NULL) was aborted: ERROR: null value in column \"c4\" violates not-null constraint.",
            new int[0]);
    e1.setNextException(e2);

    SQLException expectedTrimmed = new SQLException("e1");
    BatchUpdateException e3 = new BatchUpdateException("Batch entry 0 INSERT INTO \"abc\" (\"c1\",\"c2\",\"c3\",\"c4\")",
            new int[0]);
    expectedTrimmed.setNextException(e3);

    SQLException actualTrimmed = LogUtil.trimSensitiveData(e1);
    assertEqualsSQLException(expectedTrimmed, actualTrimmed);
  }

  @Test
  public void testBatchExceptionWithChild() {
    SQLException e1 = new SQLException("e1");
    BatchUpdateException e2 = new BatchUpdateException("Batch entry 0 INSERT INTO \"abc\" (\"c1\",\"c2\",\"c3\",\"c4\") " +
        "VALUES ('1','2','3',NULL) was aborted: ERROR: null value in column \"c4\" violates not-null constraint.",
        new int[0]);
    SQLException p1 = new SQLException("ERROR: null value in column \"c4\" violates "
        + "not-null constraint\n Detail: Failing row contains ('1','2','3',NULL).");

    e2.setNextException(p1);
    e1.setNextException(e2);

    SQLException expectedTrimmed = new SQLException("e1");
    BatchUpdateException e3 = new BatchUpdateException("Batch entry 0 INSERT INTO \"abc\" (\"c1\",\"c2\",\"c3\",\"c4\")",
        new int[0]);
    expectedTrimmed.setNextException(e3);

    SQLException actualTrimmed = LogUtil.trimSensitiveData(e1);
    assertEqualsSQLException(expectedTrimmed, actualTrimmed);
  }

  // Redshift's redshift-jdbc42 driver emits this message shape for multi-row
  // `INSERT INTO ... VALUES (...), (...)` failures (the form JDBC sink connectors batch into for
  // throughput). The server omits the DETAIL block for many error classes (e.g., string truncation),
  // so the trailing "  Call getNextException ..." text is the only stable right-edge marker.
  @Test
  public void testRedshiftMultiRowBatchUpdateSensitive() {
    BatchUpdateException e1 = new BatchUpdateException(
        "Batch entry 0 /* -partner Confluent Redshift Connector */ INSERT INTO "
            + "\"db\".\"public\".\"t\" (\"id\",\"name\") VALUES (('1'::int4),('ok')),"
            + "(('2'::int4),('secret-payload-customer-PII')) was aborted: "
            + "ERROR: value too long for type character varying(5)"
            + "  Call getNextException to see other errors in the batch.",
        new int[0]);

    BatchUpdateException expectedTrimmed = new BatchUpdateException(
        "Batch entry 0 /* -partner Confluent Redshift Connector */ INSERT INTO "
            + "\"db\".\"public\".\"t\" (\"id\",\"name\"): "
            + "ERROR: value too long for type character varying(5)",
        new int[0]);

    SQLException actualTrimmed = LogUtil.trimSensitiveData(e1);
    assertNoSensitiveLeak(actualTrimmed, "secret-payload-customer-PII");
    assertEqualsSQLException(expectedTrimmed, actualTrimmed);
  }

  // Postgres can return HINT without DETAIL for some error classes (e.g., undefined function with
  // a suggested replacement). The HINT marker should bound the safe error segment.
  @Test
  public void testBatchUpdateSensitiveHintOnly() {
    BatchUpdateException e1 = new BatchUpdateException(
        "Batch entry 0 INSERT INTO \"t\" (\"c\") VALUES ('secret') was aborted: "
            + "ERROR: function lower(integer) does not exist\n"
            + "  Hint: No function matches the given name and argument types.",
        new int[0]);

    BatchUpdateException expectedTrimmed = new BatchUpdateException(
        "Batch entry 0 INSERT INTO \"t\" (\"c\"): "
            + "ERROR: function lower(integer) does not exist",
        new int[0]);

    SQLException actualTrimmed = LogUtil.trimSensitiveData(e1);
    assertNoSensitiveLeak(actualTrimmed, "secret");
    assertEqualsSQLException(expectedTrimmed, actualTrimmed);
  }

  // When more than one structured marker is present, the earliest one (closest to ": ERROR: ")
  // wins. Here DETAIL precedes HINT so the segment stops at DETAIL; the HINT block is dropped
  // along with anything that might follow (preserves the existing conservative behavior).
  @Test
  public void testBatchUpdateSensitiveDetailBeforeHint() {
    BatchUpdateException e1 = new BatchUpdateException(
        "Batch entry 0 INSERT INTO \"t\" (\"c\") VALUES ('secret') was aborted: "
            + "ERROR: null value in column \"c\" violates not-null constraint\n"
            + "  Detail: Failing row contains ('secret').\n"
            + "  Hint: ignore",
        new int[0]);

    BatchUpdateException expectedTrimmed = new BatchUpdateException(
        "Batch entry 0 INSERT INTO \"t\" (\"c\"): "
            + "ERROR: null value in column \"c\" violates not-null constraint",
        new int[0]);

    SQLException actualTrimmed = LogUtil.trimSensitiveData(e1);
    assertNoSensitiveLeak(actualTrimmed, "secret");
    assertEqualsSQLException(expectedTrimmed, actualTrimmed);
  }

  // pgjdbc shape with both a structured DETAIL marker and the BatchResultHandler suffix present.
  // Locks the Tier 1 (structured) vs Tier 2 (suffix) precedence: DETAIL must be chosen, never the
  // suffix, regardless of which appears earlier in the string.
  @Test
  public void testBatchUpdateSensitiveDetailBeatsGetNextException() {
    BatchUpdateException e1 = new BatchUpdateException(
        "Batch entry 0 INSERT INTO \"t\" (\"c\") VALUES ('x'),('toolong') was aborted: "
            + "ERROR: value too long for type character varying(5)\n"
            + "  Detail: Failing row contains (toolong).  Call getNextException to see "
            + "other errors in the batch.",
        new int[0]);

    BatchUpdateException expectedTrimmed = new BatchUpdateException(
        "Batch entry 0 INSERT INTO \"t\" (\"c\"): "
            + "ERROR: value too long for type character varying(5)",
        new int[0]);

    SQLException actualTrimmed = LogUtil.trimSensitiveData(e1);
    assertNoSensitiveLeak(actualTrimmed, "toolong");
    assertEqualsSQLException(expectedTrimmed, actualTrimmed);
  }

  // When no marker matches at all, fall back to the prefix-only result (conservative).
  @Test
  public void testBatchUpdateSensitiveNoKnownMarker() {
    BatchUpdateException e1 = new BatchUpdateException(
        "Batch entry 0 INSERT INTO \"t\" (\"c\") VALUES ('secret') was aborted: "
            + "ERROR: some new format we have not seen before",
        new int[0]);

    BatchUpdateException expectedTrimmed = new BatchUpdateException(
        "Batch entry 0 INSERT INTO \"t\" (\"c\")",
        new int[0]);

    SQLException actualTrimmed = LogUtil.trimSensitiveData(e1);
    assertNoSensitiveLeak(actualTrimmed, "secret");
    assertEqualsSQLException(expectedTrimmed, actualTrimmed);
  }

  // Fails with a readable message if row data leaked through. Run before the strict equality
  // check so a regression in the trim logic produces "Row data leaked: ..." instead of an opaque
  // string diff.
  private static void assertNoSensitiveLeak(SQLException trimmed, String sensitiveSubstring) {
    String msg = trimmed.getMessage();
    Assert.assertFalse("Row data leaked: " + msg, msg.contains(sensitiveSubstring));
    Assert.assertFalse("VALUES clause leaked: " + msg, msg.contains("VALUES"));
  }

  private static void assertEqualsSQLException(SQLException expected, SQLException actual) {
    if (expected == actual) {
      return;
    }

    if (expected == null || actual == null) {
      Assert.assertSame(expected, actual);
    }

    Assert.assertEquals(expected.getClass(), actual.getClass());

    String msg1 = (expected.getLocalizedMessage() == null ? "" : expected.getLocalizedMessage());
    String msg2 = (actual.getLocalizedMessage() == null ? "" : actual.getLocalizedMessage());
    Assert.assertEquals(msg1, msg2);

    assertEqualsSQLException(expected.getNextException(), actual.getNextException());
  }
}
