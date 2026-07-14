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

import org.junit.Assert;
import org.junit.Test;

import java.sql.BatchUpdateException;
import java.sql.SQLException;

/**
 * Tests for the new, driver-agnostic, fail-closed structured redactor
 * {@link LogUtil#sanitizeSensitiveData(SQLException)} / {@link LogUtil#sanitizeMessage(String)}.
 *
 * <p>Fixtures are real prod-captured JDBC sink exception messages (CC-42731 / INC-12079).
 * Each fixture asserts that none of the "canary" substrings (customer row values) survive
 * sanitization, and that all "skeleton" substrings (debuggability context: table/column
 * names, error class text) do survive.
 */
public class LogUtilSanitizeTest {

  private static void assertNoLeak(String out, String... canaries) {
    for (String canary : canaries) {
      Assert.assertFalse("Canary leaked into sanitized message: [" + canary + "] -> " + out,
          out.contains(canary));
    }
  }

  private static void assertKept(String out, String... skeleton) {
    for (String s : skeleton) {
      Assert.assertTrue("Skeleton text missing from sanitized message: [" + s + "] -> " + out,
          out.contains(s));
    }
  }

  @Test
  public void testPgInsertNotNullBatchIncident() {
    BatchUpdateException e = new BatchUpdateException(
        "Batch entry 0 INSERT INTO \"appdb\".\"clean\".\"ingest.sample-di.address\" "
            + "(\"id\",\"ingested_at\",\"country\",\"city\",\"ref_col\",\"ext_col\") "
            + "VALUES (('samplerec:key:0000abcd'),('CN'),('CITYA, REGIONB'),"
            + "('EXAMPLE TRADING CO.,LTD'),(NULL),('REF-000-00000')) "
            + "ON CONFLICT (\"id\") DO UPDATE SET \"city\"=EXCLUDED.\"city\","
            + "\"ext_col\"=EXCLUDED.\"ext_col\" was aborted: ERROR: null value in column "
            + "\"ref_col\" of relation \"ingest.sample-di.address\" violates not-null "
            + "constraint\n"
            + "  Detail: Failing row contains (1000000000.000, CN, CITYA, CITYA TRADING "
            + "FIRM CO.,LTD, null).  Call getNextException to see other errors in the batch.",
        "23502", 0, new int[0]);

    SQLException sanitized = LogUtil.sanitizeSensitiveData(e);
    String out = sanitized.getMessage();

    assertNoLeak(out, "CITYA TRADING FIRM", "REF-000-00000", "samplerec:key",
        "1000000000.000");
    assertKept(out, "ingest.sample-di.address", "ref_col", "violates not-null constraint");

    // Exception-level preservation contract.
    Assert.assertTrue(sanitized instanceof BatchUpdateException);
    Assert.assertEquals("23502", sanitized.getSQLState());
    Assert.assertEquals(0, sanitized.getErrorCode());
    Assert.assertArrayEquals(new int[0], ((BatchUpdateException) sanitized).getUpdateCounts());
  }

  @Test
  public void testPgDeleteWhereBatch() {
    BatchUpdateException e = new BatchUpdateException(
        "Batch entry 0 DELETE FROM \"appdb2\".\"schemab\""
            + ".\"sample_join_table\" WHERE \"left_id\" = "
            + "('00000000-0000-0000-0000-000000000001') AND \"right_id\" = "
            + "('00000000-0000-0000-0000-000000000002') was aborted: ERROR: operator does not "
            + "exist: uuid = character varying\n"
            + "  Hint: No operator matches the given name and argument types.  Position: 95  "
            + "Call getNextException to see other errors in the batch.",
        new int[0]);

    SQLException sanitized = LogUtil.sanitizeSensitiveData(e);
    String out = sanitized.getMessage();

    assertNoLeak(out, "00000000-0000-0000-0000-000000000001", "00000000-0000-0000-0000-000000000002");
    assertKept(out, "sample_join_table", "operator does not exist");
  }

  @Test
  public void testPgUpdateSetWhereBatch() {
    BatchUpdateException e = new BatchUpdateException(
        "Batch entry 0 UPDATE \"sch\".\"tbl\" SET \"email\" = ('canary@leak.test') "
            + "WHERE \"id\" = ('99') was aborted: ERROR: some error",
        new int[0]);

    SQLException sanitized = LogUtil.sanitizeSensitiveData(e);
    String out = sanitized.getMessage();

    assertNoLeak(out, "canary@leak.test", "'99'");
    assertKept(out, "sch", "tbl");
  }

  @Test
  public void testPgNonBatchUniqueDetailKey() {
    SQLException e = new SQLException(
        "ERROR: duplicate key value violates unique constraint \"pg_type_typname_nsp_index\"\n"
            + "  Detail: Key (typname, typnamespace)=(SAMPLE_TABLE_NAME, 99999) "
            + "already exists.",
        "23505", 0);

    SQLException sanitized = LogUtil.sanitizeSensitiveData(e);
    String out = sanitized.getMessage();

    assertNoLeak(out, "SAMPLE_TABLE_NAME", "99999");
    assertKept(out, "pg_type_typname_nsp_index", "duplicate key value violates unique constraint");
  }

  @Test
  public void testMySqlIncorrectDatetimeBatch() {
    BatchUpdateException e = new BatchUpdateException(
        "Data truncation: Incorrect datetime value: '2999-01-01 00:00:00' for column "
            + "'timestamp' at row 1",
        new int[0]);

    SQLException sanitized = LogUtil.sanitizeSensitiveData(e);
    String out = sanitized.getMessage();

    assertNoLeak(out, "2999-01-01 00:00:00");
    assertKept(out, "Incorrect datetime value");
  }

  @Test
  public void testMySqlDuplicateEntryBatch() {
    BatchUpdateException e = new BatchUpdateException(
        "Duplicate entry 'canary-uuid-PII' for key 'PRIMARY'", new int[0]);

    SQLException sanitized = LogUtil.sanitizeSensitiveData(e);
    String out = sanitized.getMessage();

    assertNoLeak(out, "canary-uuid-PII");
    assertKept(out, "Duplicate entry");
  }

  @Test
  public void testSqlServerDuplicateKeyBatch() {
    BatchUpdateException e = new BatchUpdateException(
        "Violation of PRIMARY KEY constraint 'PK_x'. Cannot insert duplicate key in object "
            + "'dbo.T'. The duplicate key value is (canary-PII-value, 42).",
        new int[0]);

    SQLException sanitized = LogUtil.sanitizeSensitiveData(e);
    String out = sanitized.getMessage();

    assertNoLeak(out, "canary-PII-value");
    assertKept(out, "Violation of PRIMARY KEY", "The duplicate key value is");
    // Note: single-quoted identifiers like 'dbo.T' are intentionally redacted too (fail-closed
    // over-redaction trade) - intentionally not asserted as kept here.
  }

  @Test
  public void testOracleOra00001BenignUnchanged() {
    SQLException e = new SQLException(
        "ORA-00001: unique constraint (SAMPLE_SCHEMA.SAMPLE_CONSTRAINT_PK) violated",
        "23000", 1);

    SQLException sanitized = LogUtil.sanitizeSensitiveData(e);
    String out = sanitized.getMessage();

    assertKept(out, "ORA-00001", "SAMPLE_SCHEMA.SAMPLE_CONSTRAINT_PK", "unique constraint");
    Assert.assertEquals(
        "ORA-00001: unique constraint (SAMPLE_SCHEMA.SAMPLE_CONSTRAINT_PK) violated", out);
  }

  @Test
  public void testPgSequenceOverflowBenignBatch() {
    BatchUpdateException e = new BatchUpdateException(
        "Batch entry 0 INSERT INTO appdb.dbo.sample_table "
            + "(col_a,col_b,col_c): ERROR: nextval: reached maximum value of "
            + "sequence \"sample_table_pk_id_seq\" (2147483647)",
        new int[0]);

    SQLException sanitized = LogUtil.sanitizeSensitiveData(e);
    String out = sanitized.getMessage();

    assertKept(out, "sample_table", "reached maximum value of sequence", "2147483647");
  }

  @Test
  public void testChainIsRecursedOnTwoLevelNestedFixture() {
    BatchUpdateException e2 = new BatchUpdateException(
        "Batch entry 0 UPDATE \"sch\".\"tbl\" SET \"email\" = ('canary@leak.test') "
            + "WHERE \"id\" = ('99') was aborted: ERROR: some error",
        new int[0]);
    SQLException e1 = new SQLException(
        "Duplicate entry 'canary-uuid-PII' for key 'PRIMARY'", "23000", 1);
    e1.setNextException(e2);

    SQLException sanitized = LogUtil.sanitizeSensitiveData(e1);

    assertNoLeak(sanitized.getMessage(), "canary-uuid-PII");
    assertKept(sanitized.getMessage(), "Duplicate entry");

    SQLException next = sanitized.getNextException();
    Assert.assertNotNull(next);
    Assert.assertTrue(next instanceof BatchUpdateException);
    assertNoLeak(next.getMessage(), "canary@leak.test", "'99'");
    assertKept(next.getMessage(), "sch", "tbl");
  }

  @Test
  public void testPgBatchAbortedNoErrorMarkerFailsClosedToHeadOnly() {
    BatchUpdateException e = new BatchUpdateException(
        "Batch entry 0 UPDATE \"t\" SET \"x\" = ('secret-canary') "
            + "was aborted due to a driver-internal error",
        new int[0]);

    SQLException sanitized = LogUtil.sanitizeSensitiveData(e);
    String out = sanitized.getMessage();

    assertNoLeak(out, "secret-canary");
    assertKept(out, "UPDATE");
  }

  @Test
  public void testStrayApostropheBeforeSingleQuotedValueDoesNotShiftPairing() {
    // A prose apostrophe (e.g. "couldn't") occurring before a genuinely single-quoted value
    // must not pair with the value's opening quote and shift redaction off by one. Fail-closed:
    // the value must still be redacted regardless of the stray apostrophe.
    SQLException e = new SQLException(
        "ERROR: parser couldn't handle value 'canary-secret-PII' at position 3", "42601", 0);

    SQLException sanitized = LogUtil.sanitizeSensitiveData(e);
    String out = sanitized.getMessage();

    assertNoLeak(out, "canary-secret-PII");
  }

  @Test
  public void testNonSqlCausePreservedUnchanged() {
    // A non-SQLException cause is run through the same sanitize path, which returns it as-is.
    Throwable cause = new RuntimeException("some-non-sql-cause");
    SQLException e = new SQLException("Duplicate entry 'canary-uuid-PII' for key 'PRIMARY'");
    e.initCause(cause);

    SQLException sanitized = LogUtil.sanitizeSensitiveData(e);
    assertNoLeak(sanitized.getMessage(), "canary-uuid-PII");
    Assert.assertSame(cause, sanitized.getCause());
  }

  @Test
  public void testCauseIsPreservedAndSanitized() {
    // B1: getCause() must be carried onto the rebuilt exception, and the cause itself sanitized so
    // it cannot reintroduce a raw value.
    SQLException cause = new SQLException(
        "Batch entry 0 UPDATE \"sch\".\"tbl\" SET \"email\" = ('canary@leak.test') "
            + "WHERE \"id\" = ('99') was aborted: ERROR: some error", "23000", 7);
    SQLException e = new SQLException(
        "Duplicate entry 'canary-uuid-PII' for key 'PRIMARY'", "23001", 1);
    e.initCause(cause);

    SQLException sanitized = LogUtil.sanitizeSensitiveData(e);

    Throwable sanitizedCause = sanitized.getCause();
    Assert.assertNotNull("Cause was dropped", sanitizedCause);
    Assert.assertTrue(sanitizedCause instanceof SQLException);
    assertNoLeak(sanitizedCause.getMessage(), "canary@leak.test", "'99'");
    assertKept(sanitizedCause.getMessage(), "sch", "tbl");
    Assert.assertEquals("23000", ((SQLException) sanitizedCause).getSQLState());
    Assert.assertEquals(7, ((SQLException) sanitizedCause).getErrorCode());
    assertNoLeak(sanitized.getMessage(), "canary-uuid-PII");
  }

  @Test
  public void testDeepNextExceptionChainDoesNotStackOverflow() {
    // B1: batch failures chain one SQLException per failing row, so depth is input-influenced.
    // A deep chain must be rebuilt iteratively without a StackOverflowError.
    final int depth = 20000;
    SQLException head = new SQLException(
        "Duplicate entry 'canary-uuid-PII' for key 'PRIMARY'", "23000", 1);
    SQLException tail = head;
    for (int i = 1; i < depth; i++) {
      SQLException next = new SQLException(
          "Duplicate entry 'canary-uuid-PII' for key 'PRIMARY'", "23000", 1);
      tail.setNextException(next);
      tail = next;
    }

    SQLException sanitized = LogUtil.sanitizeSensitiveData(head);

    int count = 0;
    SQLException cur = sanitized;
    while (cur != null) {
      assertNoLeak(cur.getMessage(), "canary-uuid-PII");
      assertKept(cur.getMessage(), "Duplicate entry");
      count++;
      cur = cur.getNextException();
    }
    Assert.assertEquals(depth, count);
  }

  @Test
  public void testBatchUpdateNonEmptyUpdateCountsPreserved() {
    // B4: current fixtures all use new int[0]; assert a NON-empty getUpdateCounts() survives.
    // Statement.EXECUTE_FAILED (-3) is a legitimate per-row count that must survive round-trip.
    int[] counts = new int[] {1, java.sql.Statement.EXECUTE_FAILED, 0, 3};
    BatchUpdateException e = new BatchUpdateException(
        "Batch entry 0 INSERT INTO \"sch\".\"tbl\" (\"c\") VALUES ('canary@leak.test') "
            + "was aborted: ERROR: some error",
        "23000", 42, counts);

    SQLException sanitized = LogUtil.sanitizeSensitiveData(e);

    assertNoLeak(sanitized.getMessage(), "canary@leak.test");
    Assert.assertTrue(sanitized instanceof BatchUpdateException);
    Assert.assertArrayEquals(counts, ((BatchUpdateException) sanitized).getUpdateCounts());
    Assert.assertEquals("23000", sanitized.getSQLState());
    Assert.assertEquals(42, sanitized.getErrorCode());
  }

  @Test
  public void testSanitizeMessageIsIdempotentOnBatchFixture() {
    // B3: a re-logged/rolled exception must not be double-mangled.
    String raw =
        "Batch entry 0 INSERT INTO \"appdb\".\"clean\".\"ingest.sample-di.address\" "
            + "(\"id\",\"country\") VALUES (('samplerec:key:0000abcd'),('CN')) was aborted: "
            + "ERROR: null value in column \"ref_col\" violates not-null constraint\n"
            + "  Detail: Failing row contains (1000000000.000, CN, null).  "
            + "Call getNextException to see other errors in the batch.";
    String once = LogUtil.sanitizeMessage(raw);
    String twice = LogUtil.sanitizeMessage(once);
    Assert.assertEquals(once, twice);
  }

  @Test
  public void testSanitizeMessageIsIdempotentOnDetailFixture() {
    String raw =
        "ERROR: duplicate key value violates unique constraint \"pg_type_typname_nsp_index\"\n"
            + "  Detail: Key (typname, typnamespace)=(SAMPLE_TABLE_NAME, 99999) "
            + "already exists.";
    String once = LogUtil.sanitizeMessage(raw);
    String twice = LogUtil.sanitizeMessage(once);
    Assert.assertEquals(once, twice);
  }

  // ---------------------------------------------------------------------------------------------
  // A1 + A2 (PR #1663 review fix-ups, phase 2): redaction-shape correctness against the REAL
  // captured driver messages under .superpowers/pr1663-fixup/evidence/. Values (row data) must be
  // redacted; identifiers (key/column/constraint/object names) must be kept; and values that
  // legitimately contain ')' must not leak their tail past a naive first-')' terminator.
  // ---------------------------------------------------------------------------------------------

  @Test
  public void testMySqlDuplicateEntryKeepsIdentifierRedactsValue() {
    // Real MySQL capture: MySQL single-quotes BOTH the value and the identifier, so the redactor
    // must anchor on the preceding keyword ('entry ' => value, 'for key ' => identifier).
    SQLException e = new SQLException(
        "Duplicate entry 'dupval)x' for key 'evid_dup.uk_name'", "23000", 1062);

    String out = LogUtil.sanitizeSensitiveData(e).getMessage();

    assertNoLeak(out, "dupval)x");
    assertKept(out, "Duplicate entry", "for key ", "'evid_dup.uk_name'");
  }

  @Test
  public void testMySqlIncorrectDatetimeKeepsColumnRedactsValue() {
    // Real MySQL capture: 'value: ' => value (redact), 'for column ' => identifier (keep).
    SQLException e = new SQLException(
        "Data truncation: Incorrect datetime value: 'not-a-date)x' for column 'dt' at row 1",
        "22007", 1292);

    String out = LogUtil.sanitizeSensitiveData(e).getMessage();

    assertNoLeak(out, "not-a-date)x");
    assertKept(out, "Incorrect datetime value", "for column ", "'dt'", "at row 1");
  }

  @Test
  public void testSqlServerPkKeepsConstraintAndObjectRedactsValue() {
    // Real SQL Server capture: single quotes mark IDENTIFIERS (constraint, object) which must be
    // KEPT; the redactable value is the parenthesized 'value is (1)' tuple.
    SQLException e = new SQLException(
        "Violation of PRIMARY KEY constraint 'pk_evid'. Cannot insert duplicate key in object "
            + "'dbo.evid_pk'. The duplicate key value is (1).", "23000", 2627);

    String out = LogUtil.sanitizeSensitiveData(e).getMessage();

    assertNoLeak(out, "is (1)");
    assertKept(out, "Violation of PRIMARY KEY", "constraint 'pk_evid'", "in object 'dbo.evid_pk'",
        "The duplicate key value is (");
  }

  @Test
  public void testPgFailingRowParenInValueDoesNotLeakTail() {
    // Real Postgres capture: the row value 'foo)bar(baz)qux' contains ')', so a non-greedy
    // first-')' terminator would leak ')bar(baz)qux'. The true terminator is ').'.
    SQLException e = new SQLException(
        "ERROR: null value in column \"email\" of relation \"evid_orders\" violates not-null "
            + "constraint\n"
            + "  Detail: Failing row contains (10, null, foo)bar(baz)qux).", "23502", 0);

    String out = LogUtil.sanitizeSensitiveData(e).getMessage();

    assertNoLeak(out, "foo)bar(baz)qux", ")bar", "(baz)", "qux");
    assertKept(out, "\"email\"", "\"evid_orders\"", "violates not-null constraint",
        "Detail: Failing row contains (", ").");
    // Nothing after the value survives unredacted: the whole tuple is replaced.
    Assert.assertTrue(out.endsWith("Detail: Failing row contains (<redacted>)."));
  }

  @Test
  public void testPgDetailKeyParenInValueKeepsColumnListRedactsValue() {
    // Real Postgres capture: 'Key (code)' is the key COLUMN LIST (identifier, keep); the value
    // 'dupcode)x' contains ')' and must be redacted whole ('=(...)' up to ' already exists').
    SQLException e = new SQLException(
        "ERROR: duplicate key value violates unique constraint \"evid_orders_code_key\"\n"
            + "  Detail: Key (code)=(dupcode)x) already exists.", "23505", 0);

    String out = LogUtil.sanitizeSensitiveData(e).getMessage();

    assertNoLeak(out, "dupcode)x", "dupcode");
    assertKept(out, "\"evid_orders_code_key\"", "duplicate key value violates unique constraint",
        "Detail: Key (code)=(", ") already exists.");
    // No tail leak: the whole value is replaced, the ')x' tail cannot survive.
    Assert.assertTrue(out.endsWith("Detail: Key (code)=(<redacted>) already exists."));
  }

  @Test
  public void testStackTraceIsPreserved() {
    SQLException e = new SQLException("Duplicate entry 'canary-uuid-PII' for key 'PRIMARY'");
    e.fillInStackTrace();
    StackTraceElement[] original = e.getStackTrace();

    SQLException sanitized = LogUtil.sanitizeSensitiveData(e);
    Assert.assertArrayEquals(original, sanitized.getStackTrace());
  }
}
