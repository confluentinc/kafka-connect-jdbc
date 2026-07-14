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
        "Batch entry 0 INSERT INTO \"nextapi_main\".\"clean\".\"clean.lognet-di.address\" "
            + "(\"id\",\"meta_ingested_at\",\"country\",\"city\",\"shipment_id\",\"order_id\") "
            + "VALUES (('lognetdi:address:6d996d96'),('CN'),('NANTONG, JIANGSU'),"
            + "('NANTONG YAOHAN GARMENT CO.,LTD'),(NULL),('OGH-OVH-47673')) "
            + "ON CONFLICT (\"id\") DO UPDATE SET \"city\"=EXCLUDED.\"city\","
            + "\"order_id\"=EXCLUDED.\"order_id\" was aborted: ERROR: null value in column "
            + "\"shipment_id\" of relation \"clean.lognet-di.address\" violates not-null "
            + "constraint\n"
            + "  Detail: Failing row contains (1783660800.785, CN, NANTONG, NANTONG YAOHAN "
            + "GARMENT CO.,LTD, null).  Call getNextException to see other errors in the batch.",
        "23502", 0, new int[0]);

    SQLException sanitized = LogUtil.sanitizeSensitiveData(e);
    String out = sanitized.getMessage();

    assertNoLeak(out, "NANTONG YAOHAN GARMENT", "OGH-OVH-47673", "lognetdi:address",
        "1783660800.785");
    assertKept(out, "clean.lognet-di.address", "shipment_id", "violates not-null constraint");

    // Exception-level preservation contract.
    Assert.assertTrue(sanitized instanceof BatchUpdateException);
    Assert.assertEquals("23502", sanitized.getSQLState());
    Assert.assertEquals(0, sanitized.getErrorCode());
    Assert.assertArrayEquals(new int[0], ((BatchUpdateException) sanitized).getUpdateCounts());
  }

  @Test
  public void testPgDeleteWhereBatch() {
    BatchUpdateException e = new BatchUpdateException(
        "Batch entry 0 DELETE FROM \"platformv2\".\"vendors\""
            + ".\"kafka_platform_users_scorecard_tags\" WHERE \"scorecard_id\" = "
            + "('d9b8b443-c6b8-508a-af79-08e2fde4e0ce') AND \"tag_id\" = "
            + "('f86d9818-83dc-5e8c-b661-aee77607d7e3') was aborted: ERROR: operator does not "
            + "exist: uuid = character varying\n"
            + "  Hint: No operator matches the given name and argument types.  Position: 95  "
            + "Call getNextException to see other errors in the batch.",
        new int[0]);

    SQLException sanitized = LogUtil.sanitizeSensitiveData(e);
    String out = sanitized.getMessage();

    assertNoLeak(out, "d9b8b443-c6b8-508a-af79-08e2fde4e0ce", "f86d9818-83dc-5e8c-b661-aee77607d7e3");
    assertKept(out, "kafka_platform_users_scorecard_tags", "operator does not exist");
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
            + "  Detail: Key (typname, typnamespace)=(TBAADM_PAYMENT_ORDER_HEADER_TABLE, 17320) "
            + "already exists.",
        "23505", 0);

    SQLException sanitized = LogUtil.sanitizeSensitiveData(e);
    String out = sanitized.getMessage();

    assertNoLeak(out, "TBAADM_PAYMENT_ORDER_HEADER_TABLE", "17320");
    assertKept(out, "pg_type_typname_nsp_index", "duplicate key value violates unique constraint");
  }

  @Test
  public void testMySqlIncorrectDatetimeBatch() {
    BatchUpdateException e = new BatchUpdateException(
        "Data truncation: Incorrect datetime value: '2569-06-20 15:30:07' for column "
            + "'timestamp' at row 1",
        new int[0]);

    SQLException sanitized = LogUtil.sanitizeSensitiveData(e);
    String out = sanitized.getMessage();

    assertNoLeak(out, "2569-06-20 15:30:07");
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
        "ORA-00001: unique constraint (OCEDL.DM_EDL_PRAT_COLLECT_UPDREC_PK) violated",
        "23000", 1);

    SQLException sanitized = LogUtil.sanitizeSensitiveData(e);
    String out = sanitized.getMessage();

    assertKept(out, "ORA-00001", "OCEDL.DM_EDL_PRAT_COLLECT_UPDREC_PK", "unique constraint");
    Assert.assertEquals(
        "ORA-00001: unique constraint (OCEDL.DM_EDL_PRAT_COLLECT_UPDREC_PK) violated", out);
  }

  @Test
  public void testPgSequenceOverflowBenignBatch() {
    BatchUpdateException e = new BatchUpdateException(
        "Batch entry 0 INSERT INTO uds.dbo.inferred_job_title "
            + "(gd_user_id,pred_jt_text,city_long): ERROR: nextval: reached maximum value of "
            + "sequence \"inferred_job_title_pk_id_seq\" (2147483647)",
        new int[0]);

    SQLException sanitized = LogUtil.sanitizeSensitiveData(e);
    String out = sanitized.getMessage();

    assertKept(out, "inferred_job_title", "reached maximum value of sequence", "2147483647");
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
  public void testNonSqlThrowableReturnedUnchanged() {
    Throwable t = new RuntimeException("secret-value");
    Assert.assertSame(t, LogUtil.sanitizeSensitiveData(t));
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
