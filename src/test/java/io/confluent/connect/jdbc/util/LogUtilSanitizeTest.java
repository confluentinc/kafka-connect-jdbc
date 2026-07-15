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
import java.util.Arrays;
import java.util.List;

/**
 * Covers pgjdbc, MySQL, and SQL Server message shapes separately from exception-graph rebuilding.
 * Unknown or incomplete shapes are expected to fall closed.
 */
public class LogUtilSanitizeTest {

  private static final List<MessageShape> DRIVER_MESSAGE_SHAPES = Arrays.asList(
      shape(
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
          removed(
              "CITYA TRADING FIRM",
              "REF-000-00000",
              "samplerec:key",
              "1000000000.000"
          ),
          retained(
              "ingest.sample-di.address",
              "ref_col",
              "violates not-null constraint"
          )
      ),
      shape(
          "Batch entry 0 DELETE FROM \"appdb2\".\"schemab\".\"sample_join_table\" "
              + "WHERE \"left_id\" = ('00000000-0000-0000-0000-000000000001') "
              + "AND \"right_id\" = ('00000000-0000-0000-0000-000000000002') "
              + "was aborted: ERROR: operator does not exist: uuid = character varying\n"
              + "  Hint: No operator matches the given name and argument types.  Position: 95  "
              + "Call getNextException to see other errors in the batch.",
          removed(
              "00000000-0000-0000-0000-000000000001",
              "00000000-0000-0000-0000-000000000002"
          ),
          retained("sample_join_table", "operator does not exist")
      ),
      shape(
          "Batch entry 0 UPDATE \"sch\".\"tbl\" SET \"email\" = ('canary@leak.test') "
              + "WHERE \"id\" = ('99') was aborted: ERROR: some error",
          removed("canary@leak.test", "'99'"),
          retained("sch", "tbl")
      ),
      shape(
          "ERROR: duplicate key value violates unique constraint "
              + "\"pg_type_typname_nsp_index\"\n"
              + "  Detail: Key (typname, typnamespace)=(SAMPLE_TABLE_NAME, 99999) "
              + "already exists.",
          removed("SAMPLE_TABLE_NAME", "99999"),
          retained(
              "pg_type_typname_nsp_index",
              "duplicate key value violates unique constraint"
          )
      ),
      shape(
          "Data truncation: Incorrect datetime value: '2999-01-01 00:00:00' "
              + "for column 'timestamp' at row 1",
          removed("2999-01-01 00:00:00"),
          retained("Incorrect datetime value", "for column 'timestamp'", "at row 1")
      ),
      shape(
          "Duplicate entry 'canary-uuid-PII' for key 'PRIMARY'",
          removed("canary-uuid-PII"),
          retained("Duplicate entry", "for key 'PRIMARY'")
      ),
      shape(
          "Violation of PRIMARY KEY constraint 'PK_x'. Cannot insert duplicate key in object "
              + "'dbo.T'. The duplicate key value is (canary-PII-value, 42).",
          removed("canary-PII-value", "42"),
          retained(
              "Violation of PRIMARY KEY",
              "constraint 'PK_x'",
              "object 'dbo.T'",
              "The duplicate key value is ("
          )
      ),
      exactShape(
          "ORA-00001: unique constraint (SAMPLE_SCHEMA.SAMPLE_CONSTRAINT_PK) violated",
          "ORA-00001: unique constraint (SAMPLE_SCHEMA.SAMPLE_CONSTRAINT_PK) violated",
          removed(),
          retained("ORA-00001", "SAMPLE_SCHEMA.SAMPLE_CONSTRAINT_PK", "unique constraint")
      ),
      shape(
          "Batch entry 0 INSERT INTO appdb.dbo.sample_table "
              + "(col_a,col_b,col_c): ERROR: nextval: reached maximum value of "
              + "sequence \"sample_table_pk_id_seq\" (2147483647)",
          removed(),
          retained("sample_table", "reached maximum value of sequence", "2147483647")
      ),
      exactShape(
          "Batch entry 0 UPDATE \"t\" SET \"x\" = ('secret-canary') "
              + "was aborted due to a driver-internal error",
          "Batch entry 0 UPDATE \"t\"",
          removed("secret-canary"),
          retained("UPDATE")
      ),
      shape(
          "ERROR: parser couldn't handle value 'canary-secret-PII' at position 3",
          removed("canary-secret-PII"),
          retained("parser couldn't handle value")
      ),
      shape(
          "Duplicate entry 'dupval)x' for key 'evid_dup.uk_name'",
          removed("dupval)x"),
          retained("Duplicate entry", "for key ", "'evid_dup.uk_name'")
      ),
      shape(
          "Data truncation: Incorrect datetime value: 'not-a-date)x' for column 'dt' at row 1",
          removed("not-a-date)x"),
          retained("Incorrect datetime value", "for column ", "'dt'", "at row 1")
      ),
      shape(
          "Violation of PRIMARY KEY constraint 'pk_evid'. Cannot insert duplicate key in object "
              + "'dbo.evid_pk'. The duplicate key value is (1).",
          removed("is (1)"),
          retained(
              "Violation of PRIMARY KEY",
              "constraint 'pk_evid'",
              "in object 'dbo.evid_pk'",
              "The duplicate key value is ("
          )
      ),
      exactShape(
          "ERROR: null value in column \"email\" of relation \"evid_orders\" violates "
              + "not-null constraint\n"
              + "  Detail: Failing row contains (10, null, foo)bar(baz)qux).",
          "ERROR: null value in column \"email\" of relation \"evid_orders\" violates "
              + "not-null constraint\n"
              + "  Detail: Failing row contains (<redacted>).",
          removed("foo)bar(baz)qux", ")bar", "(baz)", "qux"),
          retained(
              "\"email\"",
              "\"evid_orders\"",
              "violates not-null constraint",
              "Detail: Failing row contains (",
              ")."
          )
      ),
      exactShape(
          "ERROR: null value violates not-null constraint\n"
              + "  Detail: Failing row contains (10, note (a). CANARY-TAIL, 20).",
          "ERROR: null value violates not-null constraint\n"
              + "  Detail: Failing row contains (<redacted>).",
          removed("CANARY-TAIL"),
          retained("Detail: Failing row contains (", ").")
      ),
      exactShape(
          "ERROR: duplicate key value violates unique constraint \"evid_orders_code_key\"\n"
              + "  Detail: Key (code)=(dupcode)x) already exists.",
          "ERROR: duplicate key value violates unique constraint \"evid_orders_code_key\"\n"
              + "  Detail: Key (code)=(<redacted>) already exists.",
          removed("dupcode)x", "dupcode"),
          retained(
              "\"evid_orders_code_key\"",
              "duplicate key value violates unique constraint",
              "Detail: Key (code)=(",
              ") already exists."
          )
      ),
      exactShape(
          "ERROR: duplicate key value violates unique constraint \"orders_code_key\"\n"
              + "  Detail: Key (code)=(part) already exists CANARY-TAIL) already exists.",
          "ERROR: duplicate key value violates unique constraint \"orders_code_key\"\n"
              + "  Detail: Key (code)=(<redacted>) already exists.",
          removed("CANARY-TAIL"),
          retained("Detail: Key (code)=(", ") already exists.")
      ),
      exactShape(
          "Batch entry 3 UPDATE \"orders\" SET \"status\" = ('secret-1') "
              + "was aborted: ERROR: trigger rejected CANARY-VALUE",
          "Batch entry 3 UPDATE \"orders\"",
          removed("secret-1", "CANARY-VALUE"),
          retained("Batch entry 3 UPDATE \"orders\"")
      ),
      shape(
          "ERROR: null value violates not-null constraint\n"
              + "  Detail: Failing row contains (1,\nCANARY-VALUE,\n3).",
          removed("CANARY-VALUE"),
          retained("Detail: Failing row contains (", ").")
      ),
      exactShape(
          "ERROR: null value violates not-null constraint\n"
              + "  Detail: Failing row contains (CANARY-VALUE",
          "ERROR: null value violates not-null constraint\n"
              + "  Detail: Failing row contains (<redacted>",
          removed("CANARY-VALUE"),
          retained("Detail: Failing row contains (")
      ),
      exactShape(
          "ERROR: constraint failure\n"
              + "  Detail: Key (code)=(CANARY-VALUE) driver-specific CANARY-TAIL",
          "ERROR: constraint failure\n"
              + "  Detail: Key (code)=(<redacted>",
          removed("CANARY-VALUE", "CANARY-TAIL"),
          retained("Detail: Key (code)=(")
      ),
      shape(
          "Violation of UNIQUE KEY constraint 'uk_orders'. The duplicate key value is "
              + "(1,\nCANARY-VALUE,\n3).",
          removed("CANARY-VALUE"),
          retained("constraint 'uk_orders'", "The duplicate key value is (", ").")
      ),
      exactShape(
          "Violation of UNIQUE KEY constraint 'uk_orders'. The duplicate key value is "
              + "(note (a). CANARY-TAIL, 3).",
          "Violation of UNIQUE KEY constraint 'uk_orders'. The duplicate key value is "
              + "(<redacted>).",
          removed("CANARY-TAIL"),
          retained("constraint 'uk_orders'", "The duplicate key value is (")
      ),
      shape(
          "ERROR: insert or update on table \"orders\" violates foreign key constraint "
              + "\"orders_customer_id_fkey\"\n"
              + "  Detail: Key (customer_id)=(CANARY-VALUE) is not present in table "
              + "\"customers\".",
          removed("CANARY-VALUE"),
          retained("Key (customer_id)=(", "is not present in table \"customers\"")
      ),
      shape(
          "ERROR: update or delete on table \"customers\" violates foreign key constraint "
              + "\"orders_customer_id_fkey\" on table \"orders\"\n"
              + "  Detail: Key (id)=(CANARY-VALUE) is still referenced from table "
              + "\"orders\".",
          removed("CANARY-VALUE"),
          retained("Key (id)=(", "is still referenced from table \"orders\"")
      ),
      exactShape(
          "Batch entry 7 CALL write_row(CANARY-VALUE) "
              + "was aborted: ERROR: procedure rejected secret-1",
          "Batch entry 7 CALL",
          removed("write_row", "CANARY-VALUE", "secret-1"),
          retained("Batch entry 7 CALL")
      ),
      shape(
          "Duplicate entry 'O'Brien-CANARY-VALUE' for key 'uk_orders_code'",
          removed("O'Brien-CANARY-VALUE", "Brien-CANARY-VALUE"),
          retained("Duplicate entry", "for key 'uk_orders_code'")
      ),
      shape(
          "Duplicate entry 'part-1' for key 'CANARY-TAIL' for key 'uk_orders_code'",
          removed("CANARY-TAIL"),
          retained("Duplicate entry", "for key 'uk_orders_code'")
      ),
      shape(
          "Data truncation: Incorrect string value: 'O'Brien-CANARY-VALUE' "
              + "for column 'display_name' at row 1",
          removed("O'Brien-CANARY-VALUE", "Brien-CANARY-VALUE"),
          retained("Incorrect string value", "for column 'display_name'")
      ),
      exactShape(
          "ERROR: invalid value 'secret-1\nsecret-2",
          "ERROR: invalid value '<redacted>",
          removed("secret-1", "secret-2"),
          retained("ERROR: invalid value '")
      ),
      shape(
          "ERROR: invalid input value N'CANARY-NATIONAL'",
          removed("CANARY-NATIONAL"),
          retained("N'<redacted>'")
      ),
      shape(
          "ERROR: invalid input value E'CANARY-ESCAPE'",
          removed("CANARY-ESCAPE"),
          retained("E'<redacted>'")
      ),
      shape(
          "ERROR: invalid input value E'O\\'Brien-CANARY-TAIL'",
          removed("Brien-CANARY-TAIL", "CANARY-TAIL"),
          retained("E'<redacted>'")
      ),
      shape(
          "ERROR: invalid input value B'CANARY-BIT'",
          removed("CANARY-BIT"),
          retained("B'<redacted>'")
      ),
      shape(
          "ERROR: invalid input value x'CANARY-HEX'",
          removed("CANARY-HEX"),
          retained("x'<redacted>'")
      ),
      shape(
          "ERROR: invalid input value _utf8mb4'CANARY-UTF8'",
          removed("CANARY-UTF8"),
          retained("_utf8mb4'<redacted>'")
      ),
      shape(
          "ERROR: invalid input syntax for type integer: \"CANARY-VALUE\" "
              + "in column \"age\" of relation \"orders\"",
          removed("CANARY-VALUE"),
          retained("invalid input syntax for type integer", "\"age\"", "\"orders\"")
      ),
      shape(
          "ERROR: invalid input syntax for type integer: "
              + "\"CANARY-HEAD\"CANARY-TAIL\" in column \"age\"",
          removed("CANARY-HEAD", "CANARY-TAIL"),
          retained("invalid input syntax for type integer", "\"age\"")
      )
  );

  @Test
  public void sanitizesDriverMessageShapes() {
    for (MessageShape shape : DRIVER_MESSAGE_SHAPES) {
      String sanitized = assertSanitizedShape(
          shape.rawMessage,
          shape.removedSubstrings,
          shape.retainedSubstrings
      );
      if (shape.expectedMessage != null) {
        Assert.assertEquals(shape.expectedMessage, sanitized);
      }
    }
  }

  @Test
  public void sanitizationIsIdempotent() {
    for (MessageShape shape : DRIVER_MESSAGE_SHAPES) {
      String sanitized = SqlErrorMessageSanitizer.sanitize(shape.rawMessage);
      Assert.assertEquals(sanitized, SqlErrorMessageSanitizer.sanitize(sanitized));
    }
  }

  @Test
  public void nullMessageRemainsNull() {
    Assert.assertNull(SqlErrorMessageSanitizer.sanitize(null));
  }

  @Test
  public void rebuildsNextExceptionAndCauseEdges() {
    BatchUpdateException next = new BatchUpdateException(
        "Batch entry 0 UPDATE \"sch\".\"tbl\" SET \"email\" = ('canary@leak.test') "
            + "WHERE \"id\" = ('99') was aborted: ERROR: some error",
        new int[0]
    );
    SQLException cause = new SQLException(
        "Duplicate entry 'cause-canary' for key 'PRIMARY'",
        "23001",
        2
    );
    SQLException root = new SQLException(
        "Duplicate entry 'root-canary' for key 'PRIMARY'",
        "23000",
        1
    );
    root.setNextException(next);
    root.initCause(cause);

    SQLException sanitized = LogUtil.sanitizeSensitiveData(root);

    assertNoLeak(sanitized.getMessage(), "root-canary");
    assertNoLeak(sanitized.getNextException().getMessage(), "canary@leak.test", "'99'");
    assertNoLeak(sanitized.getCause().getMessage(), "cause-canary");
    Assert.assertTrue(sanitized.getNextException() instanceof BatchUpdateException);
    Assert.assertEquals("23001", ((SQLException) sanitized.getCause()).getSQLState());
    Assert.assertEquals(2, ((SQLException) sanitized.getCause()).getErrorCode());
  }

  @Test
  public void nonSqlCauseCannotLeakItsRawMessage() {
    Throwable cause = new RuntimeException("runtime failure CANARY-VALUE");
    SQLException exception =
        new SQLException("Duplicate entry 'canary-uuid-PII' for key 'PRIMARY'");
    exception.initCause(cause);

    SQLException sanitized = LogUtil.sanitizeSensitiveData(exception);

    Assert.assertNotSame(cause, sanitized.getCause());
    assertNoLeak(sanitized.getMessage(), "canary-uuid-PII");
    assertNoLeak(sanitized.getCause().getMessage(), "CANARY-VALUE");
  }

  @Test
  public void deepNextExceptionChainDoesNotOverflowTheStack() {
    final int depth = 20000;
    SQLException head =
        new SQLException("Duplicate entry 'canary-uuid-PII' for key 'PRIMARY'", "23000", 1);
    SQLException tail = head;
    for (int i = 1; i < depth; i++) {
      SQLException next =
          new SQLException("Duplicate entry 'canary-uuid-PII' for key 'PRIMARY'", "23000", 1);
      tail.setNextException(next);
      tail = next;
    }

    SQLException sanitized = LogUtil.sanitizeSensitiveData(head);

    int count = 0;
    SQLException current = sanitized;
    while (current != null) {
      assertNoLeak(current.getMessage(), "canary-uuid-PII");
      count++;
      current = current.getNextException();
    }
    Assert.assertEquals(depth, count);
  }

  @Test
  public void deepCauseChainDoesNotOverflowTheStack() {
    final int depth = 20000;
    SQLException head = new SQLException("invalid value 'secret-0'", "22000", 1);
    SQLException tail = head;
    for (int i = 1; i < depth; i++) {
      SQLException cause =
          new SQLException("invalid value 'secret-" + i + "'", "22000", 1);
      tail.initCause(cause);
      tail = cause;
    }

    SQLException sanitized = LogUtil.sanitizeSensitiveData(head);

    int count = 0;
    Throwable current = sanitized;
    while (current != null) {
      assertNoLeak(current.getMessage(), "secret-");
      count++;
      current = current.getCause();
    }
    Assert.assertEquals(depth, count);
  }

  @Test
  public void cyclicCauseGraphPreservesIdentity() {
    SQLException first = new SQLException("invalid value 'secret-1'", "22000", 1);
    SQLException second = new SQLException("invalid value 'secret-2'", "22000", 2);
    first.initCause(second);
    second.initCause(first);

    SQLException sanitized = LogUtil.sanitizeSensitiveData(first);

    Throwable sanitizedSecond = sanitized.getCause();
    Assert.assertNotNull(sanitizedSecond);
    Assert.assertSame(sanitized, sanitizedSecond.getCause());
    assertNoLeak(sanitized.getMessage(), "secret-1");
    assertNoLeak(sanitizedSecond.getMessage(), "secret-2");
  }

  @Test
  public void preservesSupportedExceptionMetadata() {
    StackTraceElement[] stackTrace = {
        new StackTraceElement("Driver", "write", "Driver.java", 42)
    };
    DriverSQLException driverException =
        new DriverSQLException("invalid value 'CANARY-VALUE'", "22000", 17);
    driverException.setStackTrace(stackTrace);
    int[] updateCounts = {1, java.sql.Statement.EXECUTE_FAILED, 0, 3};
    BatchUpdateException batchException = new BatchUpdateException(
        "invalid value 'secret-1'",
        "23000",
        18,
        updateCounts
    );
    driverException.setNextException(batchException);

    SQLException sanitized = LogUtil.sanitizeSensitiveData(driverException);

    Assert.assertEquals(SQLException.class, sanitized.getClass());
    Assert.assertEquals("22000", sanitized.getSQLState());
    Assert.assertEquals(17, sanitized.getErrorCode());
    Assert.assertArrayEquals(stackTrace, sanitized.getStackTrace());
    assertNoLeak(sanitized.getMessage(), "CANARY-VALUE");

    SQLException sanitizedBatch = sanitized.getNextException();
    Assert.assertEquals(BatchUpdateException.class, sanitizedBatch.getClass());
    Assert.assertEquals("23000", sanitizedBatch.getSQLState());
    Assert.assertEquals(18, sanitizedBatch.getErrorCode());
    Assert.assertArrayEquals(
        updateCounts,
        ((BatchUpdateException) sanitizedBatch).getUpdateCounts()
    );
  }

  @Test
  @SuppressWarnings("deprecation")
  public void deprecatedTrimSensitiveDataOverloadsDelegateToSanitizer() throws Exception {
    SQLException exception = new SQLException(
        "Duplicate entry 'CANARY-VALUE' for key 'uk_orders_code'",
        "23000",
        1062
    );
    String expected = LogUtil.sanitizeSensitiveData(exception).getMessage();

    SQLException fromSqlOverload = LogUtil.trimSensitiveData(exception);
    Throwable fromThrowableOverload = LogUtil.trimSensitiveData((Throwable) exception);

    Assert.assertEquals(expected, fromSqlOverload.getMessage());
    Assert.assertEquals(expected, fromThrowableOverload.getMessage());
    Assert.assertTrue(
        LogUtil.class.getMethod("trimSensitiveData", SQLException.class)
            .isAnnotationPresent(Deprecated.class)
    );
    Assert.assertTrue(
        LogUtil.class.getMethod("trimSensitiveData", Throwable.class)
            .isAnnotationPresent(Deprecated.class)
    );
  }

  private static String assertSanitizedShape(
      String rawMessage,
      String[] removedSubstrings,
      String[] retainedSubstrings
  ) {
    String sanitized = SqlErrorMessageSanitizer.sanitize(rawMessage);
    assertNoLeak(sanitized, removedSubstrings);
    assertKept(sanitized, retainedSubstrings);
    return sanitized;
  }

  private static void assertNoLeak(String sanitized, String... canaries) {
    for (String canary : canaries) {
      Assert.assertFalse(
          "Canary leaked into sanitized message: [" + canary + "] -> " + sanitized,
          sanitized.contains(canary)
      );
    }
  }

  private static void assertKept(String sanitized, String... skeleton) {
    for (String text : skeleton) {
      Assert.assertTrue(
          "Expected text missing from sanitized message: [" + text + "] -> " + sanitized,
          sanitized.contains(text)
      );
    }
  }

  private static MessageShape shape(
      String rawMessage,
      String[] removedSubstrings,
      String[] retainedSubstrings
  ) {
    return new MessageShape(rawMessage, null, removedSubstrings, retainedSubstrings);
  }

  private static MessageShape exactShape(
      String rawMessage,
      String expectedMessage,
      String[] removedSubstrings,
      String[] retainedSubstrings
  ) {
    return new MessageShape(
        rawMessage,
        expectedMessage,
        removedSubstrings,
        retainedSubstrings
    );
  }

  private static String[] removed(String... substrings) {
    return substrings;
  }

  private static String[] retained(String... substrings) {
    return substrings;
  }

  private static final class MessageShape {
    private final String rawMessage;
    private final String expectedMessage;
    private final String[] removedSubstrings;
    private final String[] retainedSubstrings;

    private MessageShape(
        String rawMessage,
        String expectedMessage,
        String[] removedSubstrings,
        String[] retainedSubstrings
    ) {
      this.rawMessage = rawMessage;
      this.expectedMessage = expectedMessage;
      this.removedSubstrings = removedSubstrings;
      this.retainedSubstrings = retainedSubstrings;
    }
  }

  private static class DriverSQLException extends SQLException {
    private static final long serialVersionUID = 1L;

    DriverSQLException(String reason, String sqlState, int vendorCode) {
      super(reason, sqlState, vendorCode);
    }
  }
}
