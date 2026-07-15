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
import org.junit.Assert; import org.junit.Test;
public class SafeSqlDiagnosticTest {
  @Test public void reconstructInsert() {
    SafeSqlDiagnostic d = SafeSqlDiagnostic.reconstructed(
        "INSERT INTO \"db\".\"orders\" (\"id\",\"email\") VALUES (<redacted>)",
        DiagnosticCategory.UNIQUE_VIOLATION, "23505", null);
    Assert.assertEquals(
        "INSERT INTO \"db\".\"orders\" (\"id\",\"email\") VALUES (<redacted>): "
        + "ERROR: unique_violation [sqlState=23505]", d.message);
    Assert.assertEquals("23505", d.canonicalSqlState);
  }
  @Test public void reconstructOracleStandard() {
    SafeSqlDiagnostic d = SafeSqlDiagnostic.reconstructed(
        "INSERT INTO \"S\".\"ORDERS\" (\"ID\",\"EMAIL\") VALUES (<redacted>)",
        DiagnosticCategory.INTEGRITY_CONSTRAINT_VIOLATION, "23000", "ORA-00001");
    Assert.assertEquals(
        "INSERT INTO \"S\".\"ORDERS\" (\"ID\",\"EMAIL\") VALUES (<redacted>): "
        + "ERROR: integrity_constraint_violation [sqlState=23000] [ORA-00001]", d.message);
  }
  @Test public void reconstructOracleCustomCollapsed() {
    SafeSqlDiagnostic d = SafeSqlDiagnostic.reconstructed(
        "INSERT INTO \"S\".\"ORDERS\" (\"ID\",\"STATUS\") VALUES (<redacted>)",
        DiagnosticCategory.SQL_ERROR, null, "application_error");
    Assert.assertEquals(
        "INSERT INTO \"S\".\"ORDERS\" (\"ID\",\"STATUS\") VALUES (<redacted>): "
        + "ERROR: sql_error [application_error]", d.message);
  }
  @Test public void unsafeStatementFallsBackToSkeleton() {
    SafeSqlDiagnostic d = SafeSqlDiagnostic.reconstructed(
        "INSERT INTO \"t\"\n(\"c\") VALUES (<redacted>)",   // embedded newline -> unsafe
        DiagnosticCategory.SQL_ERROR, null, null);
    Assert.assertEquals("<redacted> [type=SQLException, category=sql_error]", d.message);
  }
  @Test public void skeleton() {
    Assert.assertEquals(
        "<redacted> [type=SQLException, category=connection_exception, sqlState=08S01]",
        SafeSqlDiagnostic.skeleton(SafeSqlDiagnostic.Kind.SQL_EXCEPTION,
            DiagnosticCategory.CONNECTION_EXCEPTION, "08S01").message);
  }
}
