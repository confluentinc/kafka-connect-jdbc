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
import org.junit.Test;

import java.sql.BatchUpdateException;
import java.sql.SQLException;

import static org.junit.Assert.assertEquals;

public class LogUtilTest {
  private static final String REDACTED = "<redacted>";

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
  public void testDeprecatedTrimSensitiveDataSqlExceptionSanitizes() {
    SQLException sanitized =
        LogUtil.trimSensitiveData(new SQLException("secret value", "23505", 7));
    Assert.assertFalse(sanitized.getMessage().contains("secret value"));
  }

  @Test
  public void testDeprecatedTrimSensitiveDataThrowableSqlBranchSanitizes() {
    Throwable sanitized =
        LogUtil.trimSensitiveData((Throwable) new SQLException("secret value", "23505", 7));
    Assert.assertTrue(sanitized instanceof SQLException);
    Assert.assertFalse(sanitized.getMessage().contains("secret value"));
  }

  @Test
  public void testDeprecatedTrimSensitiveDataThrowableNonSqlPassesThrough() {
    Throwable t = new RuntimeException("secret");
    Assert.assertSame(t, LogUtil.trimSensitiveData(t));
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
