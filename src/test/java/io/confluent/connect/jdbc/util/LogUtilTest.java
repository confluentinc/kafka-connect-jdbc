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
