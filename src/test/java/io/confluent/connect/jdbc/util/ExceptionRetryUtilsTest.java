/*
 * Copyright 2024 Confluent Inc.
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

import org.junit.Test;

import java.sql.SQLException;
import java.sql.SQLNonTransientException;
import java.sql.SQLRecoverableException;
import java.sql.SQLTransientException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class ExceptionRetryUtilsTest {

  @Test
  public void shouldRetryOnTransientException() {
    SQLException exception = new SQLTransientException("temporary issue");
    Set<Integer> customErrorCodes = new HashSet<>();
    customErrorCodes.add(1062); // MySQL duplicate entry
    assertTrue(ExceptionRetryUtils.shouldRetry(exception, customErrorCodes));
  }

  @Test
  public void shouldNotRetryOnNonTransientException() {
    SQLException exception = new SQLNonTransientException("permanent failure");
    Set<Integer> customErrorCodes = new HashSet<>();
    customErrorCodes.add(40001);
    assertFalse(ExceptionRetryUtils.shouldRetry(exception, customErrorCodes));
  }

  @Test
  public void shouldRetryWhenErrorCodeRegistered() {
    int customErrorCode = 1466;
    Set<Integer> customErrorCodes = new HashSet<>();
    customErrorCodes.add(40001);
    SQLException exception = new SQLException("custom error", "S0001", customErrorCode);

    assertTrue(ExceptionRetryUtils.shouldRetry(exception, customErrorCodes));
  }

  @Test
  public void shouldRetryWithDialectSpecificOracleErrorCodes() {
    // Test Oracle-specific error codes (e.g., ORA-17002 IO Error)
    Set<Integer> oracleErrorCodes = new HashSet<>(Arrays.asList(
        17002, 17008, 3113, 3114, 12170, 60, 1033, 1034
    ));

    // Test ORA-17002 (IO Error)
    SQLException oraException = new SQLException("IO Error", "S0001", 17002);
    assertTrue(ExceptionRetryUtils.shouldRetry(oraException, oracleErrorCodes));

    // Test ORA-60 (Deadlock)
    SQLException deadlockException = new SQLException("Deadlock", "S0001", 60);
    assertTrue(ExceptionRetryUtils.shouldRetry(deadlockException, oracleErrorCodes));
  }

  @Test
  public void shouldRetryWithDialectSpecificMySqlErrorCodes() {
    // Test MySQL-specific error codes
    Set<Integer> mysqlErrorCodes = new HashSet<>(Arrays.asList(
        1205, 1213, 2002, 2003, 2006, 2013, 1040
    ));

    // Test 1213 (Deadlock)
    SQLException mysqlDeadlock = new SQLException("Deadlock", "40001", 1213);
    assertTrue(ExceptionRetryUtils.shouldRetry(mysqlDeadlock, mysqlErrorCodes));

    // Test 2006 (MySQL server has gone away)
    SQLException mysqlGoneAway = new SQLException("MySQL server has gone away", "08S01", 2006);
    assertTrue(ExceptionRetryUtils.shouldRetry(mysqlGoneAway, mysqlErrorCodes));
  }

  @Test
  public void shouldRetryWithDialectSpecificSqlServerErrorCodes() {
    // Test SQL Server-specific error codes
    Set<Integer> sqlServerErrorCodes = new HashSet<>(Arrays.asList(
        1205, 1222, -2, -1, 10054, 10060, 40197, 40501, 40613
    ));

    // Test 1205 (Deadlock)
    SQLException sqlServerDeadlock = new SQLException("Deadlock victim", "40001", 1205);
    assertTrue(ExceptionRetryUtils.shouldRetry(sqlServerDeadlock, sqlServerErrorCodes));

    // Test -2 (Timeout)
    SQLException sqlServerTimeout = new SQLException("Timeout", "HYT00", -2);
    assertTrue(ExceptionRetryUtils.shouldRetry(sqlServerTimeout, sqlServerErrorCodes));

    // Test Azure-specific 40613
    SQLException azureNotAvailable = new SQLException("Database not available", "40613", 40613);
    assertTrue(ExceptionRetryUtils.shouldRetry(azureNotAvailable, sqlServerErrorCodes));
  }

  @Test
  public void shouldRetryWithDialectSpecificDb2ErrorCodes() {
    // Test DB2-specific error codes (negative values)
    Set<Integer> db2ErrorCodes = new HashSet<>(Arrays.asList(
        -911, -912, -913, -904, -30080, -30081
    ));

    // Test -911 (Deadlock/timeout rollback)
    SQLException db2Deadlock = new SQLException("Deadlock rollback", "40001", -911);
    assertTrue(ExceptionRetryUtils.shouldRetry(db2Deadlock, db2ErrorCodes));

    // Test -30080 (Communication error)
    SQLException db2CommError = new SQLException("Communication error", "08001", -30080);
    assertTrue(ExceptionRetryUtils.shouldRetry(db2CommError, db2ErrorCodes));
  }

  @Test
  public void shouldNotRetryWithEmptyErrorCodes() {
    // With empty error codes, only base retry logic should apply
    Set<Integer> emptyErrorCodes = new HashSet<>();

    // A regular SQLException with no special handling should not be retried
    SQLException regularException = new SQLException("Regular error", "S0001", 99999);
    assertFalse(ExceptionRetryUtils.shouldRetry(regularException, emptyErrorCodes));
  }

  @Test
  public void shouldRetryOnRecoverableException() {
    // SQLRecoverableException should be retried (unless connection is closed)
    SQLException recoverableException = new SQLRecoverableException("Recoverable error");
    Set<Integer> emptyErrorCodes = new HashSet<>();
    assertTrue(ExceptionRetryUtils.shouldRetry(recoverableException, emptyErrorCodes));
  }

  @Test
  public void shouldHandleNullErrorCodesSet() {
    SQLException exception = new SQLTransientException("temporary issue");
    // Passing null should not throw exception and should use default behavior
    assertTrue(ExceptionRetryUtils.shouldRetry(exception, null));
  }

  @Test
  public void shouldCombineMultipleDialectErrorCodes() {
    // Simulate combining error codes from multiple sources (config + dialect)
    Set<Integer> combinedCodes = new HashSet<>();
    // Add config error codes
    combinedCodes.add(12345);
    // Add dialect error codes (e.g., Oracle)
    combinedCodes.addAll(Arrays.asList(17002, 17008, 3113));

    // Test config error code
    SQLException configException = new SQLException("Config error", "S0001", 12345);
    assertTrue(ExceptionRetryUtils.shouldRetry(configException, combinedCodes));

    // Test dialect error code
    SQLException dialectException = new SQLException("Dialect error", "S0001", 17002);
    assertTrue(ExceptionRetryUtils.shouldRetry(dialectException, combinedCodes));
  }
}

