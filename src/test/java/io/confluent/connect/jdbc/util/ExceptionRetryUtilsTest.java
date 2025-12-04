/*
 * Copyright 2024 Confluent Inc.
 */

package io.confluent.connect.jdbc.util;

import org.junit.Test;

import java.sql.SQLException;
import java.sql.SQLNonTransientException;
import java.sql.SQLTransientException;
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
    assertTrue("Transient exceptions should be retried", ExceptionRetryUtils.shouldRetry(exception, customErrorCodes));
  }

  @Test
  public void shouldNotRetryOnNonTransientException() {
    SQLException exception = new SQLNonTransientException("permanent failure");
    Set<Integer> customErrorCodes = new HashSet<>();
    customErrorCodes.add(40001);
    assertFalse("Non transient exceptions must not be retried", ExceptionRetryUtils.shouldRetry(exception, customErrorCodes));
  }

  @Test
  public void shouldRetryWhenErrorCodeRegistered() {
    int customErrorCode = 1466;
    Set<Integer> customErrorCodes = new HashSet<>();
    customErrorCodes.add(40001);
    SQLException exception = new SQLException("custom error", "S0001", customErrorCode);

    assertTrue("Registered error codes should be retried", ExceptionRetryUtils.shouldRetry(exception, customErrorCodes));
  }
}

