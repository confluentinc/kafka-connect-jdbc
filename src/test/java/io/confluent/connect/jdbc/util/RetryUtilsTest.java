/*
 * Copyright 2024 Confluent Inc.
 */

package io.confluent.connect.jdbc.util;

import org.junit.Test;

import java.sql.SQLException;
import java.sql.SQLNonTransientException;
import java.sql.SQLTransientException;
import java.util.Collections;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class RetryUtilsTest {

  @Test
  public void shouldRetryOnTransientException() {
    SQLException exception = new SQLTransientException("temporary issue");

    assertTrue("Transient exceptions should be retried", RetryUtils.shouldRetry(exception));
  }

  @Test
  public void shouldNotRetryOnNonTransientException() {
    SQLException exception = new SQLNonTransientException("permanent failure");

    assertFalse("Non transient exceptions must not be retried", RetryUtils.shouldRetry(exception));
  }

  @Test
  public void shouldRetryWhenErrorCodeRegistered() {
    int customErrorCode = 987654;
    RetryUtils.addRetriableErrorCodes(Collections.singleton(customErrorCode));
    SQLException exception = new SQLException("custom error", "S0001", customErrorCode);

    assertTrue("Registered error codes should be retried", RetryUtils.shouldRetry(exception));
  }
}

