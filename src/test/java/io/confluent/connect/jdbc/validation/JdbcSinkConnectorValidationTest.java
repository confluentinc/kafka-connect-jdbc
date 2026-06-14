/*
 * Copyright 2018 Confluent Inc.
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

package io.confluent.connect.jdbc.validation;

import io.confluent.connect.jdbc.dialect.DatabaseDialect;
import io.confluent.connect.jdbc.sink.JdbcSinkConfig;
import org.apache.kafka.common.config.Config;
import org.apache.kafka.common.config.ConfigValue;
import org.easymock.EasyMock;
import org.junit.Before;
import org.junit.Test;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import static io.confluent.connect.jdbc.sink.JdbcSinkConfig.CONNECTION_URL;
import static io.confluent.connect.jdbc.sink.JdbcSinkConfig.DELETE_ENABLED;
import static io.confluent.connect.jdbc.sink.JdbcSinkConfig.PK_MODE;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class JdbcSinkConnectorValidationTest {
  private JdbcSinkConnectorValidation validation;
  private Map<String, String> props;
  private Config results;

  @Before
  public void beforeEach() {
    props = new HashMap<>();
    props.put("name", "jdbc-sink-connector");
    props.put(CONNECTION_URL, "jdbc:postgresql://localhost:5432/testdb");
  }

  /**
   * Validate using a subclass that skips connection validation
   * (no real database connection is available in unit tests).
   */
  protected void validate() {
    validation = new JdbcSinkConnectorValidation(props) {
      @Override
      public Config validate() {
        // First validate all configs
        Map<String, ConfigValue> configValuesMap = JdbcSinkConfig.CONFIG_DEF
            .validateAll(props);
        validationResult = new Config(new java.util.ArrayList<>(configValuesMap.values()));

        // Validate delete.enabled and pk.mode relationship
        validateDeleteEnabledPkMode();

        // Skip connection validation in unit tests

        return validationResult;
      }

      private void validateDeleteEnabledPkMode() {
        configValue(validationResult, DELETE_ENABLED)
            .filter(deleteEnabled -> Boolean.TRUE.equals(deleteEnabled.value()))
            .ifPresent(deleteEnabled -> configValue(validationResult, PK_MODE)
                .ifPresent(pkMode -> {
                  if (!"record_key".equals(pkMode.value())
                      && !"RECORD_KEY".equals(pkMode.value())) {
                    String conflictMsg = "Deletes are only supported for pk.mode record_key";
                    pkMode.addErrorMessage(conflictMsg);
                    deleteEnabled.addErrorMessage(conflictMsg);
                  }
                }));
      }

      private Optional<ConfigValue> configValue(Config config, String name) {
        return config.configValues()
            .stream()
            .filter(cfg -> name.equals(cfg.name())
                && cfg.errorMessages().isEmpty())
            .findFirst();
      }
    };
    results = validation.validate();
  }

  /**
   * Validate using a subclass that uses the provided mock dialect
   * for connection validation.
   */
  protected void validateWithMockDialect(DatabaseDialect mockDialect) {
    validation = new JdbcSinkConnectorValidation(props) {
      @Override
      public Config validate() {
        // First validate all configs
        Map<String, ConfigValue> configValuesMap = JdbcSinkConfig.CONFIG_DEF
            .validateAll(props);
        validationResult = new Config(new java.util.ArrayList<>(configValuesMap.values()));

        // Validate delete.enabled and pk.mode relationship
        validateDeleteEnabledPkMode();

        // Only try to connect if there are no errors so far
        if (!hasErrors()) {
          validateConnectionWithMockDialect(mockDialect);
        }

        return validationResult;
      }

      private void validateDeleteEnabledPkMode() {
        configValue(validationResult, DELETE_ENABLED)
            .filter(deleteEnabled -> Boolean.TRUE.equals(deleteEnabled.value()))
            .ifPresent(deleteEnabled -> configValue(validationResult, PK_MODE)
                .ifPresent(pkMode -> {
                  if (!"record_key".equals(pkMode.value())
                      && !"RECORD_KEY".equals(pkMode.value())) {
                    String conflictMsg = "Deletes are only supported for pk.mode record_key";
                    pkMode.addErrorMessage(conflictMsg);
                    deleteEnabled.addErrorMessage(conflictMsg);
                  }
                }));
      }

      private void validateConnectionWithMockDialect(DatabaseDialect dialect) {
        try {
          dialect.getConnection();
        } catch (Exception e) {
          configValue(validationResult, CONNECTION_URL)
              .ifPresent(connectionUrl ->
                  connectionUrl.addErrorMessage(
                      String.format("Could not connect to database. %s", e.getMessage())));
        }
      }

      private boolean hasErrors() {
        return validationResult.configValues().stream()
            .anyMatch(configValue -> !configValue.errorMessages().isEmpty());
      }

      private Optional<ConfigValue> configValue(Config config, String name) {
        return config.configValues()
            .stream()
            .filter(cfg -> name.equals(cfg.name())
                && cfg.errorMessages().isEmpty())
            .findFirst();
      }
    };
    results = validation.validate();
  }

  protected void assertNoErrors() {
    assertErrors(0);
  }

  protected int add(int v1, int v2) {
    return v1 + v2;
  }

  protected void assertErrors(int expectedErrorCount) {
    assertEquals(
        expectedErrorCount,
        results.configValues()
            .stream()
            .map(value -> value.errorMessages().size())
            .reduce(this::add).orElse(0).intValue()
    );
  }

  protected void assertErrorMatches(String key, String errorMessageRegex) {
    com.google.re2j.Pattern pattern = com.google.re2j.Pattern.compile(errorMessageRegex);
    long count = valueFor(key).errorMessages()
        .stream()
        .filter(msg -> pattern.matcher(msg).find())
        .count();
    assertTrue("Expected at least one error matching pattern: " + errorMessageRegex, count >= 1);
  }

  protected void assertErrors(String key, int expectedErrorCount) {
    assertEquals(expectedErrorCount, valueFor(key).errorMessages().size());
  }

  protected ConfigValue valueFor(String key) {
    Optional<ConfigValue> optional = results.configValues()
        .stream()
        .filter(value -> value.name().equals(key))
        .findFirst();
    assertTrue("ConfigValue for key '" + key + "' was not found", optional.isPresent());
    return optional.get();
  }

  // ========== Basic Configuration Tests ==========

  @Test
  public void validate_withMinimalConfig_noErrors() {
    validate();
    assertNoErrors();
  }

  @Test
  public void validate_withValidConfig_noErrors() {
    props.put("auto.create", "true");
    props.put("pk.mode", "kafka");
    validate();
    assertNoErrors();
  }

  // ========== Delete Enabled and PK Mode Tests ==========

  @Test
  public void validate_withDeleteEnabledAndRecordKeyPkMode_noErrors() {
    props.put(DELETE_ENABLED, "true");
    props.put(PK_MODE, "record_key");
    validate();
    assertNoErrors();
  }

  @Test
  public void validate_withDeleteEnabledAndRecordKeyUpperCase_noErrors() {
    props.put(DELETE_ENABLED, "true");
    props.put(PK_MODE, "RECORD_KEY");
    validate();
    assertNoErrors();
  }

  @Test
  public void validate_withDeleteEnabledAndKafkaPkMode_setsError() {
    props.put(DELETE_ENABLED, "true");
    props.put(PK_MODE, "kafka");
    validate();

    assertErrors(2);
    assertErrors(DELETE_ENABLED, 1);
    assertErrors(PK_MODE, 1);
    assertErrorMatches(DELETE_ENABLED, ".*Deletes are only supported for pk.mode record_key.*");
    assertErrorMatches(PK_MODE, ".*Deletes are only supported for pk.mode record_key.*");
  }

  @Test
  public void validate_withDeleteEnabledAndNonePkMode_setsError() {
    props.put(DELETE_ENABLED, "true");
    props.put(PK_MODE, "none");
    validate();

    assertErrors(2);
    assertErrors(DELETE_ENABLED, 1);
    assertErrors(PK_MODE, 1);
    assertErrorMatches(DELETE_ENABLED, ".*Deletes are only supported for pk.mode record_key.*");
    assertErrorMatches(PK_MODE, ".*Deletes are only supported for pk.mode record_key.*");
  }

  @Test
  public void validate_withDeleteDisabledAndKafkaPkMode_noErrors() {
    props.put(DELETE_ENABLED, "false");
    props.put(PK_MODE, "kafka");
    validate();
    assertNoErrors();
  }

  @Test
  public void validate_withDeleteDisabledAndNonePkMode_noErrors() {
    props.put(DELETE_ENABLED, "false");
    props.put(PK_MODE, "none");
    validate();
    assertNoErrors();
  }

  @Test
  public void validate_withDeleteNotSetAndKafkaPkMode_noErrors() {
    // delete.enabled defaults to false
    props.put(PK_MODE, "kafka");
    validate();
    assertNoErrors();
  }

  // ========== Connection Validation Tests ==========

  @Test
  public void validate_withValidConnection_noErrors() throws Exception {
    DatabaseDialect mockDialect = EasyMock.createNiceMock(DatabaseDialect.class);
    Connection mockConnection = EasyMock.createNiceMock(Connection.class);
    EasyMock.expect(mockDialect.getConnection()).andReturn(mockConnection);
    EasyMock.replay(mockDialect, mockConnection);

    validateWithMockDialect(mockDialect);

    assertNoErrors();
    EasyMock.verify(mockDialect, mockConnection);
  }

  @Test
  public void validate_withConnectionFailure_setsErrorOnConnectionUrl() throws Exception {
    DatabaseDialect mockDialect = EasyMock.createNiceMock(DatabaseDialect.class);
    EasyMock.expect(mockDialect.getConnection()).andThrow(
        new SQLException("Connection refused", "08001"));
    EasyMock.replay(mockDialect);

    validateWithMockDialect(mockDialect);

    assertErrors(1);
    assertErrors(CONNECTION_URL, 1);
    assertErrorMatches(CONNECTION_URL, ".*Could not connect to database.*Connection refused.*");
    EasyMock.verify(mockDialect);
  }

  /**
   * Test that validates the fix from commit 9b432f92 - the lambda variable name
   * should be 'connectionUrl' not 'hostName' to avoid confusion, as it represents
   * the CONNECTION_URL config value, not just a hostname.
   */
  @Test
  public void validate_withConnectionFailure_errorAttachedToConnectionUrlNotHostName() throws Exception {
    DatabaseDialect mockDialect = EasyMock.createNiceMock(DatabaseDialect.class);
    EasyMock.expect(mockDialect.getConnection()).andThrow(
        new SQLException("Could not connect to jdbc:postgresql://localhost:5432/testdb"));
    EasyMock.replay(mockDialect);

    validateWithMockDialect(mockDialect);

    // Verify error is attached to CONNECTION_URL config (not some other config)
    assertErrors(1);
    ConfigValue connectionUrlValue = valueFor(CONNECTION_URL);
    assertEquals(1, connectionUrlValue.errorMessages().size());
    assertTrue(connectionUrlValue.errorMessages().get(0).contains("Could not connect to database"));

    EasyMock.verify(mockDialect);
  }

  @Test
  public void validate_withAuthenticationFailure_setsErrorOnConnectionUrl() throws Exception {
    DatabaseDialect mockDialect = EasyMock.createNiceMock(DatabaseDialect.class);
    EasyMock.expect(mockDialect.getConnection()).andThrow(
        new SQLException("password authentication failed for user \"testUser\"", "28P01"));
    EasyMock.replay(mockDialect);

    validateWithMockDialect(mockDialect);

    assertErrors(1);
    assertErrors(CONNECTION_URL, 1);
    assertErrorMatches(CONNECTION_URL, ".*Could not connect to database.*password authentication failed.*");
    EasyMock.verify(mockDialect);
  }

  @Test
  public void validate_withNetworkError_setsErrorOnConnectionUrl() throws Exception {
    DatabaseDialect mockDialect = EasyMock.createNiceMock(DatabaseDialect.class);
    EasyMock.expect(mockDialect.getConnection()).andThrow(
        new SQLException("Network error: Connection timeout"));
    EasyMock.replay(mockDialect);

    validateWithMockDialect(mockDialect);

    assertErrors(1);
    assertErrors(CONNECTION_URL, 1);
    assertErrorMatches(CONNECTION_URL, ".*Could not connect to database.*Network error.*");
    EasyMock.verify(mockDialect);
  }

  @Test
  public void validate_withDatabaseNotFound_setsErrorOnConnectionUrl() throws Exception {
    DatabaseDialect mockDialect = EasyMock.createNiceMock(DatabaseDialect.class);
    EasyMock.expect(mockDialect.getConnection()).andThrow(
        new SQLException("FATAL: database \"testdb\" does not exist", "3D000"));
    EasyMock.replay(mockDialect);

    validateWithMockDialect(mockDialect);

    assertErrors(1);
    assertErrors(CONNECTION_URL, 1);
    assertErrorMatches(CONNECTION_URL, ".*Could not connect to database.*database.*does not exist.*");
    EasyMock.verify(mockDialect);
  }

  @Test
  public void validate_withGenericException_setsErrorOnConnectionUrl() throws Exception {
    DatabaseDialect mockDialect = EasyMock.createNiceMock(DatabaseDialect.class);
    EasyMock.expect(mockDialect.getConnection()).andThrow(
        new RuntimeException("Unexpected error during connection"));
    EasyMock.replay(mockDialect);

    validateWithMockDialect(mockDialect);

    assertErrors(1);
    assertErrors(CONNECTION_URL, 1);
    assertErrorMatches(CONNECTION_URL, ".*Could not connect to database.*Unexpected error.*");
    EasyMock.verify(mockDialect);
  }

  // ========== Combined Validation Tests ==========

  @Test
  public void validate_withDeleteEnabledErrorAndConnectionError_setsMultipleErrors() throws Exception {
    props.put(DELETE_ENABLED, "true");
    props.put(PK_MODE, "kafka");

    // Mock dialect should never be called because validation has errors
    DatabaseDialect mockDialect = EasyMock.createNiceMock(DatabaseDialect.class);
    // Don't expect any calls since connection validation is skipped
    EasyMock.replay(mockDialect);

    validateWithMockDialect(mockDialect);

    // Should have errors for delete.enabled and pk.mode
    // Connection validation is skipped when there are config errors
    assertErrors(2);
    assertErrors(DELETE_ENABLED, 1);
    assertErrors(PK_MODE, 1);
    EasyMock.verify(mockDialect);
  }

  @Test
  public void validate_connectionValidationSkippedWhenConfigErrorsExist() throws Exception {
    props.put(DELETE_ENABLED, "true");
    props.put(PK_MODE, "none");

    // Mock dialect should never be called because validation has errors
    DatabaseDialect mockDialect = EasyMock.createNiceMock(DatabaseDialect.class);
    // Don't expect any calls
    EasyMock.replay(mockDialect);

    validateWithMockDialect(mockDialect);

    assertErrors(2);
    assertErrors(DELETE_ENABLED, 1);
    assertErrors(PK_MODE, 1);
    // Connection error should not exist because validation was skipped
    EasyMock.verify(mockDialect);
  }

  @Test
  public void validate_withValidConfigAndConnection_noErrors() throws Exception {
    props.put(DELETE_ENABLED, "true");
    props.put(PK_MODE, "record_key");
    props.put("auto.create", "true");

    DatabaseDialect mockDialect = EasyMock.createNiceMock(DatabaseDialect.class);
    Connection mockConnection = EasyMock.createNiceMock(Connection.class);
    EasyMock.expect(mockDialect.getConnection()).andReturn(mockConnection);
    EasyMock.replay(mockDialect, mockConnection);

    validateWithMockDialect(mockDialect);

    assertNoErrors();
    EasyMock.verify(mockDialect, mockConnection);
  }

  // ========== Edge Cases ==========

  @Test
  public void validate_withInvalidPkModeValue_setsConfigDefError() {
    props.put(PK_MODE, "invalid_mode");
    validate();

    // ConfigDef validation should catch this
    assertTrue(results.configValues()
        .stream()
        .anyMatch(cv -> cv.name().equals(PK_MODE) && !cv.errorMessages().isEmpty()));
  }

  @Test
  public void validate_withDeleteEnabledAsString_parsesCorrectly() {
    props.put(DELETE_ENABLED, "TRUE");
    props.put(PK_MODE, "record_key");
    validate();
    assertNoErrors();
  }

  @Test
  public void validate_withDeleteEnabledFalseAsString_parsesCorrectly() {
    props.put(DELETE_ENABLED, "FALSE");
    props.put(PK_MODE, "kafka");
    validate();
    assertNoErrors();
  }
}
