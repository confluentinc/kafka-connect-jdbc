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
import io.confluent.connect.jdbc.dialect.DatabaseDialects;
import io.confluent.connect.jdbc.sink.JdbcSinkConfig;
import org.apache.kafka.common.config.Config;
import org.apache.kafka.common.config.ConfigValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;

import static io.confluent.connect.jdbc.sink.JdbcSinkConfig.DELETE_ENABLED;
import static io.confluent.connect.jdbc.sink.JdbcSinkConfig.PK_MODE;
import static io.confluent.connect.jdbc.sink.JdbcSinkConfig.CONNECTION_URL;
import static io.confluent.connect.jdbc.sink.JdbcSinkConfig.PrimaryKeyMode.RECORD_KEY;

public class JdbcSinkConnectorValidation {
  private static final Logger log = LoggerFactory.getLogger(JdbcSinkConnectorValidation.class);
  protected JdbcSinkConfig config;
  protected Config validationResult;
  private final Map<String, String> connectorConfigs;

  public JdbcSinkConnectorValidation(JdbcSinkConfig config,
                                     Config validationResult) {
    this.config = config;
    this.validationResult = validationResult;
    this.connectorConfigs = null;
  }

  public JdbcSinkConnectorValidation(Map<String, String> connectorConfigs) {
    this.connectorConfigs = connectorConfigs;
  }

  public Config validate() {
    // First validate all configs to populate validationResult
    validateAll();

    // Always validate delete.enabled and pk.mode relationship
    validateDeleteEnabledPkMode();

    // Only try to connect if there are no errors so far
    if (!hasErrors()) {
      // Now we can safely create the config object
      if (config == null && connectorConfigs != null) {
        config = new JdbcSinkConfig(connectorConfigs);
      }
      validateConnection();
    }

    if (hasErrors()) {
      log.info("Validation failed");
    } else {
      log.info("Validation succeeded");
    }

    return this.validationResult;
  }

  private void validateAll() {
    if (validationResult == null && connectorConfigs != null) {
      Map<String, ConfigValue> configValuesMap = JdbcSinkConfig.CONFIG_DEF
          .validateAll(connectorConfigs);
      List<ConfigValue> configValues = new ArrayList<>(configValuesMap.values());
      validationResult = new Config(configValues);
    }
  }

  private void validateDeleteEnabledPkMode() {
    configValue(validationResult, DELETE_ENABLED)
        .filter(deleteEnabled -> Boolean.TRUE.equals(deleteEnabled.value()))
        .ifPresent(deleteEnabled -> configValue(validationResult, PK_MODE)
            .ifPresent(pkMode -> {
              if (!RECORD_KEY.name().toLowerCase(Locale.ROOT).equals(pkMode.value())
                  && !RECORD_KEY.name().toUpperCase(Locale.ROOT).equals(pkMode.value())) {
                String conflictMsg = "Deletes are only supported for pk.mode record_key";
                pkMode.addErrorMessage(conflictMsg);
                deleteEnabled.addErrorMessage(conflictMsg);
              }
            }));
  }

  private void validateConnection() {
    try (DatabaseDialect dialect = config.dialectName != null
        && !config.dialectName.trim().isEmpty()
        ? DatabaseDialects.create(config.dialectName, config)
        : DatabaseDialects.findBestFor(config.connectionUrl, config)) {
      dialect.getConnection();
    } catch (Exception e) {
      // Connection error - attach to connection.url config
      configValue(validationResult, CONNECTION_URL)
          .ifPresent(connectionUrl ->
              connectionUrl.addErrorMessage(
                  String.format("Could not connect to database. %s", e.getMessage())));
      log.error("SQLException during validation", e);
    }
  }

  private boolean hasErrors() {
    return validationResult.configValues().stream()
        .anyMatch(configValue -> !configValue.errorMessages().isEmpty());
  }

  /** only if individual validation passed. */
  private Optional<ConfigValue> configValue(Config config, String name) {
    return config.configValues()
        .stream()
        .filter(cfg -> name.equals(cfg.name())
            && cfg.errorMessages().isEmpty())
        .findFirst();
  }
}