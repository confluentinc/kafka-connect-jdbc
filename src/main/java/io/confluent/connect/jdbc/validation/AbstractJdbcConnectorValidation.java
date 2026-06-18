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
import org.apache.kafka.common.config.Config;
import org.apache.kafka.common.config.ConfigValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Optional;

/**
 * Base class for JDBC connector validation with shared validation logic.
 */
public abstract class AbstractJdbcConnectorValidation {

  private static final Logger log = LoggerFactory.getLogger(AbstractJdbcConnectorValidation.class);

  protected Config validationResult;

  /**
   * Validate database connection by attempting to create a connection.
   * This method is protected to allow override in tests.
   *
   * @param connectionUrlConfigKey the config key name for the connection URL
   * @return true if connection succeeds, false otherwise
   */
  protected boolean validateConnection(String connectionUrlConfigKey) {
    DatabaseDialect dialect = null;
    try {
      dialect = createDialect();
      dialect.getConnection();
      return true;
    } catch (Exception e) {
      // Connection error - attach to connection.url config
      configValue(validationResult, connectionUrlConfigKey)
          .ifPresent(connectionUrlValue ->
              connectionUrlValue.addErrorMessage(
                  String.format("Could not connect to database. %s", e.getMessage())));
      log.error("SQLException during validation", e);
      return false;
    } finally {
      if (dialect != null) {
        dialect.close();
      }
    }
  }

  /**
   * Create a {@link DatabaseDialect} instance for the current configuration.
   * This method must be implemented by subclasses.
   *
   * @return a new dialect instance; never null
   */
  protected abstract DatabaseDialect createDialect();

  /**
   * Find a config value by name, only if it has no errors.
   * This helper is used to attach errors only to configs that passed individual validation.
   *
   * @param config the Config object
   * @param name the config key name
   * @return Optional containing the ConfigValue if found and error-free, empty otherwise
   */
  protected Optional<ConfigValue> configValue(Config config, String name) {
    return config.configValues()
        .stream()
        .filter(cfg -> name.equals(cfg.name())
            && cfg.errorMessages().isEmpty())
        .findFirst();
  }
}