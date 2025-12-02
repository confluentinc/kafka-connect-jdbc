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
import io.confluent.connect.jdbc.source.JdbcSourceConnectorConfig;
import io.confluent.connect.jdbc.source.JdbcSourceConnectorConfig.TransactionIsolationMode;
import org.apache.kafka.common.config.Config;
import org.apache.kafka.common.config.ConfigValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;

/**
 * Validation class for JDBC Source Connector configurations.
 * This class handles all validation logic for the JDBC Source Connector,
 * including static config conflicts and mode-dependent requirements.
 */
public class JdbcSourceConnectorValidation {

  private static final Logger log = LoggerFactory.getLogger(JdbcSourceConnectorValidation.class);
  private static final Pattern SELECT_STATEMENT_PATTERN =
      Pattern.compile("(?is)^SELECT\\b");
  protected JdbcSourceConnectorConfig config;
  protected Config validationResult;
  private final Map<String, String> connectorConfigs;

  public JdbcSourceConnectorValidation(JdbcSourceConnectorConfig config,
                                          Config validationResult) {
    this.config = config;
    this.validationResult = validationResult;
    this.connectorConfigs = null;
  }

  public JdbcSourceConnectorValidation(Map<String, String> connectorConfigs) {
    this.connectorConfigs = connectorConfigs;
  }


  /**
   * Perform validation and return the Config object with any validation errors.
   */
  public Config validate() {
    try {
      // Run validateAll() if not already done
      if (validationResult == null && connectorConfigs != null) {
        Map<String, ConfigValue> configValuesMap = JdbcSourceConnectorConfig.CONFIG_DEF
            .validateAll(connectorConfigs);
        List<ConfigValue> configValues = new ArrayList<>(configValuesMap.values());
        validationResult = new Config(configValues);
      }

      boolean hasValidateAllErrors = validationResult.configValues().stream()
              .anyMatch(configValue -> !configValue.errorMessages().isEmpty());

      if (hasValidateAllErrors) {
        log.info("Validation failed due to validator errors");
        return this.validationResult;
      }

      if (config == null && connectorConfigs != null) {
        config = new JdbcSourceConnectorConfig(connectorConfigs);
      }

      boolean validationResult = validateMultiConfigs()
          && validateLegacyNewConfigCompatibility()
          && validateQueryConfigs();

      if (validationResult && isUsingNewConfigs()) {
        validationResult = validateTableInclusionConfigs()
                           && validateTsAndIncModeColumnRequirements();
      }

      validationResult = validationResult && validatePluginSpecificNeeds();

      if (!validationResult) {
        log.info("Validation failed");
      } else {
        log.info("Validation succeeded");
      }

    } catch (Exception e) {
      log.error("Error during validation", e);
    }

    return this.validationResult;
  }

  /**
   * Validate multi-configs
   */
  private boolean validateMultiConfigs() {
    HashMap<String, ConfigValue> configValues = new HashMap<>();
    validationResult.configValues().stream()
        .filter((configValue) ->
            configValue.name().equals(
                JdbcSourceConnectorConfig.TRANSACTION_ISOLATION_MODE_CONFIG
            )
        ).forEach(configValue -> configValues.putIfAbsent(configValue.name(), configValue));

    TransactionIsolationMode transactionIsolationMode =
        TransactionIsolationMode.valueOf(
            config.getString(JdbcSourceConnectorConfig.TRANSACTION_ISOLATION_MODE_CONFIG)
        );
    if (transactionIsolationMode == TransactionIsolationMode.SQL_SERVER_SNAPSHOT) {
      DatabaseDialect dialect;
      final String dialectName = config.getString(
          JdbcSourceConnectorConfig.DIALECT_NAME_CONFIG);
      if (dialectName != null && !dialectName.trim().isEmpty()) {
        dialect = DatabaseDialects.create(dialectName, config);
      } else {
        dialect = DatabaseDialects.findBestFor(
            config.getString(JdbcSourceConnectorConfig.CONNECTION_URL_CONFIG),
                config);
      }
      if (!dialect.name().equals(
          DatabaseDialects.create(
              "SqlServerDatabaseDialect", config
          ).name()
      )
      ) {
        configValues
            .get(JdbcSourceConnectorConfig.TRANSACTION_ISOLATION_MODE_CONFIG)
            .addErrorMessage(
                "Isolation mode of `"
                    + TransactionIsolationMode.SQL_SERVER_SNAPSHOT.name()
                    + "` can only be configured with a Sql Server Dialect");
        log.warn(
            "Isolation mode of '{}' can only be configured with a Sql Server Dialect",
            TransactionIsolationMode.SQL_SERVER_SNAPSHOT.name());
        return false;
      }
    }
    return true;
  }

  /**
   * Validate legacy/new config compatibility and requirements.
   * Implements the pattern: legacyKeys vs newKeys with early returns.
   */
  private boolean validateLegacyNewConfigCompatibility() {
    // Define legacy and new config keys
    boolean usingLegacyConfigs = isUsingLegacyConfigs();
    boolean usingNewConfigs = isUsingNewConfigs();

    if (config.getQuery().isPresent()) {
      return true;
    }

    if (usingLegacyConfigs && usingNewConfigs) {
      return addConfigErrorsForLegacyAndNewConfigConflict();
    }

    if (!usingLegacyConfigs && !usingNewConfigs) {
      return addConfigErrorsForNoConfigProvided();
    }

    return true;
  }

  /**
   * Check if any legacy config keys are being used.
   * Legacy keys: table.whitelist, table.blacklist, incrementing.column.name, timestamp.column.name
   */
  private boolean isUsingLegacyConfigs() {
    Set<String> whitelistSet = config.getTableWhitelistSet();
    Set<String> blacklistSet = config.getTableBlacklistSet();
    String incrementingColumnName = config.getIncrementingColumnName();
    List<String> timestampColumnName = config.getTimestampColumnName();

    boolean hasWhitelist = !whitelistSet.isEmpty();
    boolean hasBlacklist = !blacklistSet.isEmpty();
    boolean hasLegacyIncrementing = incrementingColumnName != null
        && !incrementingColumnName.trim().isEmpty();
    boolean hasLegacyTimestamp = timestampColumnName != null
        && !timestampColumnName.isEmpty()
        && !timestampColumnName.get(0).trim().isEmpty();

    return hasWhitelist || hasBlacklist || hasLegacyIncrementing || hasLegacyTimestamp;
  }

  /**
   * Check if any new config keys are being used.
   * New keys: table.include.list, table.exclude.list, incrementing.column.mapping,
   * timestamp.columns.mapping
   */
  private boolean isUsingNewConfigs() {
    Set<String> includeListSet = config.getTableIncludeListSet();
    Set<String> excludeListSet = config.getTableExcludeListSet();
    List<String> incrementingColumnMapping = config.getIncrementingColumnMapping();
    List<String> timestampColumnsMapping = config.getTimestampColumnMapping();

    boolean hasIncludeList = !includeListSet.isEmpty();
    boolean hasExcludeList = !excludeListSet.isEmpty();
    boolean hasNewIncrementing = incrementingColumnMapping != null
        && !incrementingColumnMapping.isEmpty();
    boolean hasNewTimestamp = timestampColumnsMapping != null
        && !timestampColumnsMapping.isEmpty();

    return hasIncludeList || hasExcludeList || hasNewIncrementing || hasNewTimestamp;
  }

  /**
   * Validate conflict between legacy and new configs.
   * Only add errors to configs that are actually present and conflicting.
   */
  private boolean addConfigErrorsForLegacyAndNewConfigConflict() {
    String msg = "Cannot mix legacy and new configuration approaches. "
        + "Legacy configurations (table.whitelist, table.blacklist, timestamp.column.name, "
        + "incrementing.column.name) cannot be used together with new configurations "
        + "(table.include.list, table.exclude.list, timestamp.columns.mapping, "
        + "incrementing.column.mapping). Please choose one approach: either use all legacy "
        + "configurations or all new configurations.";

    // Only add errors to configs that are actually present and non-empty
    Set<String> whitelistSet = config.getTableWhitelistSet();
    if (!whitelistSet.isEmpty()) {
      addConfigError(JdbcSourceConnectorConfig.TABLE_WHITELIST_CONFIG, msg);
    }

    Set<String> blacklistSet = config.getTableBlacklistSet();
    if (!blacklistSet.isEmpty()) {
      addConfigError(JdbcSourceConnectorConfig.TABLE_BLACKLIST_CONFIG, msg);
    }

    Set<String> includeListSet = config.getTableIncludeListSet();
    if (!includeListSet.isEmpty()) {
      addConfigError(JdbcSourceConnectorConfig.TABLE_INCLUDE_LIST_CONFIG, msg);
    }

    Set<String> excludeListSet = config.getTableExcludeListSet();
    if (!excludeListSet.isEmpty()) {
      addConfigError(JdbcSourceConnectorConfig.TABLE_EXCLUDE_LIST_CONFIG, msg);
    }

    List<String> timestampColumnName = config.getTimestampColumnName();
    if (timestampColumnName != null && !timestampColumnName.isEmpty()
        && !timestampColumnName.get(0).trim().isEmpty()) {
      addConfigError(JdbcSourceConnectorConfig.TIMESTAMP_COLUMN_NAME_CONFIG, msg);
    }

    List<String> timestampColumnsMapping = config.getTimestampColumnMapping();
    if (timestampColumnsMapping != null && !timestampColumnsMapping.isEmpty()) {
      addConfigError(JdbcSourceConnectorConfig.TIMESTAMP_COLUMN_MAPPING_CONFIG, msg);
    }

    String incrementingColumnName = config.getIncrementingColumnName();
    if (incrementingColumnName != null && !incrementingColumnName.trim().isEmpty()) {
      addConfigError(JdbcSourceConnectorConfig.INCREMENTING_COLUMN_NAME_CONFIG, msg);
    }

    List<String> incrementingColumnMapping = config.getIncrementingColumnMapping();
    if (incrementingColumnMapping != null && !incrementingColumnMapping.isEmpty()) {
      addConfigError(JdbcSourceConnectorConfig.INCREMENTING_COLUMN_MAPPING_CONFIG, msg);
    }

    return false;
  }

  /**
   * Validate that at least one configuration is provided.
   */
  private boolean addConfigErrorsForNoConfigProvided() {
    String msg = "At least one table filtering configuration is required. "
        + "Provide one of: " + JdbcSourceConnectorConfig.TABLE_WHITELIST_CONFIG + ", "
        + JdbcSourceConnectorConfig.TABLE_BLACKLIST_CONFIG + ", "
        + JdbcSourceConnectorConfig.TABLE_INCLUDE_LIST_CONFIG + ", or "
        + JdbcSourceConnectorConfig.TABLE_EXCLUDE_LIST_CONFIG + ".";

    addConfigError(JdbcSourceConnectorConfig.TABLE_WHITELIST_CONFIG, msg);
    addConfigError(JdbcSourceConnectorConfig.TABLE_BLACKLIST_CONFIG, msg);
    addConfigError(JdbcSourceConnectorConfig.TABLE_INCLUDE_LIST_CONFIG, msg);
    addConfigError(JdbcSourceConnectorConfig.TABLE_EXCLUDE_LIST_CONFIG, msg);
    return false;
  }

  /**
   * Validate new config requirements (when using new configs only).
   */
  private boolean validateTableInclusionConfigs() {
    Set<String> includeListSet = config.getTableIncludeListSet();
    Set<String> excludeListSet = config.getTableExcludeListSet();

    // Validate that exclude list requires include list
    if (!excludeListSet.isEmpty() && includeListSet.isEmpty()) {
      String msg = JdbcSourceConnectorConfig.TABLE_EXCLUDE_LIST_CONFIG
          + " cannot be used without " + JdbcSourceConnectorConfig.TABLE_INCLUDE_LIST_CONFIG
          + ". Exclude list only applies to tables that match the include list.";
      addConfigError(JdbcSourceConnectorConfig.TABLE_EXCLUDE_LIST_CONFIG, msg);
      return false;
    }

    return true;
  }

  /**
   * Validate that only one of query or query.masked configs is set at a time.
   * Both configs should not be set simultaneously to avoid ambiguity.
   */
  private boolean validateQueryConfigs() {
    String query = config.getString(JdbcSourceConnectorConfig.QUERY_CONFIG);
    String queryMaskedValue = null;
    org.apache.kafka.common.config.types.Password queryMasked =
        config.getPassword(JdbcSourceConnectorConfig.QUERY_MASKED_CONFIG);
    if (queryMasked != null && queryMasked.value() != null) {
      queryMaskedValue = queryMasked.value();
    }

    boolean hasQuery = query != null && !query.isEmpty();
    boolean hasQueryMasked = queryMaskedValue != null && !queryMaskedValue.isEmpty();

    if (hasQuery && hasQueryMasked) {
      String msg = "Both 'query' and 'query.masked' configs cannot be set at the same time. "
          + "Please use only one of them.";

      addConfigError(JdbcSourceConnectorConfig.QUERY_CONFIG, msg);
      addConfigError(JdbcSourceConnectorConfig.QUERY_MASKED_CONFIG, msg);

      log.error("Validation failed: Both query and query.masked configs are set");
      return false;
    }

    if (config.getQuery().isPresent() && isUsingTableFilteringConfigs()) {
      String msg =
          "Do not specify table filtering configs with 'query'. "
              + "Remove table.whitelist / table.blacklist / table.include.list / "
              + "table.exclude.list when using query mode"
              + " or 'query' when using table filtering mode.";
      addConfigError(JdbcSourceConnectorConfig.QUERY_CONFIG, msg);
      addConfigError(JdbcSourceConnectorConfig.QUERY_MASKED_CONFIG, msg);
      if (!config.getTableWhitelistSet().isEmpty()) {
        addConfigError(JdbcSourceConnectorConfig.TABLE_WHITELIST_CONFIG, msg);
      }
      if (!config.getTableBlacklistSet().isEmpty()) {
        addConfigError(JdbcSourceConnectorConfig.TABLE_BLACKLIST_CONFIG, msg);
      }
      if (!config.getTableIncludeListSet().isEmpty()) {
        addConfigError(JdbcSourceConnectorConfig.TABLE_INCLUDE_LIST_CONFIG, msg);
      }
      if (!config.getTableExcludeListSet().isEmpty()) {
        addConfigError(JdbcSourceConnectorConfig.TABLE_EXCLUDE_LIST_CONFIG, msg);
      }
      return false;
    }

    if (hasQuery
        && !validateSelectStatement(query, JdbcSourceConnectorConfig.QUERY_CONFIG)) {
      return false;
    }
    if (hasQueryMasked
        && !validateSelectStatement(
            queryMaskedValue, JdbcSourceConnectorConfig.QUERY_MASKED_CONFIG)) {
      return false;
    }

    return true;
  }

  /**
   * Validate plugin-specific needs. This method can be overridden by specific
   * connector implementations to add their own validation logic.
   */
  protected boolean validatePluginSpecificNeeds() {
    // Default implementation - no plugin-specific validation
    // This can be overridden by specific connector implementations
    return true;
  }

  /**
   * Validate that mode-dependent column configurations are properly provided.
   */
  private boolean validateTsAndIncModeColumnRequirements() {
    return validateTsColProvidedWhenRequired()
           && validateTsColNotProvidedWhenNotRequired()
           && validateIncrColProvidedWhenRequired()
           && validateIncrColumnNotProvidedWhenNotRequired();
  }


  /**
   * Validate that timestamp column configuration is provided when required.
   * Accepts either legacy timestamp.column.name or new timestamp.columns.mapping.
   */
  private boolean validateTsColProvidedWhenRequired() {
    if (config.modeUsesTimestampColumn()) {
      List<String> timestampColumnsMapping = config.getTimestampColumnMapping();
      boolean hasNewTimestampConfig = timestampColumnsMapping != null
          && !timestampColumnsMapping.isEmpty();

      if (!hasNewTimestampConfig) {
        String msg = String.format(
            "Timestamp column configuration must be provided when using mode '%s' or '%s'. "
            + "Provide 'timestamp.columns.mapping'.",
            JdbcSourceConnectorConfig.MODE_TIMESTAMP,
            JdbcSourceConnectorConfig.MODE_TIMESTAMP_INCREMENTING
        );
        addConfigError(JdbcSourceConnectorConfig.MODE_CONFIG, msg);
        log.error(msg);
        return false;
      }
    }
    return true;
  }

  /**
   * Validate that timestamp column configuration is not provided when not required.
   * Rejects both legacy timestamp.column.name and new timestamp.columns.mapping.
   */
  private boolean validateTsColNotProvidedWhenNotRequired() {
    if (!config.modeUsesTimestampColumn()) {
      List<String> timestampColumnsMapping = config.getTimestampColumnMapping();

      boolean hasNewTimestampConfig = timestampColumnsMapping != null
          && !timestampColumnsMapping.isEmpty();

      if (hasNewTimestampConfig) {
        String msg = String.format(
            "Timestamp column configurations should not be provided if mode is not '%s' or '%s'. "
            + "Remove 'timestamp.columns.mapping'.",
            JdbcSourceConnectorConfig.MODE_TIMESTAMP,
            JdbcSourceConnectorConfig.MODE_TIMESTAMP_INCREMENTING
        );
        addConfigError(JdbcSourceConnectorConfig.MODE_CONFIG, msg);
        log.error(msg);
        return false;
      }
    }
    return true;
  }



  /**
   * Validate that incrementing column configuration is provided when required.
   * Accepts either legacy incrementing.column.name or new incrementing.column.mapping.
   */
  private boolean validateIncrColProvidedWhenRequired() {
    if (config.modeUsesIncrementingColumn()) {
      List<String> incrementingColumnMapping = config.getIncrementingColumnMapping();

      boolean hasNewIncrementingConfig = incrementingColumnMapping != null
          && !incrementingColumnMapping.isEmpty();

      if (!hasNewIncrementingConfig) {
        String msg = String.format(
            "Incrementing column configuration must be provided when using mode '%s' or '%s'. "
            + "Provide 'incrementing.column.mapping'.",
            JdbcSourceConnectorConfig.MODE_INCREMENTING,
            JdbcSourceConnectorConfig.MODE_TIMESTAMP_INCREMENTING
        );
        addConfigError(JdbcSourceConnectorConfig.MODE_CONFIG, msg);
        log.error(msg);
        return false;
      }
    }
    return true;
  }

  /**
   * Validate that incrementing column configuration is not provided when not required.
   * Rejects both legacy incrementing.column.name and new incrementing.column.mapping.
   */
  private boolean validateIncrColumnNotProvidedWhenNotRequired() {
    if (!config.modeUsesIncrementingColumn()) {
      List<String> incrementingColumnMapping = config.getIncrementingColumnMapping();

      boolean hasNewIncrementingConfig = incrementingColumnMapping != null
          && !incrementingColumnMapping.isEmpty();

      if (hasNewIncrementingConfig) {
        String msg = String.format(
            "Incrementing column configurations "
              + "should not be provided if mode is not '%s' or '%s'. "
              + "Remove 'incrementing.column.mapping'.",
            JdbcSourceConnectorConfig.MODE_INCREMENTING,
            JdbcSourceConnectorConfig.MODE_TIMESTAMP_INCREMENTING
        );
        addConfigError(JdbcSourceConnectorConfig.MODE_CONFIG, msg);
        log.error(msg);
        return false;
      }
    }
    return true;
  }

  /**
   * Helper method to add validation errors to config values.
   */
  protected void addConfigError(String configName, String errorMessage) {
    validationResult.configValues().stream()
        .filter(cv -> cv.name().equals(configName))
        .findFirst()
        .ifPresent(cv -> cv.addErrorMessage(errorMessage));
  }

  /** Validate that provided query strings start with a SELECT statement. */
  private boolean validateSelectStatement(String statement, String configKey) {
    String trimmedStatement = statement.trim();
    if (!SELECT_STATEMENT_PATTERN.matcher(trimmedStatement).find()) {
      String msg =
          String.format(
              "Only SELECT statements are supported for '%s'. Please provide "
              + "a statement that starts with SELECT.",
              configKey);
      addConfigError(configKey, msg);
      log.error(msg);
      return false;
    }
    return true;
  }

  /**
   * Determine whether any table filtering configurations are in use.
   */
  private boolean isUsingTableFilteringConfigs() {
    return !config.getTableWhitelistSet().isEmpty()
        || !config.getTableBlacklistSet().isEmpty()
        || !config.getTableIncludeListSet().isEmpty()
        || !config.getTableExcludeListSet().isEmpty();
  }

}