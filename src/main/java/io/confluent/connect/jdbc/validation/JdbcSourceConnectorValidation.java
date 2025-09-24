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
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Validation class for JDBC Source Connector configurations.
 * This class handles all validation logic for the JDBC Source Connector,
 * including static config conflicts and mode-dependent requirements.
 */
public class JdbcSourceConnectorValidation {

  private static final Logger log = LoggerFactory.getLogger(JdbcSourceConnectorValidation.class);
  protected final JdbcSourceConnectorConfig jdbcConfigParser;
  private final Config connectorConfigValidationResult;

  public JdbcSourceConnectorValidation(Map<String, String> connectorConfigs, 
                                       Config connectorConfigValidationResult) {
    this.jdbcConfigParser = new JdbcSourceConnectorConfig(connectorConfigs);
    this.connectorConfigValidationResult = connectorConfigValidationResult;
  }

  /**
   * Perform validation and return the Config object with any validation errors.
   */
  public Config validate() {
    try {
      validateMultiConfigs();
      
      // Validate static config conflicts that don't require database access
      validateStaticConfigConflicts();

      validateModeColumnRequirements();
      
      // Validate plugin-specific needs (for future extensibility)
      validatePluginSpecificNeeds();
      
    } catch (Exception e) {
      log.error("Error during validation", e);
    }
    
    return connectorConfigValidationResult;
  }

  /**
   * Validate multi-configs (transaction isolation mode validation).
   */
  private void validateMultiConfigs() {
    HashMap<String, ConfigValue> configValues = new HashMap<>();
    connectorConfigValidationResult.configValues().stream()
        .filter((configValue) ->
            configValue.name().equals(
                JdbcSourceConnectorConfig.TRANSACTION_ISOLATION_MODE_CONFIG
            )
        ).forEach(configValue -> configValues.putIfAbsent(configValue.name(), configValue));

    TransactionIsolationMode transactionIsolationMode =
        TransactionIsolationMode.valueOf(
            jdbcConfigParser.getString(JdbcSourceConnectorConfig.TRANSACTION_ISOLATION_MODE_CONFIG)
        );
    if (transactionIsolationMode == TransactionIsolationMode.SQL_SERVER_SNAPSHOT) {
      DatabaseDialect dialect;
      final String dialectName = jdbcConfigParser.getString(
          JdbcSourceConnectorConfig.DIALECT_NAME_CONFIG);
      if (dialectName != null && !dialectName.trim().isEmpty()) {
        dialect = DatabaseDialects.create(dialectName, jdbcConfigParser);
      } else {
        dialect = DatabaseDialects.findBestFor(
            jdbcConfigParser.getString(JdbcSourceConnectorConfig.CONNECTION_URL_CONFIG), 
            jdbcConfigParser);
      }
      if (!dialect.name().equals(
          DatabaseDialects.create(
              "SqlServerDatabaseDialect", jdbcConfigParser
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
      }
    }
  }

  /**
   * Validate plugin-specific needs. This method can be overridden by specific
   * connector implementations to add their own validation logic.
   */
  protected void validatePluginSpecificNeeds() {
    // Default implementation - no plugin-specific validation
    // This can be overridden by specific connector implementations
  }

  /**
   * Validate static configuration conflicts that don't require database connectivity.
   * This includes mutually exclusive configurations and logical inconsistencies.
   */
  private void validateStaticConfigConflicts() {
    // Extract table filtering configurations
    List<String> whitelist = jdbcConfigParser.getList(
        JdbcSourceConnectorConfig.TABLE_WHITELIST_CONFIG);
    Set<String> whitelistSet = whitelist.isEmpty() ? null : new HashSet<>(whitelist);
    
    List<String> blacklist = jdbcConfigParser.getList(
        JdbcSourceConnectorConfig.TABLE_BLACKLIST_CONFIG);
    Set<String> blacklistSet = blacklist.isEmpty() ? null : new HashSet<>(blacklist);
    
    List<String> includeList = jdbcConfigParser.tableIncludeListRegexes();
    Set<String> includeListSet = includeList.isEmpty() ? null : new HashSet<>(includeList);
    
    List<String> excludeList = jdbcConfigParser.tableExcludeListRegexes();
    Set<String> excludeListSet = excludeList.isEmpty() ? null : new HashSet<>(excludeList);

    // Validate table filtering conflicts
    validateTableFilteringConflicts(whitelistSet, blacklistSet, includeListSet, 
        excludeListSet);
    
    // Validate column mapping conflicts
    validateColumnMappingConflicts();
  }

  /**
   * Validate table filtering configuration conflicts.
   */
  private void validateTableFilteringConflicts(Set<String> whitelistSet, 
                                               Set<String> blacklistSet,
                                               Set<String> includeListSet, 
                                               Set<String> excludeListSet) {
    // Legacy configs cannot be used together
    if (whitelistSet != null && blacklistSet != null) {
      String msg = "Legacy table filtering configurations are mutually exclusive. "
          + "Cannot use both " + JdbcSourceConnectorConfig.TABLE_WHITELIST_CONFIG 
          + " and " + JdbcSourceConnectorConfig.TABLE_BLACKLIST_CONFIG + " together.";
      addConfigError(JdbcSourceConnectorConfig.TABLE_WHITELIST_CONFIG, msg);
      addConfigError(JdbcSourceConnectorConfig.TABLE_BLACKLIST_CONFIG, msg);
    }

    // Exclude list requires include list
    if (excludeListSet != null && includeListSet == null) {
      String msg = JdbcSourceConnectorConfig.TABLE_EXCLUDE_LIST_CONFIG 
          + " cannot be used without " + JdbcSourceConnectorConfig.TABLE_INCLUDE_LIST_CONFIG
          + ". Exclude list only applies to tables that match the include list.";
      addConfigError(JdbcSourceConnectorConfig.TABLE_EXCLUDE_LIST_CONFIG, msg);
    }

    // Legacy and new configs are mutually exclusive
    boolean hasLegacyConfig = whitelistSet != null || blacklistSet != null;
    boolean hasNewConfig = includeListSet != null || excludeListSet != null;
    
    if (hasLegacyConfig && hasNewConfig) {
      List<String> configsUsed = new ArrayList<>();
      if (whitelistSet != null) {
        configsUsed.add(JdbcSourceConnectorConfig.TABLE_WHITELIST_CONFIG);
      }
      if (blacklistSet != null) {
        configsUsed.add(JdbcSourceConnectorConfig.TABLE_BLACKLIST_CONFIG);
      }
      if (includeListSet != null) {
        configsUsed.add(JdbcSourceConnectorConfig.TABLE_INCLUDE_LIST_CONFIG);
      }
      if (excludeListSet != null) {
        configsUsed.add(JdbcSourceConnectorConfig.TABLE_EXCLUDE_LIST_CONFIG);
      }
      
      String msg = "Cannot use legacy whitelist/blacklist with new include/exclude lists. "
          + "Configured: " + String.join(", ", configsUsed);
      
      if (whitelistSet != null) {
        addConfigError(JdbcSourceConnectorConfig.TABLE_WHITELIST_CONFIG, msg);
      }
      if (blacklistSet != null) {
        addConfigError(JdbcSourceConnectorConfig.TABLE_BLACKLIST_CONFIG, msg);
      }
      if (includeListSet != null) {
        addConfigError(JdbcSourceConnectorConfig.TABLE_INCLUDE_LIST_CONFIG, msg);
      }
      if (excludeListSet != null) {
        addConfigError(JdbcSourceConnectorConfig.TABLE_EXCLUDE_LIST_CONFIG, msg);
      }
    }
  }

  /**
   * Validate column mapping configuration conflicts.
   */
  private void validateColumnMappingConflicts() {
    // Get column configurations
    List<String> timestampColumnName = jdbcConfigParser.getList(
        JdbcSourceConnectorConfig.TIMESTAMP_COLUMN_NAME_CONFIG);
    List<String> timestampColumnsMapping = jdbcConfigParser.timestampColumnMapping();
    String incrementingColumnName = jdbcConfigParser.getString(
        JdbcSourceConnectorConfig.INCREMENTING_COLUMN_NAME_CONFIG);
    List<String> incrementingColumnMapping = jdbcConfigParser.incrementingColumnMapping();

    // Timestamp column conflicts
    boolean hasLegacyTimestampConfig = timestampColumnName != null 
        && !timestampColumnName.isEmpty() 
        && !timestampColumnName.get(0).trim().isEmpty();
    boolean hasNewTimestampConfig = timestampColumnsMapping != null 
        && !timestampColumnsMapping.isEmpty();
    
    if (hasLegacyTimestampConfig && hasNewTimestampConfig) {
      String msg = "Cannot use both timestamp.column.name and timestamp.columns.mapping. "
          + "Use timestamp.columns.mapping for new implementations.";
      addConfigError(JdbcSourceConnectorConfig.TIMESTAMP_COLUMN_NAME_CONFIG, msg);
      addConfigError(JdbcSourceConnectorConfig.TIMESTAMP_COLUMN_MAPPING_CONFIG, msg);
    }

    // Incrementing column conflicts
    boolean hasLegacyIncrementingConfig = incrementingColumnName != null 
        && !incrementingColumnName.trim().isEmpty();
    boolean hasNewIncrementingConfig = incrementingColumnMapping != null 
        && !incrementingColumnMapping.isEmpty();
    
    if (hasLegacyIncrementingConfig && hasNewIncrementingConfig) {
      String msg = "Cannot use both incrementing.column.name and incrementing.column.mapping. "
          + "Use incrementing.column.mapping for new implementations.";
      addConfigError(JdbcSourceConnectorConfig.INCREMENTING_COLUMN_NAME_CONFIG, msg);
      addConfigError(JdbcSourceConnectorConfig.INCREMENTING_COLUMN_MAPPING_CONFIG, msg);
    }
  }

  /**
   * Validate that mode-dependent column configurations are properly provided.
   */
  private void validateModeColumnRequirements() {
    String mode = jdbcConfigParser.getString(JdbcSourceConnectorConfig.MODE_CONFIG);
    
    // Get column configurations
    List<String> timestampColumnName = jdbcConfigParser.getList(
        JdbcSourceConnectorConfig.TIMESTAMP_COLUMN_NAME_CONFIG);
    List<String> timestampColumnsMapping = jdbcConfigParser.timestampColumnMapping();
    String incrementingColumnName = jdbcConfigParser.getString(
        JdbcSourceConnectorConfig.INCREMENTING_COLUMN_NAME_CONFIG);
    List<String> incrementingColumnMapping = jdbcConfigParser.incrementingColumnMapping();

    // Check if legacy configs are provided and non-empty
    boolean hasLegacyTimestampConfig = timestampColumnName != null 
        && !timestampColumnName.isEmpty() 
        && !timestampColumnName.get(0).trim().isEmpty();
    boolean hasNewTimestampConfig = timestampColumnsMapping != null 
        && !timestampColumnsMapping.isEmpty();
    boolean hasLegacyIncrementingConfig = incrementingColumnName != null 
        && !incrementingColumnName.trim().isEmpty();
    boolean hasNewIncrementingConfig = incrementingColumnMapping != null 
        && !incrementingColumnMapping.isEmpty();

    // Validate timestamp mode requirements
    if (JdbcSourceConnectorConfig.MODE_TIMESTAMP.equals(mode) 
        || JdbcSourceConnectorConfig.MODE_TIMESTAMP_INCREMENTING.equals(mode)) {
      if (!hasLegacyTimestampConfig && !hasNewTimestampConfig) {
        String msg = "Mode '" + mode + "' requires timestamp column configuration. "
            + "Provide either 'timestamp.column.name' or 'timestamp.columns.mapping'.";
        addConfigError(JdbcSourceConnectorConfig.TIMESTAMP_COLUMN_NAME_CONFIG, msg);
        addConfigError(JdbcSourceConnectorConfig.TIMESTAMP_COLUMN_MAPPING_CONFIG, msg);
      }
    }

    // Validate incrementing mode requirements
    if (JdbcSourceConnectorConfig.MODE_INCREMENTING.equals(mode) 
        || JdbcSourceConnectorConfig.MODE_TIMESTAMP_INCREMENTING.equals(mode)) {
      if (!hasLegacyIncrementingConfig && !hasNewIncrementingConfig) {
        String msg = "Mode '" + mode + "' requires incrementing column configuration. "
            + "Provide either 'incrementing.column.name' or 'incrementing.column.mapping'.";
        addConfigError(JdbcSourceConnectorConfig.INCREMENTING_COLUMN_NAME_CONFIG, msg);
        addConfigError(JdbcSourceConnectorConfig.INCREMENTING_COLUMN_MAPPING_CONFIG, msg);
      }
    }
  }

  /**
   * Helper method to add validation errors to config values.
   */
  private void addConfigError(String configName, String errorMessage) {
    connectorConfigValidationResult.configValues().stream()
        .filter(cv -> cv.name().equals(configName))
        .findFirst()
        .ifPresent(cv -> cv.addErrorMessage(errorMessage));
  }
}
