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
  protected final JdbcSourceConnectorConfig config;
  protected final Config validationResult;

  public JdbcSourceConnectorValidation(Map<String, String> connectorConfigs) {
    this.config = new JdbcSourceConnectorConfig(connectorConfigs);
    // Create Config object using ConfigDef.validateAll() approach
    Map<String, ConfigValue> configValuesMap = JdbcSourceConnectorConfig.CONFIG_DEF
        .validateAll(connectorConfigs);
    List<ConfigValue> configValues = new ArrayList<>(configValuesMap.values());
    this.validationResult = new Config(configValues);
  }

  /**
   * Perform validation and return the Config object with any validation errors.
   */
  public Config validate() {
    try {
      boolean validationResult = validateMultiConfigs()
          && validateTableFilteringExists()
          && validateStaticConfigConflicts()
          && validateModeColumnRequirements()
          && validatePluginSpecificNeeds();
      
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
   * Validate that at least one table filtering configuration exists.
   * The connector needs at least one way to determine which tables to process.
   */
  private boolean validateTableFilteringExists() {
    Set<String> whitelistSet = getTableWhitelistSet();
    Set<String> blacklistSet = getTableBlacklistSet();
    Set<String> includeListSet = getTableIncludeListSet();
    Set<String> excludeListSet = getTableExcludeListSet();
    
    boolean hasWhitelist = whitelistSet != null;
    boolean hasBlacklist = blacklistSet != null;
    boolean hasIncludeList = includeListSet != null;
    boolean hasExcludeList = excludeListSet != null;
    
    if (!hasWhitelist && !hasBlacklist && !hasIncludeList && !hasExcludeList) {
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
   * Validate static configuration conflicts that don't require database connectivity.
   * This includes mutually exclusive configurations and logical inconsistencies.
   */
  private boolean validateStaticConfigConflicts() {
    return validateTableFilteringConflicts() && validateColumnMappingConflicts();
  }

  /**
   * Validate table filtering configuration conflicts.
   */
  private boolean validateTableFilteringConflicts() {
    
    // Extract table filtering configurations using helper methods
    Set<String> whitelistSet = getTableWhitelistSet();
    Set<String> blacklistSet = getTableBlacklistSet();
    Set<String> includeListSet = getTableIncludeListSet();
    Set<String> excludeListSet = getTableExcludeListSet();

    // Exclude list requires include list
    if (excludeListSet != null && includeListSet == null) {
      String msg = JdbcSourceConnectorConfig.TABLE_EXCLUDE_LIST_CONFIG 
          + " cannot be used without " + JdbcSourceConnectorConfig.TABLE_INCLUDE_LIST_CONFIG
          + ". Exclude list only applies to tables that match the include list.";
      addConfigError(JdbcSourceConnectorConfig.TABLE_EXCLUDE_LIST_CONFIG, msg);
      return false;
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

      // Add error messages for all conflicting configs
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

      return false;
    }

    
    
    return true;
  }

  /**
   * Validate column mapping configuration conflicts.
   */
  private boolean validateColumnMappingConflicts() {
    boolean isValid = true;
    
    // Get column configurations using helper methods
    List<String> timestampColumnName = getTimestampColumnName();
    List<String> timestampColumnsMapping = getTimestampColumnMapping();
    String incrementingColumnName = getIncrementingColumnName();
    List<String> incrementingColumnMapping = getIncrementingColumnMapping();

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
      isValid = false;
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
      isValid = false;
    }
    
    return isValid;
  }

  /**
   * Validate that mode-dependent column configurations are properly provided.
   */
  private boolean validateModeColumnRequirements() {
    String mode = getMode();
    log.debug("validateModeColumnRequirements called with mode: {}", mode);
    
    // Get column configurations using helper methods
    List<String> timestampColumnName = getTimestampColumnName();
    List<String> timestampColumnsMapping = getTimestampColumnMapping();
    String incrementingColumnName = getIncrementingColumnName();
    List<String> incrementingColumnMapping = getIncrementingColumnMapping();
    
    log.debug("Config values - timestampColumnName: {}, timestampColumnsMapping: {}, "
        + "incrementingColumnName: {}, incrementingColumnMapping: {}", 
        timestampColumnName, timestampColumnsMapping,
            incrementingColumnName, incrementingColumnMapping);


    boolean hasLegacyTimestampConfig = timestampColumnName != null 
        && !timestampColumnName.isEmpty() 
        && !timestampColumnName.get(0).trim().isEmpty();
    boolean hasNewTimestampConfig = timestampColumnsMapping != null 
        && !timestampColumnsMapping.isEmpty();
    boolean hasLegacyIncrementingConfig = incrementingColumnName != null 
        && !incrementingColumnName.trim().isEmpty();
    boolean hasNewIncrementingConfig = incrementingColumnMapping != null 
        && !incrementingColumnMapping.isEmpty();

    // Validate mode-specific requirements
    if (JdbcSourceConnectorConfig.MODE_TIMESTAMP_INCREMENTING.equals(mode)) {
      // Mode TIMESTAMP_INCREMENTING: Need both timestamp AND incrementing configs
      if (!hasLegacyTimestampConfig && !hasNewTimestampConfig) {
        String msg = "Timestamp column configuration must be provided. "
            + "Provide either 'timestamp.column.name' or 'timestamp.columns.mapping'.";
        addConfigError(JdbcSourceConnectorConfig.MODE_CONFIG, msg);
        return false;
      }
      if (!hasLegacyIncrementingConfig && !hasNewIncrementingConfig) {
        String msg = "Incrementing column configuration must be provided. "
            + "Provide either 'incrementing.column.name' or 'incrementing.column.mapping'.";
        addConfigError(JdbcSourceConnectorConfig.MODE_CONFIG, msg);
        return false;
      }
    } else if (JdbcSourceConnectorConfig.MODE_TIMESTAMP.equals(mode)) {
      // Mode TIMESTAMP: Need timestamp config, should NOT have incrementing config
      if (!hasLegacyTimestampConfig && !hasNewTimestampConfig) {
        String msg = "Timestamp column configuration must be provided. "
            + "Provide either 'timestamp.column.name' or 'timestamp.columns.mapping'.";
        addConfigError(JdbcSourceConnectorConfig.MODE_CONFIG, msg);
        return false;
      }
      if (hasLegacyIncrementingConfig || hasNewIncrementingConfig) {
        String msg = "Incrementing column mappings should not be provided";
        addConfigError(JdbcSourceConnectorConfig.MODE_CONFIG, msg);
        return false;
      }
    } else if (JdbcSourceConnectorConfig.MODE_INCREMENTING.equals(mode)) {
      // Mode INCREMENTING: Need incrementing config, should NOT have timestamp config
      log.debug("MODE_INCREMENTING validation - hasLegacyTimestampConfig: {}, "
          + "hasNewTimestampConfig: {}", hasLegacyTimestampConfig, hasNewTimestampConfig);
      log.debug("timestampColumnsMapping: {}", timestampColumnsMapping);
      
      if (!hasLegacyIncrementingConfig && !hasNewIncrementingConfig) {
        String msg = "Incrementing column configuration must be provided. "
            + "Provide either 'incrementing.column.name' or 'incrementing.column.mapping'.";
        addConfigError(JdbcSourceConnectorConfig.MODE_CONFIG, msg);
        return false;
      }
      if (hasLegacyTimestampConfig || hasNewTimestampConfig) {
        log.debug("Adding error for conflicting timestamp config in MODE_INCREMENTING");
        final String msg = "Timestamp column mappings should not be provided";
        addConfigError(JdbcSourceConnectorConfig.MODE_CONFIG, msg);
        return false;
      }
    }

    return true;
  }

  /**
   * Helper method to add validation errors to config values.
   */
  private void addConfigError(String configName, String errorMessage) {
    validationResult.configValues().stream()
        .filter(cv -> cv.name().equals(configName))
        .findFirst()
        .ifPresent(cv -> cv.addErrorMessage(errorMessage));
  }

  // Helper methods for configuration access
  
  /**
   * Get table whitelist configuration as a set.
   */
  private Set<String> getTableWhitelistSet() {
    List<String> whitelist = config.getList(JdbcSourceConnectorConfig.TABLE_WHITELIST_CONFIG);
    return whitelist.isEmpty() ? null : new HashSet<>(whitelist);
  }
  
  /**
   * Get table blacklist configuration as a set.
   */
  private Set<String> getTableBlacklistSet() {
    List<String> blacklist = config.getList(JdbcSourceConnectorConfig.TABLE_BLACKLIST_CONFIG);
    return blacklist.isEmpty() ? null : new HashSet<>(blacklist);
  }
  
  /**
   * Get table include list configuration as a set.
   */
  private Set<String> getTableIncludeListSet() {
    List<String> includeList = config.tableIncludeListRegexes();
    return includeList.isEmpty() ? null : new HashSet<>(includeList);
  }
  
  /**
   * Get table exclude list configuration as a set.
   */
  private Set<String> getTableExcludeListSet() {
    List<String> excludeList = config.tableExcludeListRegexes();
    return excludeList.isEmpty() ? null : new HashSet<>(excludeList);
  }
  
  /**
   * Get timestamp column name configuration.
   */
  private List<String> getTimestampColumnName() {
    return config.getList(JdbcSourceConnectorConfig.TIMESTAMP_COLUMN_NAME_CONFIG);
  }
  
  /**
   * Get timestamp column mapping configuration.
   */
  private List<String> getTimestampColumnMapping() {
    return config.timestampColumnMapping();
  }
  
  /**
   * Get incrementing column name configuration.
   */
  private String getIncrementingColumnName() {
    return config.getString(JdbcSourceConnectorConfig.INCREMENTING_COLUMN_NAME_CONFIG);
  }
  
  /**
   * Get incrementing column mapping configuration.
   */
  private List<String> getIncrementingColumnMapping() {
    return config.incrementingColumnMapping();
  }
  
  /**
   * Get mode configuration.
   */
  private String getMode() {
    return config.getString(JdbcSourceConnectorConfig.MODE_CONFIG);
  }
}
