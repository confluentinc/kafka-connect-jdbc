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
          && validateTableFiltering()
          && validateStaticConfigConflicts()
          && validateTsAndIncModeColumnRequirements()
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
   * Validate table filtering configurations.
   * Ensures at least one table filtering configuration exists and validates new config
   * requirements.
   */
  private boolean validateTableFiltering() {
    Set<String> whitelistSet = getTableWhitelistSet();
    Set<String> blacklistSet = getTableBlacklistSet();
    Set<String> includeListSet = getTableIncludeListSet();
    Set<String> excludeListSet = getTableExcludeListSet();
    
    boolean hasWhitelist = whitelistSet != null;
    boolean hasBlacklist = blacklistSet != null;
    boolean hasIncludeList = includeListSet != null;
    boolean hasExcludeList = excludeListSet != null;
    
    // Validate that at least one table filtering configuration exists
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
    
    // Validate new table filtering requirements
    if (excludeListSet != null && includeListSet == null) {
      String msg = JdbcSourceConnectorConfig.TABLE_EXCLUDE_LIST_CONFIG 
          + " cannot be used without " + JdbcSourceConnectorConfig.TABLE_INCLUDE_LIST_CONFIG
          + ". Exclude list only applies to tables that match the include list.";
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
    
    Set<String> whitelistSet = getTableWhitelistSet();
    Set<String> blacklistSet = getTableBlacklistSet();
    Set<String> includeListSet = getTableIncludeListSet();
    Set<String> excludeListSet = getTableExcludeListSet();
    boolean hasLegacyConfig = whitelistSet != null || blacklistSet != null;
    boolean hasNewConfig = includeListSet != null || excludeListSet != null;


    if (hasLegacyConfig && hasNewConfig) {

      String msg = "Cannot use legacy whitelist/blacklist with new include/exclude lists. ";

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
    
    List<String> timestampColumnName = getTimestampColumnName();
    List<String> timestampColumnsMapping = getTimestampColumnMapping();
    List<String> incrementingColumnMapping = getIncrementingColumnMapping();

    Set<String> whitelistSet = getTableWhitelistSet();
    Set<String> blacklistSet = getTableBlacklistSet();
    boolean hasLegacyTableFiltering = whitelistSet != null || blacklistSet != null;
    boolean hasNewColumnMapping = (timestampColumnsMapping != null 
        && !timestampColumnsMapping.isEmpty())
        || (incrementingColumnMapping != null && !incrementingColumnMapping.isEmpty());
    
    if (hasLegacyTableFiltering && hasNewColumnMapping) {
      String msg = "Cannot use legacy table filtering (table.whitelist/table.blacklist) with new "
          + "column mapping (timestamp.columns.mapping/incrementing.column.mapping). "
          + "Use table.include.list with column mappings.";
      
      if (whitelistSet != null) {
        addConfigError(JdbcSourceConnectorConfig.TABLE_WHITELIST_CONFIG, msg);
      }
      if (blacklistSet != null) {
        addConfigError(JdbcSourceConnectorConfig.TABLE_BLACKLIST_CONFIG, msg);
      }
      if (timestampColumnsMapping != null && !timestampColumnsMapping.isEmpty()) {
        addConfigError(JdbcSourceConnectorConfig.TIMESTAMP_COLUMN_MAPPING_CONFIG, msg);
      }
      if (incrementingColumnMapping != null && !incrementingColumnMapping.isEmpty()) {
        addConfigError(JdbcSourceConnectorConfig.INCREMENTING_COLUMN_MAPPING_CONFIG, msg);
      }
      isValid = false;
    }

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

    // Check for table.include.list with legacy column configurations
    Set<String> includeListSet = getTableIncludeListSet();
    boolean hasNewTableFiltering = includeListSet != null;
    
    if (hasNewTableFiltering && hasLegacyTimestampConfig) {
      String msg = "Cannot use table.include.list with legacy timestamp.column.name. "
          + "Use timestamp.columns.mapping with table.include.list.";
      addConfigError(JdbcSourceConnectorConfig.TABLE_INCLUDE_LIST_CONFIG, msg);
      addConfigError(JdbcSourceConnectorConfig.TIMESTAMP_COLUMN_NAME_CONFIG, msg);
      isValid = false;
    }
    
    String incrementingColumnName = getIncrementingColumnName();
    boolean hasLegacyIncrementingConfig = incrementingColumnName != null 
        && !incrementingColumnName.trim().isEmpty();
    
    if (hasNewTableFiltering && hasLegacyIncrementingConfig) {
      String msg = "Cannot use table.include.list with legacy incrementing.column.name. "
          + "Use incrementing.column.mapping with table.include.list.";
      addConfigError(JdbcSourceConnectorConfig.TABLE_INCLUDE_LIST_CONFIG, msg);
      addConfigError(JdbcSourceConnectorConfig.INCREMENTING_COLUMN_NAME_CONFIG, msg);
      isValid = false;
    }
    
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
  private boolean validateTsAndIncModeColumnRequirements() {
    return validateTimestampColumnConfigProvidedWhenRequired()
        && validateTimestampColumnConfigNotProvidedWhenNotRequired()
        && validateIncrementingColumnConfigProvidedWhenRequired()
        && validateIncrementingColumnConfigNotProvidedWhenNotRequired();
  }


  /**
   * Validate that timestamp column configuration is provided when required.
   * Accepts either legacy timestamp.column.name or new timestamp.columns.mapping.
   */
  private boolean validateTimestampColumnConfigProvidedWhenRequired() {
    if (config.modeUsesTimestampColumn()) {
      List<String> timestampColumnName = getTimestampColumnName();
      List<String> timestampColumnsMapping = getTimestampColumnMapping();
      
      boolean hasLegacyTimestampConfig = timestampColumnName != null 
          && !timestampColumnName.isEmpty() 
          && !timestampColumnName.get(0).trim().isEmpty();
      boolean hasNewTimestampConfig = timestampColumnsMapping != null 
          && !timestampColumnsMapping.isEmpty();
      
      if (!hasLegacyTimestampConfig && !hasNewTimestampConfig) {
        String msg = String.format(
            "Timestamp column configuration must be provided when using mode '%s' or '%s'. "
            + "Provide either 'timestamp.column.name' or 'timestamp.columns.mapping'.",
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
  private boolean validateTimestampColumnConfigNotProvidedWhenNotRequired() {
    if (!config.modeUsesTimestampColumn()) {
      List<String> timestampColumnName = getTimestampColumnName();
      List<String> timestampColumnsMapping = getTimestampColumnMapping();
      
      boolean hasLegacyTimestampConfig = timestampColumnName != null 
          && !timestampColumnName.isEmpty() 
          && !timestampColumnName.get(0).trim().isEmpty();
      boolean hasNewTimestampConfig = timestampColumnsMapping != null 
          && !timestampColumnsMapping.isEmpty();
      
      if (hasLegacyTimestampConfig || hasNewTimestampConfig) {
        String msg = String.format(
            "Timestamp column configurations should not be provided if mode is not '%s' or '%s'. "
            + "Remove 'timestamp.column.name' or 'timestamp.columns.mapping'.",
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
  private boolean validateIncrementingColumnConfigProvidedWhenRequired() {
    if (config.modeUsesIncrementingColumn()) {
      String incrementingColumnName = getIncrementingColumnName();
      List<String> incrementingColumnMapping = getIncrementingColumnMapping();
      
      boolean hasLegacyIncrementingConfig = incrementingColumnName != null 
          && !incrementingColumnName.trim().isEmpty();
      boolean hasNewIncrementingConfig = incrementingColumnMapping != null 
          && !incrementingColumnMapping.isEmpty();
      
      if (!hasLegacyIncrementingConfig && !hasNewIncrementingConfig) {
        String msg = String.format(
            "Incrementing column configuration must be provided when using mode '%s' or '%s'. "
            + "Provide either 'incrementing.column.name' or 'incrementing.column.mapping'.",
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
  private boolean validateIncrementingColumnConfigNotProvidedWhenNotRequired() {
    if (!config.modeUsesIncrementingColumn()) {
      String incrementingColumnName = getIncrementingColumnName();
      List<String> incrementingColumnMapping = getIncrementingColumnMapping();
      
      boolean hasLegacyIncrementingConfig = incrementingColumnName != null 
          && !incrementingColumnName.trim().isEmpty();
      boolean hasNewIncrementingConfig = incrementingColumnMapping != null 
          && !incrementingColumnMapping.isEmpty();
      
      if (hasLegacyIncrementingConfig || hasNewIncrementingConfig) {
        String msg = String.format(
            "Incrementing column configurations "
              + "should not be provided if mode is not '%s' or '%s'. "
              + "Remove 'incrementing.column.name' or 'incrementing.column.mapping'.",
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
  
}
