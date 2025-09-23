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

package io.confluent.connect.jdbc;

import org.apache.kafka.common.config.Config;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.source.SourceConnector;
import org.apache.kafka.connect.util.ConnectorUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import io.confluent.connect.jdbc.dialect.DatabaseDialect;
import io.confluent.connect.jdbc.dialect.DatabaseDialects;
import io.confluent.connect.jdbc.source.JdbcSourceConnectorConfig;
import io.confluent.connect.jdbc.source.JdbcSourceTask;
import io.confluent.connect.jdbc.source.JdbcSourceTaskConfig;
import io.confluent.connect.jdbc.source.TableMonitorThread;
import io.confluent.connect.jdbc.util.CachedConnectionProvider;
import io.confluent.connect.jdbc.util.ExpressionBuilder;
import io.confluent.connect.jdbc.util.TableId;
import io.confluent.connect.jdbc.util.Version;

/**
 * JdbcConnector is a Kafka Connect Connector implementation that watches a JDBC database and
 * generates tasks to ingest database contents.
 */
public class JdbcSourceConnector extends SourceConnector {

  private static final Logger log = LoggerFactory.getLogger(JdbcSourceConnector.class);

  private static final long MAX_TIMEOUT = 10000L;

  private Map<String, String> configProperties;
  private JdbcSourceConnectorConfig config;
  private CachedConnectionProvider cachedConnectionProvider;
  private TableMonitorThread tableMonitorThread;
  private DatabaseDialect dialect;

  @Override
  public String version() {
    return Version.getVersion();
  }

  @Override
  public void start(Map<String, String> properties) throws ConnectException {
    log.info("Starting JDBC Source Connector");
    log.info("Configuration properties: {}", properties);
    try {
      configProperties = properties;
      config = new JdbcSourceConnectorConfig(configProperties);
    } catch (ConfigException e) {
      throw new ConnectException("Couldn't start JdbcSourceConnector due to configuration error",
                                 e);
    }
    
    // Log table filtering configuration
    log.info("Table include list: {}", config.tableIncludeListRegexes());
    log.info("Table exclude list: {}", config.tableExcludeListRegexes());

    final String dbUrl = config.getString(JdbcSourceConnectorConfig.CONNECTION_URL_CONFIG);
    final int maxConnectionAttempts = config.getInt(
        JdbcSourceConnectorConfig.CONNECTION_ATTEMPTS_CONFIG
    );
    final long connectionRetryBackoff = config.getLong(
        JdbcSourceConnectorConfig.CONNECTION_BACKOFF_CONFIG
    );
    dialect = DatabaseDialects.findBestFor(
        dbUrl,
        config
    );
    cachedConnectionProvider = connectionProvider(maxConnectionAttempts, connectionRetryBackoff);
    log.info("Cached Connection Provider created");

    // Initial connection attempt
    log.info("Initial connection attempt with the database.");
    cachedConnectionProvider.getConnection();

    long tablePollMs = config.getLong(JdbcSourceConnectorConfig.TABLE_POLL_INTERVAL_MS_CONFIG);
    long tableStartupLimitMs =
        config.getLong(JdbcSourceConnectorConfig.TABLE_MONITORING_STARTUP_POLLING_LIMIT_MS_CONFIG);
    
    // Extract table filtering configurations for table monitor thread
    List<String> whitelist = config.getList(JdbcSourceConnectorConfig.TABLE_WHITELIST_CONFIG);
    Set<String> whitelistSet = whitelist.isEmpty() ? null : new HashSet<>(whitelist);
    
    List<String> blacklist = config.getList(JdbcSourceConnectorConfig.TABLE_BLACKLIST_CONFIG);
    Set<String> blacklistSet = blacklist.isEmpty() ? null : new HashSet<>(blacklist);
    
    List<String> includeList = config.tableIncludeListRegexes();
    Set<String> includeListSet = includeList.isEmpty() ? null : new HashSet<>(includeList);
    
    List<String> excludeList = config.tableExcludeListRegexes();
    Set<String> excludeListSet = excludeList.isEmpty() ? null : new HashSet<>(excludeList);

    String query = config.getString(JdbcSourceConnectorConfig.QUERY_CONFIG);
    if (!query.isEmpty()) {
      if (whitelistSet != null || blacklistSet != null 
          || includeListSet != null || excludeListSet != null) {
        log.error(
            "Configuration error: {} is set, but table filtering options are also specified. "
                + "These settings cannot be used together.",
            JdbcSourceConnectorConfig.QUERY_CONFIG);
        throw new ConnectException(JdbcSourceConnectorConfig.QUERY_CONFIG + " may not be combined"
                                   + " with whole-table copying settings.");
      }
      // Force filtering out the entire set of tables since the one task we'll generate is for the
      // query.
      whitelistSet = Collections.emptySet();
    }
    
    tableMonitorThread = new TableMonitorThread(
        dialect,
        cachedConnectionProvider,
        context,
        tableStartupLimitMs,
        tablePollMs,
        whitelistSet,      // Legacy
        blacklistSet,      // Legacy
        includeListSet,    // New
        excludeListSet,    // New
        Time.SYSTEM
    );
    if (query.isEmpty()) {
      tableMonitorThread.start();
      log.info("Starting Table Monitor Thread");
    }
  }

  protected CachedConnectionProvider connectionProvider(int maxConnAttempts, long retryBackoff) {
    return new CachedConnectionProvider(dialect, maxConnAttempts, retryBackoff);
  }

  /**
   * Validate table filtering configurations to ensure they are mutually exclusive.
   * This method ensures that legacy whitelist/blacklist configurations cannot be used
   * together with the new include/exclude list configurations.
   */
  private void validateTableFilteringConfigs(Set<String> whitelistSet, Set<String> blacklistSet, 
                                           Set<String> includeListSet, Set<String> excludeListSet) {
    validateLegacyConfigExclusivity(whitelistSet, blacklistSet);
    validateLegacyAndNewConfigExclusivity(whitelistSet, blacklistSet, 
                                          includeListSet, excludeListSet);
    warnOnEmptyIncludeWithExclude(includeListSet, excludeListSet);
  }

  private void validateLegacyConfigExclusivity(Set<String> whitelistSet, Set<String> blacklistSet) {
    if (whitelistSet != null && blacklistSet != null) {
      throw new ConnectException(
          "Legacy table filtering configurations are mutually exclusive. Cannot use both "
          + JdbcSourceConnectorConfig.TABLE_WHITELIST_CONFIG + " and "
          + JdbcSourceConnectorConfig.TABLE_BLACKLIST_CONFIG + " together."
      );
    }
  }

  private void validateLegacyAndNewConfigExclusivity(Set<String> whitelistSet, 
                                                    Set<String> blacklistSet,
                                                    Set<String> includeListSet, 
                                                    Set<String> excludeListSet) {
    boolean hasLegacyConfig = whitelistSet != null || blacklistSet != null;
    boolean hasNewConfig = includeListSet != null || excludeListSet != null;
    
    // Exclude list cannot exist without include list
    if (excludeListSet != null && includeListSet == null) {
      throw new ConnectException(
          JdbcSourceConnectorConfig.TABLE_EXCLUDE_LIST_CONFIG 
          + " cannot be used without " + JdbcSourceConnectorConfig.TABLE_INCLUDE_LIST_CONFIG
          + ". Exclude list only applies to tables that match the include list."
      );
    }
    
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
      
      throw new ConnectException(
          "Table filtering configurations are mutually exclusive. "
          + "Cannot use legacy whitelist/blacklist with new include/exclude lists. "
          + "Configured: " + String.join(", ", configsUsed)
      );
    }
  }

  private void warnOnEmptyIncludeWithExclude(Set<String> includeListSet, 
                                             Set<String> excludeListSet) {
    if (includeListSet != null && excludeListSet != null && includeListSet.isEmpty()) {
      log.warn("Include list is empty but exclude list is specified. No tables will be selected.");
    }
  }

  @Override
  public Class<? extends Task> taskClass() {
    return JdbcSourceTask.class;
  }

  @Override
  public Config validate(Map<String, String> connectorConfigs) {
    Config config = super.validate(connectorConfigs);
    JdbcSourceConnectorConfig jdbcSourceConnectorConfig
            = new JdbcSourceConnectorConfig(connectorConfigs);
    jdbcSourceConnectorConfig.validateMultiConfigs(config);
    
    // Validate static config conflicts that don't require database access
    validateStaticConfigConflicts(jdbcSourceConnectorConfig, config);
    
    // Validate mode-dependent column requirements
    validateModeColumnRequirements(jdbcSourceConnectorConfig, config);
    
    return config;
  }

  /**
   * Validate static configuration conflicts that don't require database connectivity.
   * This includes mutually exclusive configurations and logical inconsistencies.
   */
  private void validateStaticConfigConflicts(JdbcSourceConnectorConfig jdbcConfig, Config config) {
    // Extract table filtering configurations
    List<String> whitelist = jdbcConfig.getList(JdbcSourceConnectorConfig.TABLE_WHITELIST_CONFIG);
    Set<String> whitelistSet = whitelist.isEmpty() ? null : new HashSet<>(whitelist);
    
    List<String> blacklist = jdbcConfig.getList(JdbcSourceConnectorConfig.TABLE_BLACKLIST_CONFIG);
    Set<String> blacklistSet = blacklist.isEmpty() ? null : new HashSet<>(blacklist);
    
    List<String> includeList = jdbcConfig.tableIncludeListRegexes();
    Set<String> includeListSet = includeList.isEmpty() ? null : new HashSet<>(includeList);
    
    List<String> excludeList = jdbcConfig.tableExcludeListRegexes();
    Set<String> excludeListSet = excludeList.isEmpty() ? null : new HashSet<>(excludeList);

    // Validate table filtering conflicts
    validateTableFilteringConflicts(whitelistSet, blacklistSet, includeListSet, excludeListSet, 
        config);
    
    // Validate column mapping conflicts
    validateColumnMappingConflicts(jdbcConfig, config);
  }

  /**
   * Validate table filtering configuration conflicts.
   */
  private void validateTableFilteringConflicts(Set<String> whitelistSet, Set<String> blacklistSet,
                                               Set<String> includeListSet, 
                                               Set<String> excludeListSet,
                                               Config config) {
    // Legacy configs cannot be used together
    if (whitelistSet != null && blacklistSet != null) {
      addConfigError(config, JdbcSourceConnectorConfig.TABLE_WHITELIST_CONFIG,
          "Legacy table filtering configurations are mutually exclusive. Cannot use both "
          + JdbcSourceConnectorConfig.TABLE_WHITELIST_CONFIG + " and "
          + JdbcSourceConnectorConfig.TABLE_BLACKLIST_CONFIG + " together.");
      addConfigError(config, JdbcSourceConnectorConfig.TABLE_BLACKLIST_CONFIG,
          "Cannot use with " + JdbcSourceConnectorConfig.TABLE_WHITELIST_CONFIG);
    }

    // Exclude list requires include list
    if (excludeListSet != null && includeListSet == null) {
      addConfigError(config, JdbcSourceConnectorConfig.TABLE_EXCLUDE_LIST_CONFIG,
          JdbcSourceConnectorConfig.TABLE_EXCLUDE_LIST_CONFIG 
          + " cannot be used without " + JdbcSourceConnectorConfig.TABLE_INCLUDE_LIST_CONFIG
          + ". Exclude list only applies to tables that match the include list.");
    }

    // Legacy and new configs are mutually exclusive
    boolean hasLegacyConfig = whitelistSet != null || blacklistSet != null;
    boolean hasNewConfig = includeListSet != null || excludeListSet != null;
    
    if (hasLegacyConfig && hasNewConfig) {
      List<String> configsUsed = new ArrayList<>();
      if (whitelistSet != null) {
        configsUsed.add(JdbcSourceConnectorConfig.TABLE_WHITELIST_CONFIG);
        addConfigError(config, JdbcSourceConnectorConfig.TABLE_WHITELIST_CONFIG,
            "Cannot use legacy whitelist/blacklist with new include/exclude lists. "
            + "Configured: " + String.join(", ", configsUsed));
      }
      if (blacklistSet != null) {
        configsUsed.add(JdbcSourceConnectorConfig.TABLE_BLACKLIST_CONFIG);
        addConfigError(config, JdbcSourceConnectorConfig.TABLE_BLACKLIST_CONFIG,
            "Cannot use legacy whitelist/blacklist with new include/exclude lists. "
            + "Configured: " + String.join(", ", configsUsed));
      }
      if (includeListSet != null) {
        configsUsed.add(JdbcSourceConnectorConfig.TABLE_INCLUDE_LIST_CONFIG);
        addConfigError(config, JdbcSourceConnectorConfig.TABLE_INCLUDE_LIST_CONFIG,
            "Cannot use new include/exclude lists with legacy whitelist/blacklist. "
            + "Configured: " + String.join(", ", configsUsed));
      }
      if (excludeListSet != null) {
        configsUsed.add(JdbcSourceConnectorConfig.TABLE_EXCLUDE_LIST_CONFIG);
        addConfigError(config, JdbcSourceConnectorConfig.TABLE_EXCLUDE_LIST_CONFIG,
            "Cannot use new include/exclude lists with legacy whitelist/blacklist. "
            + "Configured: " + String.join(", ", configsUsed));
      }
    }
  }

  /**
   * Validate column mapping configuration conflicts.
   */
  private void validateColumnMappingConflicts(JdbcSourceConnectorConfig jdbcConfig, 
                                              Config config) {
    // Get column configurations
    List<String> timestampColumnName = jdbcConfig.getList(
        JdbcSourceConnectorConfig.TIMESTAMP_COLUMN_NAME_CONFIG);
    List<String> timestampColumnsMapping = jdbcConfig.timestampColumnMapping();
    String incrementingColumnName = jdbcConfig.getString(
        JdbcSourceConnectorConfig.INCREMENTING_COLUMN_NAME_CONFIG);
    List<String> incrementingColumnMapping = jdbcConfig.incrementingColumnMapping();

    // Timestamp column conflicts
    boolean hasLegacyTimestampConfig = timestampColumnName != null 
        && !timestampColumnName.isEmpty() 
        && !timestampColumnName.get(0).trim().isEmpty();
    boolean hasNewTimestampConfig = timestampColumnsMapping != null 
        && !timestampColumnsMapping.isEmpty();
    
    if (hasLegacyTimestampConfig && hasNewTimestampConfig) {
      addConfigError(config, JdbcSourceConnectorConfig.TIMESTAMP_COLUMN_NAME_CONFIG,
          "Cannot use both timestamp.column.name and timestamp.columns.mapping. "
          + "Use timestamp.columns.mapping for new implementations.");
      addConfigError(config, JdbcSourceConnectorConfig.TIMESTAMP_COLUMN_MAPPING_CONFIG,
          "Cannot use both timestamp.columns.mapping and timestamp.column.name. "
          + "Remove timestamp.column.name for new implementations.");
    }

    // Incrementing column conflicts
    boolean hasLegacyIncrementingConfig = incrementingColumnName != null 
        && !incrementingColumnName.trim().isEmpty();
    boolean hasNewIncrementingConfig = incrementingColumnMapping != null 
        && !incrementingColumnMapping.isEmpty();
    
    if (hasLegacyIncrementingConfig && hasNewIncrementingConfig) {
      addConfigError(config, JdbcSourceConnectorConfig.INCREMENTING_COLUMN_NAME_CONFIG,
          "Cannot use both incrementing.column.name and incrementing.column.mapping. "
          + "Use incrementing.column.mapping for new implementations.");
      addConfigError(config, JdbcSourceConnectorConfig.INCREMENTING_COLUMN_MAPPING_CONFIG,
          "Cannot use both incrementing.column.mapping and incrementing.column.name. "
          + "Remove incrementing.column.name for new implementations.");
    }
  }

  /**
   * Validate that mode-dependent column configurations are properly provided.
   */
  private void validateModeColumnRequirements(JdbcSourceConnectorConfig jdbcConfig, Config config) {
    String mode = jdbcConfig.getString(JdbcSourceConnectorConfig.MODE_CONFIG);
    
    // Get column configurations
    List<String> timestampColumnName = jdbcConfig.getList(
        JdbcSourceConnectorConfig.TIMESTAMP_COLUMN_NAME_CONFIG);
    List<String> timestampColumnsMapping = jdbcConfig.timestampColumnMapping();
    String incrementingColumnName = jdbcConfig.getString(
        JdbcSourceConnectorConfig.INCREMENTING_COLUMN_NAME_CONFIG);
    List<String> incrementingColumnMapping = jdbcConfig.incrementingColumnMapping();

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
        addConfigError(config, JdbcSourceConnectorConfig.TIMESTAMP_COLUMN_NAME_CONFIG,
            "Mode '" + mode + "' requires timestamp column configuration. "
            + "Provide either 'timestamp.column.name' or 'timestamp.columns.mapping'.");
        addConfigError(config, JdbcSourceConnectorConfig.TIMESTAMP_COLUMN_MAPPING_CONFIG,
            "Mode '" + mode + "' requires timestamp column configuration. "
            + "Provide either 'timestamp.column.name' or 'timestamp.columns.mapping'.");
      }
    }

    // Validate incrementing mode requirements
    if (JdbcSourceConnectorConfig.MODE_INCREMENTING.equals(mode) 
        || JdbcSourceConnectorConfig.MODE_TIMESTAMP_INCREMENTING.equals(mode)) {
      if (!hasLegacyIncrementingConfig && !hasNewIncrementingConfig) {
        addConfigError(config, JdbcSourceConnectorConfig.INCREMENTING_COLUMN_NAME_CONFIG,
            "Mode '" + mode + "' requires incrementing column configuration. "
            + "Provide either 'incrementing.column.name' or 'incrementing.column.mapping'.");
        addConfigError(config, JdbcSourceConnectorConfig.INCREMENTING_COLUMN_MAPPING_CONFIG,
            "Mode '" + mode + "' requires incrementing column configuration. "
            + "Provide either 'incrementing.column.name' or 'incrementing.column.mapping'.");
      }
    }
  }

  /**
   * Helper method to add validation errors to config values.
   */
  private void addConfigError(Config config, String configName, String errorMessage) {
    config.configValues().stream()
        .filter(cv -> cv.name().equals(configName))
        .findFirst()
        .ifPresent(cv -> cv.addErrorMessage(errorMessage));
  }

  @Override
  public List<Map<String, String>> taskConfigs(int maxTasks) {
    log.info("Starting with the task Configuration method.");
    String query = config.getString(JdbcSourceConnectorConfig.QUERY_CONFIG);
    List<Map<String, String>> taskConfigs;
    if (!query.isEmpty()) {
      log.info("Custom query provided, generating task configuration for the query");
      Map<String, String> taskProps = new HashMap<>(configProperties);
      taskProps.put(JdbcSourceTaskConfig.TABLES_CONFIG, "");
      taskProps.put(JdbcSourceTaskConfig.TABLES_FETCHED, "true");
      taskConfigs = Collections.singletonList(taskProps);
      log.trace("Producing task configs with custom query");
      return taskConfigs;
    } else {
      log.info("No custom query provided, generating task configurations for tables");
      List<TableId> currentTables = tableMonitorThread.tables();
      log.info("Current tables from tableMonitorThread: {}", currentTables);
      log.info("Number of tables found: {}", currentTables != null ? currentTables.size() : 0);
      
      if (currentTables == null || currentTables.isEmpty()) {
        taskConfigs = new ArrayList<>(1);
        Map<String, String> taskProps = new HashMap<>(configProperties);
        taskProps.put(JdbcSourceTaskConfig.TABLES_CONFIG, "");
        if (currentTables == null) {
          /*
          currentTables is only null when the connector is starting up/restarting. In this case we
          start the connector with 1 task with no tables assigned. This task does no do anything
          until the call to fetch all tables is completed. TABLES_FETCH config is used to tell the
          task to skip all processing. For more ref:
          https://github.com/confluentinc/kafka-connect-jdbc/pull/1348
           */
          taskProps.put(JdbcSourceTaskConfig.TABLES_FETCHED, "false");
          log.warn("The connector has not been able to read the "
              + "list of tables from the database yet.");
        } else {
          log.trace("currentTables is empty - no tables found after fetch");
          taskProps.put(JdbcSourceTaskConfig.TABLES_FETCHED, "true");
          log.warn("No tables were found so there's no work to be done.");
        }
        taskConfigs.add(taskProps);
      } else {
        log.trace("Found {} tables to process", currentTables.size());
        int numGroups = Math.min(currentTables.size(), maxTasks);
        List<List<TableId>> tablesGrouped =
            ConnectorUtils.groupPartitions(currentTables, numGroups);
        taskConfigs = new ArrayList<>(tablesGrouped.size());
        for (List<TableId> taskTables : tablesGrouped) {
          Map<String, String> taskProps = new HashMap<>(configProperties);
          ExpressionBuilder builder = dialect.expressionBuilder();
          builder.appendList().delimitedBy(",").of(taskTables);
          taskProps.put(JdbcSourceTaskConfig.TABLES_CONFIG, builder.toString());
          taskProps.put(JdbcSourceTaskConfig.TABLES_FETCHED, "true");
          log.trace("Assigned tables {} to task with tablesFetched=true", taskTables);
          taskConfigs.add(taskProps);
        }
        log.info("Current Tables size: {}", currentTables.size());
        log.trace(
            "Producing task configs with no custom query for tables: {}",
            currentTables.toArray()
        );
      }
    }
    return taskConfigs;
  }

  @Override
  public void stop() throws ConnectException {
    log.info("Stopping table monitoring thread");
    tableMonitorThread.shutdown();
    try {
      tableMonitorThread.join(MAX_TIMEOUT);
    } catch (InterruptedException e) {
      // Ignore, shouldn't be interrupted
    } finally {
      try {
        cachedConnectionProvider.close(true);
      } finally {
        try {
          if (dialect != null) {
            dialect.close();
          }
        } catch (Throwable t) {
          log.warn("Error while closing the {} dialect: ", dialect, t);
        } finally {
          dialect = null;
        }
      }
    }
  }

  @Override
  public ConfigDef config() {
    return JdbcSourceConnectorConfig.CONFIG_DEF;
  }
}
