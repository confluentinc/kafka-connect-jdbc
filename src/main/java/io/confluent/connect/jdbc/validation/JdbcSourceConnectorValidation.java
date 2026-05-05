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

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
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
          && validateQueryConfigs()
          && validateQuerySemantics();

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
   * Validate the user query through the dialect's constant-false probe. Surfaces syntax,
   * missing-object and {@code SELECT}-permission errors without requiring
   * {@code EXPLAIN PLAN} privileges and without scanning user data.
   *
   * @return {@code true} if validation passes or no query is configured;
   *         {@code false} on {@link SQLException}
   */
  protected boolean validateQuerySemantics() {
    Optional<String> queryVal = config.getQuery();
    if (!queryVal.isPresent()) {
      log.info("Query validation skipped: no 'query' or 'query.masked' configured");
      return true;
    }

    String query = queryVal.get();
    String configKey = config.isQueryMasked()
        ? JdbcSourceConnectorConfig.QUERY_MASKED_CONFIG
        : JdbcSourceConnectorConfig.QUERY_CONFIG;

    DatabaseDialect dialect = null;
    boolean connectionOpened = false;
    long startMs = System.currentTimeMillis();

    try {
      log.info("Starting query validation [configKey='{}']", configKey);

      dialect = createDialect();
      log.info("Resolved dialect: {} [sanitizedUrl='{}', connectionUser='{}', "
              + "DriverManager.loginTimeoutSec={}]",
          dialect.getClass().getSimpleName(),
          maskJdbcUrl(
              config.getString(JdbcSourceConnectorConfig.CONNECTION_URL_CONFIG)),
          config.getString(JdbcSourceConnectorConfig.CONNECTION_USER_CONFIG),
          DriverManager.getLoginTimeout());

      // Force-load the JDBC driver from the dialect's classloader. Kafka Connect
      // runs validation in a thread whose context classloader does not see the
      // connector's plugin JARs, so JDBC 4.0 SPI auto-discovery in
      // DriverManager.getConnection() can fail with "No suitable driver found"
      // even though the runtime task path works fine.
      preloadJdbcDriverFromDialect(
          dialect,
          config.getString(JdbcSourceConnectorConfig.CONNECTION_URL_CONFIG));

      logRegisteredJdbcDrivers();

      log.info("Attempting to open database connection for query validation");
      long connectStartMs = System.currentTimeMillis();
      try (Connection connection = dialect.getConnection()) {
        connectionOpened = true;
        long connectElapsedMs = System.currentTimeMillis() - connectStartMs;

        logConnectionMetadata(connection, connectElapsedMs);

        log.info("Invoking dialect.validateQuery() to execute the validation probe");
        long queryStartMs = System.currentTimeMillis();
        dialect.validateQuery(connection, query);
        log.info("Query validation probe succeeded in {} ms",
            System.currentTimeMillis() - queryStartMs);
      }
      log.info("Query validation completed successfully [totalMs={}]",
          System.currentTimeMillis() - startMs);
      return true;
    } catch (SQLException e) {
      long elapsedMs = System.currentTimeMillis() - startMs;
      log.info("Query validation FAILED with SQLException [connectionOpened={}, "
              + "totalMs={}, topLevel.SQLState={}, topLevel.errorCode={}, "
              + "topLevel.type={}, topLevel.message='{}']",
          connectionOpened, elapsedMs,
          e.getSQLState(), e.getErrorCode(),
          e.getClass().getName(), e.getMessage());
      logSqlExceptionChain(e);
      logCauseChain(e);
      log.info("Query validation SQLException stack trace:", e);

      String msg = "The configured query is not valid and has database/connection errors"
          + ". Please provide the correct query by validating the "
          + "query syntax and the existing table/column names with the database being connected";
      if (e.getSQLState() != null) {
        msg += " (SQLState: " + e.getSQLState() + ")";
      }
      addConfigError(configKey, msg);
      return false;
    } catch (Exception e) {
      long elapsedMs = System.currentTimeMillis() - startMs;
      log.info("Query validation hit non-SQL exception [connectionOpened={}, totalMs={}, "
              + "type={}, message='{}']",
          connectionOpened, elapsedMs, e.getClass().getName(), e.getMessage());
      logCauseChain(e);
      log.info("Query validation non-SQL exception stack trace:", e);
      return true;
    } finally {
      if (dialect != null) {
        try {
          dialect.close();
          log.info("Dialect closed after query validation [totalMs={}]",
              System.currentTimeMillis() - startMs);
        } catch (Exception closeEx) {
          log.warn("Error while closing dialect after query validation", closeEx);
        }
      }
    }
  }

  /**
   * Create a {@link DatabaseDialect} instance for the current configuration.
   * This method is protected to allow override in tests.
   *
   * @return a new dialect instance; never null
   */
  protected DatabaseDialect createDialect() {
    final String dialectName = config.getString(
        JdbcSourceConnectorConfig.DIALECT_NAME_CONFIG);
    if (dialectName != null && !dialectName.trim().isEmpty()) {
      return DatabaseDialects.create(dialectName, config);
    }
    return DatabaseDialects.findBestFor(
        config.getString(JdbcSourceConnectorConfig.CONNECTION_URL_CONFIG), config);
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

  /**
   * Map of JDBC URL prefix to the canonical driver class name. Used by
   * {@link #preloadJdbcDriverFromDialect} to force-register the driver from the
   * dialect's classloader, working around Kafka Connect plugin classloader
   * isolation that would otherwise prevent JDBC 4.0 SPI auto-discovery from
   * finding the driver during validation.
   */
  private static final Map<String, String> JDBC_DRIVER_CLASS_BY_URL_PREFIX;

  static {
    Map<String, String> m = new HashMap<>();
    m.put("jdbc:db2:",        "com.ibm.db2.jcc.DB2Driver");
    m.put("jdbc:postgresql:", "org.postgresql.Driver");
    m.put("jdbc:mysql:",      "com.mysql.cj.jdbc.Driver");
    m.put("jdbc:mariadb:",    "org.mariadb.jdbc.Driver");
    m.put("jdbc:sqlserver:",  "com.microsoft.sqlserver.jdbc.SQLServerDriver");
    m.put("jdbc:oracle:",     "oracle.jdbc.OracleDriver");
    m.put("jdbc:derby:",      "org.apache.derby.jdbc.EmbeddedDriver");
    m.put("jdbc:sqlite:",     "org.sqlite.JDBC");
    m.put("jdbc:vertica:",    "com.vertica.jdbc.Driver");
    m.put("jdbc:sap:",        "com.sap.db.jdbc.Driver");
    m.put("jdbc:sybase:",     "com.sybase.jdbc4.jdbc.SybDriver");
    JDBC_DRIVER_CLASS_BY_URL_PREFIX = java.util.Collections.unmodifiableMap(m);
  }

  /**
   * Force-load the JDBC driver class for the given URL using the dialect's own
   * classloader. Kafka Connect validates connectors on threads whose context
   * classloader does not see the connector's plugin JARs, so
   * {@link DriverManager#getConnection(String)} cannot rely on JDBC 4.0 SPI
   * auto-discovery to locate the driver. Calling
   * {@link Class#forName(String, boolean, ClassLoader)} from the dialect's
   * classloader triggers the driver's static initializer, which registers it
   * with {@code DriverManager}; the registration is process-global and visible
   * from every thread.
   *
   * <p>If the URL prefix is unknown or the driver class is missing, this method
   * logs the failure and returns silently. The subsequent {@code getConnection()}
   * call will surface the underlying {@code SQLException} with full context.
   */
  private void preloadJdbcDriverFromDialect(DatabaseDialect dialect, String url) {
    if (url == null) {
      return;
    }
    String driverClass = null;
    for (Map.Entry<String, String> entry : JDBC_DRIVER_CLASS_BY_URL_PREFIX.entrySet()) {
      if (url.toLowerCase(java.util.Locale.ROOT).startsWith(entry.getKey())) {
        driverClass = entry.getValue();
        break;
      }
    }
    if (driverClass == null) {
      log.info("No known JDBC driver class for URL prefix; relying on SPI discovery "
          + "[sanitizedUrl='{}']", maskJdbcUrl(url));
      return;
    }
    ClassLoader dialectCl = dialect.getClass().getClassLoader();
    try {
      Class<?> loaded = Class.forName(driverClass, true, dialectCl);
      log.info("Preloaded JDBC driver class '{}' from dialect classloader [codeSource='{}']",
          loaded.getName(),
          loaded.getProtectionDomain().getCodeSource() != null
              ? loaded.getProtectionDomain().getCodeSource().getLocation()
              : "unknown");
    } catch (ClassNotFoundException ex) {
      log.info("JDBC driver class '{}' not found on dialect classloader; "
              + "falling back to SPI discovery in DriverManager [error='{}']",
          driverClass, ex.getMessage());
    }
  }

  /**
   * Log every {@link java.sql.Driver} currently registered with
   * {@link DriverManager}, including the classloader and code-source of each
   * driver class. This pinpoints whether
   * {@code "No suitable driver found"} stems from an unregistered driver
   * (driver class never loaded) or from {@link DriverManager}'s caller-classloader
   * filtering rejecting a driver that IS registered but lives in a different
   * classloader (e.g. the framework rather than the connector plugin).
   */
  private void logRegisteredJdbcDrivers() {
    try {
      java.util.Enumeration<java.sql.Driver> drivers = DriverManager.getDrivers();
      int count = 0;
      while (drivers.hasMoreElements()) {
        java.sql.Driver d = drivers.nextElement();
        Class<?> dc = d.getClass();
        String codeSource;
        try {
          codeSource = dc.getProtectionDomain().getCodeSource() != null
              ? dc.getProtectionDomain().getCodeSource().getLocation().toString()
              : "unknown";
        } catch (Throwable ignored) {
          codeSource = "unknown";
        }
        log.info("Registered JDBC driver #{}: class='{}', classLoader='{}', codeSource='{}'",
            count++, dc.getName(),
            dc.getClassLoader() != null ? dc.getClassLoader().toString() : "bootstrap",
            codeSource);
      }
      if (count == 0) {
        log.info("DriverManager has zero registered drivers");
      }
    } catch (Throwable t) {
      log.info("Could not enumerate registered JDBC drivers [{}: {}]",
          t.getClass().getSimpleName(), t.getMessage());
    }
  }

  /**
   * Log driver and database metadata for diagnostic purposes. Wrapped in a broad
   * catch because metadata reads must never break the validation flow — a buggy
   * driver, partially-initialised connection, or test mock returning {@code null}
   * should not abort validation. Catches {@link Throwable} (rather than
   * {@link Exception}) deliberately, so that test mocks raising
   * {@link AssertionError} for unexpected calls also fall through safely.
   */
  private void logConnectionMetadata(Connection connection, long connectElapsedMs) {
    try {
      DatabaseMetaData md = connection.getMetaData();
      if (md == null) {
        log.info("Connection opened in {} ms [metadata unavailable: getMetaData() returned null]",
            connectElapsedMs);
        return;
      }
      log.info("Connection opened in {} ms [driver='{} {}', database='{} {}', "
              + "autoCommit={}, isolation={}]",
          connectElapsedMs,
          md.getDriverName(), md.getDriverVersion(),
          md.getDatabaseProductName(), md.getDatabaseProductVersion(),
          connection.getAutoCommit(), connection.getTransactionIsolation());
    } catch (Throwable t) {
      log.info("Connection opened in {} ms [metadata read failed: {}: {}]",
          connectElapsedMs, t.getClass().getSimpleName(), t.getMessage());
    }
  }

  /**
   * Walk and log every {@link SQLException#getNextException() chained SQLException} below
   * the head exception. JDBC drivers (DB2 in particular) often attach the real root cause
   * to the chain rather than to {@code getCause()}, so this is essential for diagnosis.
   */
  private void logSqlExceptionChain(SQLException head) {
    SQLException next = head.getNextException();
    int idx = 1;
    while (next != null && idx <= 10) {
      log.info("Query validation chained SQLException #{} [SQLState={}, errorCode={}, "
              + "type={}, message='{}']",
          idx, next.getSQLState(), next.getErrorCode(),
          next.getClass().getName(), next.getMessage());
      next = next.getNextException();
      idx++;
    }
  }

  /**
   * Walk and log the {@link Throwable#getCause() cause chain} of an exception. JDBC
   * driver failures often wrap a lower-level transport exception (e.g.
   * {@code SocketTimeoutException}, {@code SSLHandshakeException}) as the cause.
   */
  private void logCauseChain(Throwable head) {
    Throwable cause = head.getCause();
    int idx = 1;
    while (cause != null && idx <= 10) {
      log.info("Query validation cause #{} [type={}, message='{}']",
          idx, cause.getClass().getName(), cause.getMessage());
      cause = cause.getCause();
      idx++;
    }
  }

  /**
   * Mask common credential patterns in a JDBC URL so it can be logged safely. Handles
   * both {@code password=...} property style and {@code user:password@host} userinfo
   * style. Used only by validation diagnostic logs.
   */
  static String maskJdbcUrl(String url) {
    if (url == null) {
      return null;
    }
    return url.replaceAll("(?i)(password=)[^;&]*", "$1****")
        .replaceAll("(://[^:/?#]+:)[^@]*(@)", "$1****$2");
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