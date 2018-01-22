/**
 * Copyright 2015 Confluent Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 **/

package io.confluent.connect.jdbc;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.config.types.Password;
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

import io.confluent.connect.jdbc.util.CachedConnectionProvider;
import io.confluent.connect.jdbc.source.JdbcSourceConnectorConfig;
import io.confluent.connect.jdbc.source.JdbcSourceTask;
import io.confluent.connect.jdbc.source.JdbcSourceTaskConfig;
import io.confluent.connect.jdbc.source.TableMonitorThread;
import io.confluent.connect.jdbc.util.StringUtils;
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
  private Map<String, String> inlineViewsDefinitions;

  @Override
  public String version() {
    return Version.getVersion();
  }

  @Override
  public void start(Map<String, String> properties) throws ConnectException {
    try {
      configProperties = properties;
      config = new JdbcSourceConnectorConfig(configProperties);
    } catch (ConfigException e) {
      throw new ConnectException("Couldn't start JdbcSourceConnector due to configuration "
                                 + "error", e);
    }

    final String dbUrl = config.getString(JdbcSourceConnectorConfig.CONNECTION_URL_CONFIG);
    final String dbUser = config.getString(JdbcSourceConnectorConfig.CONNECTION_USER_CONFIG);
    final Password dbPassword = config.getPassword(
        JdbcSourceConnectorConfig.CONNECTION_PASSWORD_CONFIG
    );
    final int maxConnectionAttempts = config.getInt(
        JdbcSourceConnectorConfig.CONNECTION_ATTEMPTS_CONFIG
    );
    final long connectionRetryBackoff = config.getLong(
        JdbcSourceConnectorConfig.CONNECTION_BACKOFF_CONFIG
    );
    cachedConnectionProvider = new CachedConnectionProvider(
        dbUrl,
        dbUser,
        dbPassword == null ? null : dbPassword.value(),
        maxConnectionAttempts,
        connectionRetryBackoff
    );

    // Initial connection attempt
    cachedConnectionProvider.getValidConnection();

    List<String> whitelist = config.getList(JdbcSourceConnectorConfig.TABLE_WHITELIST_CONFIG);
    String inlineViewTag = config.getString(JdbcSourceConnectorConfig
        .INLINE_VIEW_TAG_CONFIG);
    this.inlineViewsDefinitions = this.mapDefinitionsToInlineViews(whitelist,
      StringUtils.getListFromStringValueWithEscapedCommas(
          config.getString(JdbcSourceConnectorConfig.INLINE_VIEWS_DEFINITIONS_CONFIG), false),
          inlineViewTag
      );
    
    Set<String> whitelistSet = whitelist.isEmpty() ? null : new HashSet<>(whitelist);
    List<String> blacklist = config.getList(JdbcSourceConnectorConfig.TABLE_BLACKLIST_CONFIG);
    Set<String> blacklistSet = blacklist.isEmpty() ? null : new HashSet<>(blacklist);
    List<String> tableTypes =  config.getList(JdbcSourceConnectorConfig.TABLE_TYPE_CONFIG);
    Set<String> tableTypesSet =  new HashSet<>(tableTypes);


    if (whitelistSet != null && blacklistSet != null) {
      throw new ConnectException(JdbcSourceConnectorConfig.TABLE_WHITELIST_CONFIG + " and "
                                 + JdbcSourceConnectorConfig.TABLE_BLACKLIST_CONFIG + " are "
                                 + "exclusive.");
    }
    String query = config.getString(JdbcSourceConnectorConfig.QUERY_CONFIG);
    String schemaPattern = config.getString(JdbcSourceConnectorConfig.SCHEMA_PATTERN_CONFIG);
    if (!query.isEmpty()) {
      if (whitelistSet != null || blacklistSet != null) {
        throw new ConnectException(JdbcSourceConnectorConfig.QUERY_CONFIG + " may not be combined"
                                   + " with whole-table copying settings.");
      }
      // Force filtering out the entire set of tables since the one task we'll generate is for the
      // query.
      whitelistSet = Collections.emptySet();

    }
    long tablePollMs = config.getLong(JdbcSourceConnectorConfig.TABLE_POLL_INTERVAL_MS_CONFIG);
    tableMonitorThread = new TableMonitorThread(
        cachedConnectionProvider,
        context,
        schemaPattern,
        tablePollMs,
        whitelistSet,
        blacklistSet,
        tableTypesSet,
        inlineViewTag
    );
    tableMonitorThread.start();
  }

  @Override
  public Class<? extends Task> taskClass() {
    return JdbcSourceTask.class;
  }

  @Override
  public List<Map<String, String>> taskConfigs(int maxTasks) {
    String query = config.getString(JdbcSourceConnectorConfig.QUERY_CONFIG);
    if (!query.isEmpty()) {
      List<Map<String, String>> taskConfigs = new ArrayList<>(1);
      Map<String, String> taskProps = new HashMap<>(configProperties);
      taskProps.put(JdbcSourceTaskConfig.TABLES_CONFIG, "");
      taskConfigs.add(taskProps);
      return taskConfigs;
    } else {
      List<String> currentTables = tableMonitorThread.tables();
      int numGroups = Math.min(currentTables.size(), maxTasks);
      List<List<String>> tablesGrouped = ConnectorUtils.groupPartitions(currentTables, numGroups);
      List<Map<String, String>> taskConfigs = new ArrayList<>(tablesGrouped.size());
      for (List<String> taskTables : tablesGrouped) {
        List<String> taskInlineViewsDefinitions = new ArrayList<>();
        for (String table : taskTables) {
          if (inlineViewsDefinitions != null && inlineViewsDefinitions.keySet().contains(table)) {
            taskInlineViewsDefinitions.add(
                StringUtils.augmentCommaEscapingForTaskConfigParsing(
                    inlineViewsDefinitions.get(table)));
          }
        }
        Map<String, String> taskProps = new HashMap<>(configProperties);
        taskProps.put(JdbcSourceTaskConfig.TABLES_CONFIG,
                      StringUtils.join(taskTables, ","));
        taskProps.put(JdbcSourceTaskConfig.TASK_INLINE_VIEWS_DEFINITIONS_CONFIG,
            StringUtils.join(taskInlineViewsDefinitions, ","));
        taskConfigs.add(taskProps);
      }
      return taskConfigs;
    }
  }

  @Override
  public void stop() throws ConnectException {
    log.info("Stopping table monitoring thread");
    tableMonitorThread.shutdown();
    try {
      tableMonitorThread.join(MAX_TIMEOUT);
    } catch (InterruptedException e) {
      // Ignore, shouldn't be interrupted
    }
    cachedConnectionProvider.closeQuietly();
  }

  @Override
  public ConfigDef config() {
    return JdbcSourceConnectorConfig.CONFIG_DEF;
  }
  
  private Map<String, String> mapDefinitionsToInlineViews(List<String> whitelist,
      List<String> inlineViewsDefinitionsConfig, String viewDefinitionTag) {
    List<String> inlineViewsList = new ArrayList<>();
    for (String whiteListElement : whitelist) {
      if (whiteListElement.startsWith(viewDefinitionTag)) {
        log.debug("'" + whiteListElement + "' is a view");
        inlineViewsList.add(whiteListElement);
      }
    }
    if (inlineViewsList.size() == 0) {
      log.debug("no inline views were listed in "
          + JdbcSourceConnectorConfig.TABLE_WHITELIST_CONFIG);
      return null;
    }
    if (inlineViewsDefinitionsConfig.isEmpty()) {
      throw new ConnectException(JdbcSourceConnectorConfig.INLINE_VIEWS_DEFINITIONS_CONFIG
          + " must not be empty when views are declared in "
              + JdbcSourceConnectorConfig.TABLE_WHITELIST_CONFIG + " (each prefixed by "
              + JdbcSourceConnectorConfig.INLINE_VIEW_TAG_CONFIG + ").");
    }
    for (String inlineViewDefinition : inlineViewsDefinitionsConfig) {
      log.debug(JdbcSourceConnectorConfig.INLINE_VIEWS_DEFINITIONS_CONFIG + " element : "
          + inlineViewDefinition);
      if (! (inlineViewDefinition.trim().startsWith("(")
          && inlineViewDefinition.trim().endsWith(")"))) {
        throw new ConnectException("Each view definition in "
            + JdbcSourceConnectorConfig.INLINE_VIEWS_DEFINITIONS_CONFIG + " must be enclosed "
                + "in parentheses.");
      }
    }
    Map<String, String> inlineViewsWithDefinition = new HashMap<>();
    try {
      inlineViewsWithDefinition = StringUtils.listsToMap(inlineViewsList,
          inlineViewsDefinitionsConfig);
    } catch (IllegalArgumentException e) {
      throw new ConnectException(JdbcSourceConnectorConfig.INLINE_VIEWS_DEFINITIONS_CONFIG
          + " must contain the same number of views definitions than views declared in "
          + JdbcSourceConnectorConfig.TABLE_WHITELIST_CONFIG + ", in the same order."
          + "Possible solution : check SQL comma escaping (with 4 backslashes before "
          + "each comma : \\\\\\\\,).");
    }
    log.debug("inlineViewsWithDefinition : " + inlineViewsWithDefinition);
    return inlineViewsWithDefinition;
  }
  
}
