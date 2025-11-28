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

package io.confluent.connect.jdbc.source;

import io.confluent.connect.jdbc.util.LogUtil;
import io.confluent.connect.jdbc.util.TableId;
import io.confluent.connect.jdbc.util.TableCollectionUtils;
import io.confluent.connect.jdbc.util.ColumnDefinition;
import io.confluent.connect.jdbc.util.ColumnId;
import io.confluent.connect.jdbc.util.CachedConnectionProvider;
import io.confluent.connect.jdbc.util.RecordQueue;
import io.confluent.connect.jdbc.util.Version;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.SQLException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Set;
import java.time.ZoneId;
import java.util.TimeZone;
import java.util.stream.Collectors;

import io.confluent.connect.jdbc.dialect.DatabaseDialect;
import io.confluent.connect.jdbc.dialect.DatabaseDialects;
import io.confluent.connect.jdbc.source.JdbcSourceConnectorConfig.TransactionIsolationMode;

/**
 * JdbcSourceTask is a Kafka Connect SourceTask implementation that reads from JDBC databases and
 * generates Kafka Connect records.
 */
public class JdbcSourceTask extends SourceTask {

  private static final Logger log = LoggerFactory.getLogger(JdbcSourceTask.class);

  private Time time;
  private JdbcSourceTaskConfig config;
  private DatabaseDialect dialect;
  //Visible for Testing
  CachedConnectionProvider cachedConnectionProvider;
  PriorityQueue<TableQuerier> tableQueue = new PriorityQueue<>();
  protected RecordQueue<SourceRecord> engine;
  private TableQuerierProcessor tableQuerierProcessor;
  private final Map<String, String> tableToIncrCol = new HashMap<>();
  private final Map<String, List<String>> tableToTsCols = new HashMap<>();

  public JdbcSourceTask() {
    this.time = Time.SYSTEM;
  }

  public JdbcSourceTask(Time time) {
    this.time = time;
  }

  @Override
  public String version() {
    return Version.getVersion();
  }

  @Override
  public void start(Map<String, String> properties) {
    log.info("Starting JDBC source task");
    try {
      config = new JdbcSourceTaskConfig(properties);
    } catch (ConfigException e) {
      throw new ConfigException("Couldn't start JdbcSourceTask due to configuration error", e);
    }

    List<String> tables = config.getList(JdbcSourceTaskConfig.TABLES_CONFIG);
    Boolean tablesFetched = config.getBoolean(JdbcSourceTaskConfig.TABLES_FETCHED);
    List<String> tableType = config.getList(JdbcSourceConnectorConfig.TABLE_TYPE_CONFIG);

    if ((tables.isEmpty() && !config.getQuery().isPresent())) {
      // We are still waiting for the tables call to complete.
      // Start task but do nothing.
      if (!tablesFetched) {
        log.info("Started JDBC source task. Waiting for DB tables to be fetched.");
        return;
      }

      // Tables call has completed, but we didn't get any table assigned to this task
      throw new ConfigException("Task is being killed because"
              + " it was not assigned a table nor a query to execute."
              + " If run in table mode please make sure that the tables"
              + " exist on the database. If the table does exist on"
              + " the database, we recommend using the fully qualified"
              + " table name.");
    }

    if ((!tables.isEmpty() && config.getQuery().isPresent())) {
      throw new ConfigException("Invalid configuration: a JdbcSourceTask"
              + " cannot have both a table and a query assigned to it");
    }


    final String url = config.getString(JdbcSourceConnectorConfig.CONNECTION_URL_CONFIG);
    final int maxConnAttempts = config.getInt(JdbcSourceConnectorConfig.CONNECTION_ATTEMPTS_CONFIG);
    final long retryBackoff = config.getLong(JdbcSourceConnectorConfig.CONNECTION_BACKOFF_CONFIG);

    final String dialectName = config.getString(JdbcSourceConnectorConfig.DIALECT_NAME_CONFIG);
    if (dialectName != null && !dialectName.trim().isEmpty()) {
      dialect = DatabaseDialects.create(dialectName, config);
    } else {
      log.info("Finding the database dialect that is best fit for the provided JDBC URL.");
      dialect = DatabaseDialects.findBestFor(url, config);
    }
    log.info("Using JDBC dialect {}", dialect.name());

    cachedConnectionProvider = connectionProvider(maxConnAttempts, retryBackoff);


    dialect.setConnectionIsolationMode(
            cachedConnectionProvider.getConnection(),
            TransactionIsolationMode
                    .valueOf(
                            config.getString(
                                    JdbcSourceConnectorConfig
                                            .TRANSACTION_ISOLATION_MODE_CONFIG
                            )
                    )
    );
    TableQuerier.QueryMode queryMode =
        config.getQuery().isPresent() ? TableQuerier.QueryMode.QUERY : TableQuerier.QueryMode.TABLE;
    final List<String> tablesOrQuery = queryMode == TableQuerier.QueryMode.QUERY
                                 ? Collections.singletonList(config.getQuery().get()) : tables;

    String mode = config.getString(JdbcSourceTaskConfig.MODE_CONFIG);
    //used only in table mode
    Map<String, List<Map<String, String>>> partitionsByTableFqn = new HashMap<>();
    Map<Map<String, String>, Map<String, Object>> offsets = null;
    if (mode.equals(JdbcSourceTaskConfig.MODE_INCREMENTING)
        || mode.equals(JdbcSourceTaskConfig.MODE_TIMESTAMP)
        || mode.equals(JdbcSourceTaskConfig.MODE_TIMESTAMP_INCREMENTING)) {
      List<Map<String, String>> partitions = new ArrayList<>(tables.size());
      switch (queryMode) {
        case TABLE:
          log.trace("Starting in TABLE mode");
          for (String table : tables) {
            // Find possible partition maps for different offset protocols
            // We need to search by all offset protocol partition keys to support compatibility
            List<Map<String, String>> tablePartitions = possibleTablePartitions(table);
            partitions.addAll(tablePartitions);
            partitionsByTableFqn.put(table, tablePartitions);
          }
          break;
        case QUERY:
          log.trace("Starting in QUERY mode");
          partitions.add(Collections.singletonMap(JdbcSourceConnectorConstants.QUERY_NAME_KEY,
                                                  JdbcSourceConnectorConstants.QUERY_NAME_VALUE));
          break;
        default:
          throw new ConfigException("Unknown query mode: " + queryMode);
      }
      offsets = context.offsetStorageReader().offsets(partitions);
      log.trace("The partition offsets are {}", offsets);
    }

    // Support both legacy and new mapping configurations
    List<String> timestampColumnMappings = config.timestampColumnMapping();
    List<String> incrementingColumnMappings = config.incrementingColumnMapping();

    // Validate mapping configurations
    if (config.modeUsesTimestampColumn() && !timestampColumnMappings.isEmpty()) {
      // Convert string table names to TableId objects for validation
      List<TableId> tableIds = tables.stream()
              .map(dialect::parseTableIdentifier)
              .collect(java.util.stream.Collectors.toList());

      TableCollectionUtils.validateEachTableMatchesExactlyOneRegex(
          config.timestampColMappingRegexes(), tableIds, TableId::toUnquotedString,
          problem -> {
            throw new ConnectException(
                "Error while validating timestamp column mappings: " + problem);
          }
      );
      populateTableToTsColsMap();
    }
    if (config.modeUsesIncrementingColumn() && !incrementingColumnMappings.isEmpty()) {
      // Convert string table names to TableId objects for validation
      List<TableId> tableIds = tables.stream()
              .map(dialect::parseTableIdentifier)
              .collect(java.util.stream.Collectors.toList());

      TableCollectionUtils.validateEachTableMatchesExactlyOneRegex(
          config.incrementingColMappingRegexes(), tableIds, TableId::toUnquotedString,
          problem -> {
            throw new ConnectException(
                "Error while validating incrementing column mappings: " + problem);
          }
      );
      populateTableToIncrementingColMap();
    }

    Long timestampDelayInterval
        = config.getLong(JdbcSourceTaskConfig.TIMESTAMP_DELAY_INTERVAL_MS_CONFIG);
    boolean validateNonNulls
        = config.getBoolean(JdbcSourceTaskConfig.VALIDATE_NON_NULL_CONFIG);
    ZoneId zoneId = config.zoneId();
    String suffix = config.getString(JdbcSourceTaskConfig.QUERY_SUFFIX_CONFIG).trim();

    if (queryMode.equals(TableQuerier.QueryMode.TABLE)) {
      validateColumnsExist(
              mode, getIncrementingColumn(tables.get(0)),
              getTimestampColumns(tables.get(0)), tables.get(0),
              tableType);
    }

    for (String tableOrQuery : tablesOrQuery) {
      final List<Map<String, String>> tablePartitionsToCheck;
      final Map<String, String> partition;
      
      // Get columns for this specific table (either from mapping or legacy config)
      String incrementingColumn = getIncrementingColumn(tableOrQuery);
      List<String> timestampColumns = getTimestampColumns(tableOrQuery);

      log.trace("Task executing in {} mode",queryMode);
      switch (queryMode) {
        case TABLE:
          if (validateNonNulls) {
            validateNonNullable(
                mode,
                tableOrQuery,
                incrementingColumn,
                timestampColumns,
                tableType
            );
          }
          tablePartitionsToCheck = partitionsByTableFqn.get(tableOrQuery);
          break;
        case QUERY:
          partition = Collections.singletonMap(
              JdbcSourceConnectorConstants.QUERY_NAME_KEY,
              JdbcSourceConnectorConstants.QUERY_NAME_VALUE
          );
          tablePartitionsToCheck = Collections.singletonList(partition);
          break;
        default:
          throw new ConfigException("Unexpected query mode: " + queryMode);
      }

      // The partition map varies by offset protocol. Since we don't know which protocol each
      // table's offsets are keyed by, we need to use the different possible partitions
      // (newest protocol version first) to find the actual offsets for each table.
      Map<String, Object> offset = null;
      if (offsets != null) {
        for (Map<String, String> toCheckPartition : tablePartitionsToCheck) {
          offset = offsets.get(toCheckPartition);
          if (offset != null) {
            log.info("Found offset {} for partition {}", offsets, toCheckPartition);
            break;
          }
        }
      }
      offset = computeInitialOffset(tableOrQuery, offset, zoneId);

      String topicPrefix = config.topicPrefix();
      JdbcSourceConnectorConfig.TimestampGranularity timestampGranularity
          = JdbcSourceConnectorConfig.TimestampGranularity.get(config);

      if (mode.equals(JdbcSourceTaskConfig.MODE_BULK)) {
        tableQueue.add(
            new BulkTableQuerier(
                config,
                dialect, 
                queryMode, 
                tableOrQuery, 
                topicPrefix, 
                suffix
            )
        );
      } else if (mode.equals(JdbcSourceTaskConfig.MODE_INCREMENTING)) {
        tableQueue.add(
            new TimestampIncrementingTableQuerier(
                config,
                dialect,
                queryMode,
                tableOrQuery,
                topicPrefix,
                null,
                incrementingColumn,
                offset,
                timestampDelayInterval,
                zoneId,
                suffix,
                timestampGranularity
            )
        );
      } else if (mode.equals(JdbcSourceTaskConfig.MODE_TIMESTAMP)) {
        tableQueue.add(
            new TimestampTableQuerier(
                config,
                dialect,
                queryMode,
                tableOrQuery,
                topicPrefix,
                timestampColumns,
                offset,
                timestampDelayInterval,
                zoneId,
                suffix,
                timestampGranularity
            )
        );
      } else if (mode.endsWith(JdbcSourceTaskConfig.MODE_TIMESTAMP_INCREMENTING)) {
        tableQueue.add(
            new TimestampIncrementingTableQuerier(
                config,
                dialect,
                queryMode,
                tableOrQuery,
                topicPrefix,
                timestampColumns,
                incrementingColumn,
                offset,
                timestampDelayInterval,
                zoneId,
                suffix,
                timestampGranularity
            )
        );
      }
    }

    if (!tableQueue.isEmpty()) {
      startTableQuerierProcessor();
    }
    log.info("Started JDBC source task");
  }

  private void startTableQuerierProcessor() {
    initEngine();
    tableQuerierProcessor = new TableQuerierProcessor(
        config,
        time,
        tableQueue,
        cachedConnectionProvider,
        dialect
    );
    engine.submit("Table Querier Processor", "tableQuerierProcessor",
        tableQuerierProcessor::process);
  }

  private void initEngine() {
    // Create the queue, which by default uses a blocking array queue, batches created from
    // new ArrayList(maxBatchSize), and a caching thread pool executor,
    RecordQueue.Builder<SourceRecord> builder = RecordQueue.<SourceRecord>builder()
        .maxBatchSize(config.maxBatchSize())
        .maxQueueSize(config.maxBufferSize())
        .maxPollLinger(config.pollLingerMs())
        .maxExecutorThreads(1)
        .clock(time);
    engine = builder.build();
  }

  private void validateColumnsExist(
      String mode, 
      String incrementingColumn, 
      List<String> timestampColumns, 
      String table,
      List<String> tableType
  ) {
    try {
      final Connection conn = cachedConnectionProvider.getConnection();
      boolean autoCommit = conn.getAutoCommit();
      try {
        log.info("Validating columns exist for table: {}", table);
        conn.setAutoCommit(true);
        Map<ColumnId, ColumnDefinition> defnsById =
            describeColumnsForTables(
                conn, table, tableType);

        Set<String> columnNames =
            defnsById.keySet().stream()
                .map(ColumnId::name)
                .map(String::toLowerCase)
                .collect(Collectors.toSet());
        if ((mode.equals(JdbcSourceTaskConfig.MODE_INCREMENTING)
            || mode.equals(JdbcSourceTaskConfig.MODE_TIMESTAMP_INCREMENTING))
            && !incrementingColumn.isEmpty()
            && !columnNames.contains(incrementingColumn.toLowerCase(Locale.getDefault()))) {
          throw new ConfigException(
              "Incrementing column: " + incrementingColumn
              + " does not exist in table '" + table + "'"
          );
        }

        if ((mode.equals(JdbcSourceTaskConfig.MODE_TIMESTAMP)
            || mode.equals(JdbcSourceTaskConfig.MODE_TIMESTAMP_INCREMENTING))
            && !timestampColumns.isEmpty()) {
          Set<String> missingTsColumns = timestampColumns.stream()
              .filter(tsColumn -> !columnNames.contains(
                  tsColumn.toLowerCase(Locale.getDefault())
              ))
              .collect(Collectors.toSet());

          if (!missingTsColumns.isEmpty()) {
            throw new ConfigException(
                "Timestamp columns: " + String.join(", ", missingTsColumns)
                + " do not exist in table '" + table + "'"
            );
          }
        }
      } finally {
        conn.setAutoCommit(autoCommit);
      }
    } catch (SQLException e) {
      throw new ConnectException(
          "Failed trying to validate that columns used for offsets exist",
          e
      );
    }
  }

  protected CachedConnectionProvider connectionProvider(int maxConnAttempts, long retryBackoff) {
    return new CachedConnectionProvider(dialect, maxConnAttempts, retryBackoff) {
      @Override
      protected void onConnect(final Connection connection) throws SQLException {
        super.onConnect(connection);
        connection.setAutoCommit(false);
      }
    };
  }

  //This method returns a list of possible partition maps for different offset protocols
  //This helps with the upgrades
  private List<Map<String, String>> possibleTablePartitions(String table) {
    TableId tableId = dialect.parseTableIdentifier(table);
    return Arrays.asList(
        OffsetProtocols.sourcePartitionForProtocolV1(tableId),
        OffsetProtocols.sourcePartitionForProtocolV0(tableId)
    );
  }

  protected Map<String, Object> computeInitialOffset(
          String tableOrQuery,
          Map<String, Object> partitionOffset,
          ZoneId zoneId) {
    Boolean shouldTrimSensitiveLogs =
        config.getBoolean(JdbcSourceConnectorConfig.TRIM_SENSITIVE_LOG_ENABLED);
    if (shouldTrimSensitiveLogs) {
      tableOrQuery = LogUtil.sensitiveLog(true, tableOrQuery);
    }
    if (!(partitionOffset == null)) {
      log.info("Partition offset for '{}' is not null. Using existing offset.", tableOrQuery);
      return partitionOffset;
    } else {
      log.info("Partition offset for '{}' is null. Computing initial offset.", tableOrQuery);
      Map<String, Object> initialPartitionOffset = null;
      // no offsets found
      Long timestampInitial = config.getLong(JdbcSourceConnectorConfig.TIMESTAMP_INITIAL_CONFIG);
      if (timestampInitial != null) {
        // start at the specified timestamp
        if (timestampInitial == JdbcSourceConnectorConfig.TIMESTAMP_INITIAL_CURRENT) {
          // use the current time
          try {
            final Connection con = cachedConnectionProvider.getConnection();
            Calendar cal = Calendar.getInstance(TimeZone.getTimeZone(zoneId));
            timestampInitial = dialect.currentTimeOnDB(con, cal).getTime();
          } catch (SQLException e) {
            throw new ConnectException("Error while getting initial timestamp from database", e);
          }
        }
        initialPartitionOffset = new HashMap<String, Object>();
        initialPartitionOffset.put(TimestampIncrementingOffset.TIMESTAMP_FIELD, timestampInitial);
        log.info("No offsets found for '{}', so using configured timestamp {}", tableOrQuery,
                timestampInitial);
      }
      return initialPartitionOffset;
    }
  }

  @Override
  public void stop() throws ConnectException {
    log.info("Stopping JDBC source task");

    if (engine != null) {
      try {
        engine.stop();
        try {
          boolean gracefulStop = engine.awaitTermination(Duration.ofSeconds(
              config.getInt(JdbcSourceTaskConfig.ENGINE_SHUTDOWN_TIMEOUT)));
          if (!gracefulStop) {
            log.warn("Could not shut down the engine gracefully in time.");
          }
        } catch (InterruptedException e) {
          Thread.interrupted();
          log.debug("Interrupted while waiting for the task's queue to shut down");
        }
      } finally {
        engine = null;
      }
    }

    if (tableQuerierProcessor != null) {
      tableQuerierProcessor.shutdown();
    }

    closeResources();
  }

  protected void closeResources() {
    log.info("Closing resources for JDBC source task");
    try {
      if (cachedConnectionProvider != null) {
        cachedConnectionProvider.close(true);
      }
    } catch (Throwable t) {
      log.warn("Error while closing the connections", t);
    } finally {
      cachedConnectionProvider = null;
      try {
        if (dialect != null) {
          dialect.close();
        }
      } catch (Throwable t) {
        log.warn("Error while closing the {} dialect: ", dialect.name(), t);
      } finally {
        dialect = null;
      }
    }
  }

  @Override
  public List<SourceRecord> poll() throws InterruptedException {
    if (engine == null) {
      // Engine is not initialized yet, so nothing to poll
      return null;
    }
    // Get the next batch from the queue
    List<SourceRecord> results = engine.poll();
    if (log.isTraceEnabled()) {
      if (results != null && !results.isEmpty()) {
        SourceRecord lastRecord = results.get(results.size() - 1);
        Map<String, ?> lastOffset = lastRecord.sourceOffset();
        if (lastOffset != null) {
          log.trace("Offset of last polled record: {}", lastOffset.entrySet().stream()
              .map(k ->
                  k.getKey().toString() + " : " + k.getValue().toString()
              ).collect(Collectors.joining(", ")));
        }
      }
    }
    return results;
  }


  private Map<ColumnId, ColumnDefinition> describeColumnsForTables(
      Connection conn, String table, List<String> tableType) throws SQLException {
    Map<ColumnId, ColumnDefinition> defnsById = dialect.describeColumns(conn, table, null);

    if (tableType.stream().anyMatch("SYNONYM"::equalsIgnoreCase) && defnsById.isEmpty()) {
      String actualTable = dialect.resolveSynonym(conn, table);
      log.debug("Resolved synonym {} to base table {}", table, actualTable);
      if (actualTable == null) {
        throw new ConfigException("Could not resolve base table for synonym: " + table);
      }
      defnsById = dialect.describeColumns(conn, actualTable, null);
    }

    return defnsById;
  }

  private void validateNonNullable(
      String incrementalMode,
      String table,
      String incrementingColumn,
      List<String> timestampColumns,
      List<String> tableType
  ) {
    log.info("Validating non-nullable fields for table: {}", table);
    try {
      Set<String> lowercaseTsColumns = new HashSet<>();
      for (String timestampColumn: timestampColumns) {
        lowercaseTsColumns.add(timestampColumn.toLowerCase(Locale.getDefault()));
      }

      boolean incrementingOptional = false;
      boolean atLeastOneTimestampNotOptional = false;
      final Connection conn = cachedConnectionProvider.getConnection();
      boolean autoCommit = conn.getAutoCommit();
      try {
        conn.setAutoCommit(true);
        Map<ColumnId, ColumnDefinition> defnsById = describeColumnsForTables(
            conn,
            table,
            tableType
        );
        for (ColumnDefinition defn : defnsById.values()) {
          String columnName = defn.id().name();
          if (columnName.equalsIgnoreCase(incrementingColumn)) {
            incrementingOptional = defn.isOptional();
          } else if (lowercaseTsColumns.contains(columnName.toLowerCase(Locale.getDefault()))) {
            if (!defn.isOptional()) {
              atLeastOneTimestampNotOptional = true;
            }
          }
        }
      } finally {
        conn.setAutoCommit(autoCommit);
      }

      // Validate that requested columns for offsets are NOT NULL. Currently this is only performed
      // for table-based copying because custom query mode doesn't allow this to be looked up
      // without a query or parsing the query since we don't have a table name.
      if ((incrementalMode.equals(JdbcSourceConnectorConfig.MODE_INCREMENTING)
           || incrementalMode.equals(JdbcSourceConnectorConfig.MODE_TIMESTAMP_INCREMENTING))
          && incrementingOptional) {
        throw new ConnectException("Cannot make incremental queries using incrementing column "
                                   + incrementingColumn + " on " + table + " because this column "
                                   + "is nullable.");
      }
      if ((incrementalMode.equals(JdbcSourceConnectorConfig.MODE_TIMESTAMP)
           || incrementalMode.equals(JdbcSourceConnectorConfig.MODE_TIMESTAMP_INCREMENTING))
          && !atLeastOneTimestampNotOptional) {
        throw new ConnectException("Cannot make incremental queries using timestamp columns "
                                   + String.join(",", timestampColumns) + " on " + table
                                   + " because all of these columns are nullable.");
      }
    } catch (SQLException e) {
      throw new ConnectException("Failed trying to validate that columns used for offsets are NOT"
                                 + " NULL", e);
    }
  }

  void populateTableToIncrementingColMap() {
    List<String> tables = config.getList(JdbcSourceTaskConfig.TABLES_CONFIG);
    List<String> tableRegexToIncrCols = config.incrementingColumnMapping();
    
    for (String table : tables) {
      // Convert table string to TableId to get unquoted string for regex matching
      TableId tableId = dialect.parseTableIdentifier(table);
      String unquotedTableString = tableId.toUnquotedString();
      
      for (String tableRegexToIncrCol : tableRegexToIncrCols) {
        String[] tableAndIncrCol = tableRegexToIncrCol.split(":", 2);
        if (unquotedTableString.matches(tableAndIncrCol[0].trim())) {
          tableToIncrCol.put(table, tableAndIncrCol[1].trim());
          break;
        }
      }
    }
  }

  void populateTableToTsColsMap() {
    List<String> tables = config.getList(JdbcSourceTaskConfig.TABLES_CONFIG);
    List<String> tableRegexToTsCols = config.timestampColumnMapping();
    
    // If there are no timestamp mappings, nothing to populate
    if (tableRegexToTsCols.isEmpty()) {
      return;
    }
    
    for (String table : tables) {
      // Convert table string to TableId to get unquoted string for regex matching
      TableId tableId = dialect.parseTableIdentifier(table);
      String unquotedTableString = tableId.toUnquotedString();
      
      for (String tableRegexToTsCol : tableRegexToTsCols) {
        String[] tableAndTsCols = tableRegexToTsCol.split(":", 2);
        if (unquotedTableString.matches(tableAndTsCols[0].trim())) {
          String columnsString = tableAndTsCols[1].trim();
          // Remove brackets and split by pipe
          String columnsContent = columnsString.substring(1, columnsString.length() - 1);
          tableToTsCols.put(table, Arrays.asList(columnsContent.split("\\|")));
          break;
        }
      }
    }
  }

  private String getIncrementingColumn(String table) {
    if (!config.tableIncludeListRegexes().isEmpty()) {
      return tableToIncrCol.get(table) != null ? tableToIncrCol.get(table) : "";
    }
    return config.getString(JdbcSourceTaskConfig.INCREMENTING_COLUMN_NAME_CONFIG);
  }

  private List<String> getTimestampColumns(String table) {
    if (!config.tableIncludeListRegexes().isEmpty()) {
      return tableToTsCols.get(table) != null ? tableToTsCols.get(table) : Collections.emptyList();
    }
    return config.getList(JdbcSourceTaskConfig.TIMESTAMP_COLUMN_NAME_CONFIG);
  }

}
