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

package io.confluent.connect.jdbc.integration;

import io.confluent.common.utils.IntegrationTest;
import io.confluent.connect.jdbc.source.JdbcSourceConnectorConfig;
import io.confluent.connect.jdbc.source.JdbcSourceTaskConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.connect.runtime.ConnectorConfig;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Abstract base class for JDBC connector integration tests.
 * Provides common test logic for all 4 modes:
 * 1. Bulk mode - fetches all data on each poll
 * 2. Incrementing mode - fetches data based on incrementing column
 * 3. Timestamp mode - fetches data based on timestamp column
 * 4. Timestamp+Incrementing mode - uses both timestamp and incrementing columns
 */
@Category(IntegrationTest.class)
public abstract class AbstractJdbcSourceConnectorIT extends BaseConnectorIT {

  private static final Logger log = LoggerFactory.getLogger(AbstractJdbcSourceConnectorIT.class);

  protected static final long POLLING_INTERVAL_MS = TimeUnit.SECONDS.toMillis(2);
  protected static final long CONSUME_MAX_DURATION_MS = TimeUnit.SECONDS.toMillis(30);

  // Test table names
  protected static final String TEST_TABLE_NAME = "test_table";
  protected static final String ID_COLUMN_NAME = "id";
  protected static final String TIMESTAMP_COLUMN_NAME = "updated_at";

  protected Map<String, String> props;
  protected static Connection connection;

  @Before
  public void setup() throws SQLException {
    startConnect();
    
    // Base connector properties
    props = new HashMap<>();
    props.put(ConnectorConfig.CONNECTOR_CLASS_CONFIG, getSourceConnectorClass().getName());
    props.put(ConnectorConfig.NAME_CONFIG, getConnectorName());
    props.put(ConnectorConfig.TASKS_MAX_CONFIG, "1");
    props.put(JdbcSourceConnectorConfig.CONNECTION_URL_CONFIG, getDatabaseConfig().getJdbcUrl());
    props.put(JdbcSourceConnectorConfig.CONNECTION_USER_CONFIG, getDatabaseConfig().getUsername());
    props.put(JdbcSourceConnectorConfig.CONNECTION_PASSWORD_CONFIG, getDatabaseConfig().getPassword());
    props.put(JdbcSourceConnectorConfig.POLL_INTERVAL_MS_CONFIG, String.valueOf(POLLING_INTERVAL_MS));
    // task thread is stuck while lingering, hence graceful stop fails in some of the tests. It's
    // better to not linger.
    props.put(JdbcSourceConnectorConfig.POLL_LINGER_MS_CONFIG, String.valueOf(0));
    props.put(JdbcSourceTaskConfig.TOPIC_PREFIX_CONFIG, getTopicPrefix());
    props.put(JdbcSourceConnectorConfig.BATCH_MAX_ROWS_CONFIG, "100");
    props.put(JdbcSourceConnectorConfig.TABLE_INCLUDE_LIST_CONFIG, ".*" + (needsUpperCaseIdentifiers() ? TEST_TABLE_NAME.toUpperCase() : TEST_TABLE_NAME));
    props.put(JdbcSourceConnectorConfig.VALIDATE_NON_NULL_CONFIG, "false");
    props.put(JdbcSourceConnectorConfig.DIALECT_NAME_CONFIG, getDatabaseConfig().getDialectName());
  }

  @After
  public void tearDown() throws SQLException {
    stopConnect();
    if (connection != null) {
      // Drop all test tables to ensure clean state for next test
      try (Statement stmt = connection.createStatement()) {
        stmt.execute("DROP TABLE " + TEST_TABLE_NAME);
      } catch (SQLException e) {
        log.error("Error dropping tables", e);
      }
      // Don't close connection here - it's shared across tests
    }
  }

  @Test
  public void testBulkMode() throws Exception {
    // Create table with some initial data
    String tableName = needsUpperCaseIdentifiers() ? TEST_TABLE_NAME.toUpperCase() : TEST_TABLE_NAME;
    createTable(tableName);
    insertData(tableName, 5);

    // Configure connector for bulk mode
    props.put(JdbcSourceConnectorConfig.MODE_CONFIG, JdbcSourceConnectorConfig.MODE_BULK);

    String topic = getTopicPrefix() + tableName;
    connect.kafka().createTopic(topic, 1);

    // Start connector
    connect.configureConnector(getConnectorName(), props);
    waitForConnectorToStart(getConnectorName(), 1);

    // Wait for initial poll
    Thread.sleep(POLLING_INTERVAL_MS * 2);

    // Consume records - should get all 5 records
    ConsumerRecords<byte[], byte[]> records = consumeRecords(topic, 5);
    // assert than number of records is more than or equal to 5 as in bulk mode, all records are fetched on each poll
    assertTrue("Should fetch at least 5 existing records", records.count() >= 5);
  }

  @Test
  public void testIncrementingMode() throws Exception {
    // Create table with auto-increment primary key
    String tableName = needsUpperCaseIdentifiers() ? TEST_TABLE_NAME.toUpperCase() : TEST_TABLE_NAME;
    createTable(tableName);

    // Insert initial data before starting connector
    insertData(tableName, 3);

    // Configure connector for incrementing mode
    String normalizedTableName = needsUpperCaseIdentifiers() ? TEST_TABLE_NAME.toUpperCase() : TEST_TABLE_NAME;
    String normalizedIdColumn = needsUpperCaseIdentifiers() ? ID_COLUMN_NAME.toUpperCase() : ID_COLUMN_NAME;
    props.put(JdbcSourceConnectorConfig.MODE_CONFIG, JdbcSourceConnectorConfig.MODE_INCREMENTING);
    props.put(JdbcSourceConnectorConfig.INCREMENTING_COLUMN_MAPPING_CONFIG, normalizedTableName + ".*:" + normalizedIdColumn);

    String topic = getTopicPrefix() + tableName;
    connect.kafka().createTopic(topic, 1);

    // Start connector
    connect.configureConnector(getConnectorName(), props);
    waitForConnectorToStart(getConnectorName(), 1);

    // Wait for initial poll
    Thread.sleep(POLLING_INTERVAL_MS * 2);

    // Should get the 3 existing records
    ConsumerRecords<byte[], byte[]> records = consumeRecords(topic, 3);
    assertEquals("Should fetch 3 existing records", 3, records.count());

    // Insert new records after connector started
    insertDataWithStartId(tableName, 2, 3);

    // Wait for next poll
    Thread.sleep(POLLING_INTERVAL_MS * 2);

    // Should get 5 records now (3 old + 2 new)
    records = consumeRecords(topic, 5);
    assertEquals("Should fetch 5 existing records (3 old + 2 new)", 5, records.count());
  }

  @Test
  public void testTimestampMode() throws Exception {
    // Create table with timestamp column
    String tableName = needsUpperCaseIdentifiers() ? TEST_TABLE_NAME.toUpperCase() : TEST_TABLE_NAME;
    createTable(tableName);

    // Insert initial data with timestamps
    insertData(tableName, 3);

    // Configure connector for timestamp mode
    String normalizedTableName = needsUpperCaseIdentifiers() ? TEST_TABLE_NAME.toUpperCase() : TEST_TABLE_NAME;
    String normalizedTimestampColumn = needsUpperCaseIdentifiers() ? TIMESTAMP_COLUMN_NAME.toUpperCase() : TIMESTAMP_COLUMN_NAME;
    props.put(JdbcSourceConnectorConfig.MODE_CONFIG, JdbcSourceConnectorConfig.MODE_TIMESTAMP);
    props.put(JdbcSourceConnectorConfig.TIMESTAMP_COLUMN_MAPPING_CONFIG, ".*" + normalizedTableName + ":" + "["+normalizedTimestampColumn+"]");

    String topic = getTopicPrefix() + tableName;
    connect.kafka().createTopic(topic, 1);

    // Start connector
    connect.configureConnector(getConnectorName(), props);
    waitForConnectorToStart(getConnectorName(), 1);

    // Wait for initial poll
    Thread.sleep(POLLING_INTERVAL_MS * 2);

    // Should get the 3 existing records
    ConsumerRecords<byte[], byte[]> records = consumeRecords(topic, 3);
    assertEquals("Should fetch 3 existing records", 3, records.count());

    // Insert new records with newer timestamps
    Thread.sleep(1000); // Ensure newer timestamp
    insertDataWithStartId(tableName, 2, 3);

    // Wait for next poll
    Thread.sleep(POLLING_INTERVAL_MS * 2);

    // Should get the 2 new records based on timestamp
    records = consumeRecords(topic, 5);
    assertEquals("Should fetch 5 existing records (3 old + 2 new)", 5, records.count());

    // Update an existing record
    updateRecordTimestamp(tableName, 2);

    // Wait for next poll
    Thread.sleep(POLLING_INTERVAL_MS * 2);

    // Should get the updated record
    records = consumeRecords(topic, 6);
    assertEquals("Should fetch 6 existing records (5 old + 1 updated)", 6, records.count());
  }

  @Test
  public void testTimestampIncrementingMode() throws Exception {
    // Create table with both timestamp and incrementing columns
    String tableName = needsUpperCaseIdentifiers() ? TEST_TABLE_NAME.toUpperCase() : TEST_TABLE_NAME;
    createTable(tableName);

    // Insert initial data
    insertData(tableName, 3);

    // Configure connector for timestamp+incrementing mode
    String normalizedTableName = needsUpperCaseIdentifiers() ? TEST_TABLE_NAME.toUpperCase() : TEST_TABLE_NAME;
    String normalizedTimestampColumn = needsUpperCaseIdentifiers() ? TIMESTAMP_COLUMN_NAME.toUpperCase() : TIMESTAMP_COLUMN_NAME;
    String normalizedIdColumn = needsUpperCaseIdentifiers() ? ID_COLUMN_NAME.toUpperCase() : ID_COLUMN_NAME;
    props.put(JdbcSourceConnectorConfig.MODE_CONFIG,
        JdbcSourceConnectorConfig.MODE_TIMESTAMP_INCREMENTING);
    props.put(JdbcSourceConnectorConfig.TIMESTAMP_COLUMN_MAPPING_CONFIG, ".*" + normalizedTableName + ":" + "["+normalizedTimestampColumn+"]");
    props.put(JdbcSourceConnectorConfig.INCREMENTING_COLUMN_MAPPING_CONFIG, ".*" + normalizedTableName + ":" + normalizedIdColumn);

    String topic = getTopicPrefix() + tableName;
    connect.kafka().createTopic(topic, 1);

    // Start connector
    connect.configureConnector(getConnectorName(), props);
    waitForConnectorToStart(getConnectorName(), 1);

    // Wait for initial poll
    Thread.sleep(POLLING_INTERVAL_MS * 2);

    // Should get the 3 existing records
    ConsumerRecords<byte[], byte[]> records = consumeRecords(topic, 3);
    assertEquals("Should fetch 3 existing records", 3, records.count());

    // Insert new records
    Thread.sleep(1000); // Ensure newer timestamp
    insertDataWithStartId(tableName, 2, 3);

    // Wait for next poll
    Thread.sleep(POLLING_INTERVAL_MS * 2);

    // Should get new records based on both timestamp and incrementing
    records = consumeRecords(topic, 5);
    assertEquals("Should fetch 5 existing records (3 old + 2 new)", 5, records.count());

    // Update an existing record with same timestamp but ensure it's captured
    updateRecordTimestamp(tableName, 1);

    // Wait for next poll
    Thread.sleep(POLLING_INTERVAL_MS * 2);

    // Should get the updated record
    records = consumeRecords(topic, 6);
    assertEquals("Should fetch 6 existing records (5 old + 1 updated)", 6, records.count());
  }

  // Abstract methods to be implemented by concrete test classes

  /**
   * Get the database configuration for this connector
   */
  protected abstract DatabaseTestConfig getDatabaseConfig();

  /**
   * Get the source connector class for this database
   */
  protected abstract Class<?> getSourceConnectorClass();

  /**
   * Get the connector name for this test
   */
  protected abstract String getConnectorName();

  protected abstract boolean needsUpperCaseIdentifiers();

  /**
   * Get the topic prefix for this test
   */
  protected abstract String getTopicPrefix();

  /**
   * Create a standard test table with columns: id, name, value, updated_at.
   * The id should be auto-incrementing, and updated_at should default to current timestamp.
   */
  protected abstract void createTable(String tableName) throws SQLException;

  protected void insertData(String tableName, int count) throws SQLException {
    try (Statement stmt = connection.createStatement()) {
      for (int i = 1; i <= count; i++) {
        stmt.execute(String.format(
            "INSERT INTO %s (name, value) VALUES ('name_%d', 'value_%d')",
            tableName, i, i));
      }
    }
  }

  protected void insertDataWithStartId(String tableName, int count, int startId) throws SQLException {
    try (Statement stmt = connection.createStatement()) {
      for (int i = 1; i <= count; i++) {
        int id = startId + i;
        stmt.execute(String.format(
            "INSERT INTO %s (name, value) VALUES ('name_%d', 'value_%d')",
            tableName, id, id));
      }
    }
  }

  protected void updateRecordTimestamp(String tableName, int id) throws SQLException {
    try (Statement stmt = connection.createStatement()) {
      stmt.execute(String.format(
          "UPDATE %s SET name = 'name_%d_updated', updated_at = NOW() WHERE id = %d",
          tableName, id, id));
    }
  }

  // Helper methods for consuming and validating records

  protected ConsumerRecords<byte[], byte[]> consumeRecords(String topic, int expectedRecords)
      throws Exception {
    // Consume the expected number of records
    return connect.kafka().consume(expectedRecords, CONSUME_MAX_DURATION_MS, topic);
  }

  /**
   * Configuration class for database-specific test settings
   */
  public static class DatabaseTestConfig {
    private final String jdbcUrl;
    private final String username;
    private final String password;
    private final String dialectName;

    public DatabaseTestConfig(String jdbcUrl, String username, String password, String dialectName) {
      this.jdbcUrl = jdbcUrl;
      this.username = username;
      this.password = password;
      this.dialectName = dialectName;
    }

    public String getJdbcUrl() {
      return jdbcUrl;
    }

    public String getUsername() {
      return username;
    }

    public String getPassword() {
      return password;
    }

    public String getDialectName() {
      return dialectName;
    }
  }
}
