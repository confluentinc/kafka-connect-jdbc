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
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.testcontainers.containers.PostgreSQLContainer;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertTrue;

/**
 * Verifies the {@link io.confluent.connect.jdbc.source.TableMonitorThread} lifecycle: a table that
 * matches the include list and is created while the connector is running is discovered on the next
 * poll and its rows are streamed. No integration test covered runtime table discovery or the
 * reconfiguration it triggers before this.
 *
 * <p>Only the add case is asserted. Dropping a table mid-run is intentionally left out: it races
 * with in-flight queries against that table and can produce a transient task error, which would
 * make the assertion flaky.
 */
@Category(IntegrationTest.class)
public class PostgresTableLifecycleIT extends BaseConnectorIT {

  @ClassRule
  public static final PostgreSQLContainer<?> postgres = new PostgreSQLContainer<>("postgres:13")
      .withDatabaseName("testdb")
      .withUsername("test")
      .withPassword("test123");

  private static final String CONNECTOR_NAME = "table-lifecycle-source";
  private static final String TABLE_A = "test_a";
  private static final String TABLE_B = "test_b";
  private static final String TOPIC_PREFIX = "lifecycle-";
  private static final long POLL_INTERVAL_MS = TimeUnit.SECONDS.toMillis(1);
  private static final long CONSUME_TIMEOUT_MS = TimeUnit.SECONDS.toMillis(60);

  private static Connection connection;

  private Map<String, String> props;

  @BeforeClass
  public static void setupClass() throws SQLException {
    connection = DriverManager.getConnection(
        postgres.getJdbcUrl(), postgres.getUsername(), postgres.getPassword());
  }

  @AfterClass
  public static void teardownClass() throws SQLException {
    if (connection != null && !connection.isClosed()) {
      connection.close();
    }
  }

  @Before
  public void setup() throws SQLException {
    startConnect();
    createTable(TABLE_A);
    insertRows(TABLE_A, 2);

    props = new HashMap<>();
    props.put("connector.class", "io.confluent.connect.jdbc.JdbcSourceConnector");
    props.put("tasks.max", "1");
    props.put(JdbcSourceConnectorConfig.CONNECTION_URL_CONFIG, postgres.getJdbcUrl());
    props.put(JdbcSourceConnectorConfig.CONNECTION_USER_CONFIG, postgres.getUsername());
    props.put(JdbcSourceConnectorConfig.CONNECTION_PASSWORD_CONFIG, postgres.getPassword());
    props.put(JdbcSourceConnectorConfig.MODE_CONFIG, JdbcSourceConnectorConfig.MODE_INCREMENTING);
    props.put(JdbcSourceConnectorConfig.INCREMENTING_COLUMN_NAME_CONFIG, "id");
    props.put(JdbcSourceConnectorConfig.TABLE_INCLUDE_LIST_CONFIG, ".*test_.*");
    props.put(JdbcSourceConnectorConfig.TOPIC_PREFIX_CONFIG, TOPIC_PREFIX);
    props.put(JdbcSourceConnectorConfig.VALIDATE_NON_NULL_CONFIG, "false");
    props.put(JdbcSourceConnectorConfig.TABLE_POLL_INTERVAL_MS_CONFIG,
        String.valueOf(POLL_INTERVAL_MS));
    props.put(JdbcSourceConnectorConfig.POLL_INTERVAL_MS_CONFIG, String.valueOf(POLL_INTERVAL_MS));
    props.put(JdbcSourceConnectorConfig.POLL_LINGER_MS_CONFIG, "0");
    props.put("value.converter", "org.apache.kafka.connect.json.JsonConverter");
    props.put("value.converter.schemas.enable", "false");
    props.put("key.converter", "org.apache.kafka.connect.storage.StringConverter");
  }

  @After
  public void tearDown() throws SQLException {
    stopConnect();
    try (Statement stmt = connection.createStatement()) {
      stmt.execute("DROP TABLE IF EXISTS " + TABLE_A);
      stmt.execute("DROP TABLE IF EXISTS " + TABLE_B);
    }
  }

  @Test
  public void tableCreatedMidRunIsDiscoveredAndStreamed() throws Exception {
    String topicA = TOPIC_PREFIX + TABLE_A;
    String topicB = TOPIC_PREFIX + TABLE_B;
    connect.kafka().createTopic(topicA, 1);
    connect.kafka().createTopic(topicB, 1);

    connect.configureConnector(CONNECTOR_NAME, props);
    waitForConnectorToStart(CONNECTOR_NAME, 1);

    // The pre-existing table streams immediately.
    ConsumerRecords<byte[], byte[]> fromA = connect.kafka().consume(2, CONSUME_TIMEOUT_MS, topicA);
    assertTrue("Pre-existing table should stream its rows", fromA.count() >= 2);

    // Create a matching table after the connector is already running.
    createTable(TABLE_B);
    insertRows(TABLE_B, 3);

    // The monitor thread should discover it within the poll interval, trigger reconfiguration,
    // and its rows should arrive.
    ConsumerRecords<byte[], byte[]> fromB = connect.kafka().consume(3, CONSUME_TIMEOUT_MS, topicB);
    assertTrue("Table created mid-run should be discovered and streamed", fromB.count() >= 3);
  }

  private void createTable(String tableName) throws SQLException {
    try (Statement stmt = connection.createStatement()) {
      stmt.execute("CREATE TABLE " + tableName + " ("
          + "id SERIAL PRIMARY KEY, name VARCHAR(100))");
    }
  }

  private void insertRows(String tableName, int count) throws SQLException {
    try (Statement stmt = connection.createStatement()) {
      for (int i = 0; i < count; i++) {
        stmt.execute("INSERT INTO " + tableName + " (name) VALUES ('name_" + i + "')");
      }
    }
  }
}
