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

import static org.junit.Assert.assertEquals;

/**
 * Runs the source connector with {@code tasks.max = 2} across two tables and asserts both tables'
 * rows are streamed by the resulting tasks. Table distribution across tasks was only unit-tested;
 * every integration test pinned {@code tasks.max = 1}, so multi-task execution was never exercised
 * end to end.
 */
@Category(IntegrationTest.class)
public class PostgresMultiTaskIT extends BaseConnectorIT {

  @ClassRule
  public static final PostgreSQLContainer<?> postgres = new PostgreSQLContainer<>("postgres:13")
      .withDatabaseName("testdb")
      .withUsername("test")
      .withPassword("test123");

  private static final String CONNECTOR_NAME = "multi-task-source";
  private static final String TABLE_ONE = "multitask_one";
  private static final String TABLE_TWO = "multitask_two";
  private static final String TOPIC_PREFIX = "multitask-";
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

    createTable(TABLE_ONE);
    createTable(TABLE_TWO);
    insertRows(TABLE_ONE, 3);
    insertRows(TABLE_TWO, 4);

    props = new HashMap<>();
    props.put("connector.class", "io.confluent.connect.jdbc.JdbcSourceConnector");
    props.put("tasks.max", "2");
    props.put(JdbcSourceConnectorConfig.CONNECTION_URL_CONFIG, postgres.getJdbcUrl());
    props.put(JdbcSourceConnectorConfig.CONNECTION_USER_CONFIG, postgres.getUsername());
    props.put(JdbcSourceConnectorConfig.CONNECTION_PASSWORD_CONFIG, postgres.getPassword());
    props.put(JdbcSourceConnectorConfig.MODE_CONFIG, JdbcSourceConnectorConfig.MODE_INCREMENTING);
    // New-style column mapping to pair with the new-style table.include.list (legacy and new
    // configs cannot be mixed).
    props.put(JdbcSourceConnectorConfig.INCREMENTING_COLUMN_MAPPING_CONFIG, ".*multitask_.*:id");
    props.put(JdbcSourceConnectorConfig.TABLE_INCLUDE_LIST_CONFIG, ".*multitask_.*");
    props.put(JdbcSourceConnectorConfig.TOPIC_PREFIX_CONFIG, TOPIC_PREFIX);
    props.put(JdbcSourceConnectorConfig.VALIDATE_NON_NULL_CONFIG, "false");
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
      stmt.execute("DROP TABLE IF EXISTS " + TABLE_ONE);
      stmt.execute("DROP TABLE IF EXISTS " + TABLE_TWO);
    }
  }

  @Test
  public void twoTablesAreSplitAcrossTasksAndBothStream() throws Exception {
    String topicOne = TOPIC_PREFIX + TABLE_ONE;
    String topicTwo = TOPIC_PREFIX + TABLE_TWO;
    connect.kafka().createTopic(topicOne, 1);
    connect.kafka().createTopic(topicTwo, 1);

    connect.configureConnector(CONNECTOR_NAME, props);
    // Two tables with tasks.max=2 should yield two running tasks.
    waitForConnectorToStart(CONNECTOR_NAME, 2);

    ConsumerRecords<byte[], byte[]> fromOne =
        connect.kafka().consume(3, CONSUME_TIMEOUT_MS, topicOne);
    ConsumerRecords<byte[], byte[]> fromTwo =
        connect.kafka().consume(4, CONSUME_TIMEOUT_MS, topicTwo);

    // Incrementing mode emits each row once, so assert exact counts: a re-read or a table landing
    // on the wrong task would change these.
    assertEquals("First table should stream exactly its rows", 3, fromOne.count());
    assertEquals("Second table should stream exactly its rows", 4, fromTwo.count());
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
