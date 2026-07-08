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
import org.apache.kafka.clients.consumer.ConsumerRecord;
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

import java.nio.charset.StandardCharsets;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Runs the source connector in query mode end to end. Query mode had no functional integration
 * coverage; only task-config-level use existed. In query mode the topic is exactly the configured
 * {@code topic.prefix} (see {@code BulkTableQuerier}), with no table suffix.
 */
@Category(IntegrationTest.class)
public class PostgresQueryModeIT extends BaseConnectorIT {

  @ClassRule
  public static final PostgreSQLContainer<?> postgres = new PostgreSQLContainer<>("postgres:13")
      .withDatabaseName("testdb")
      .withUsername("test")
      .withPassword("test123");

  private static final String CONNECTOR_NAME = "query-mode-source";
  private static final String TABLE_NAME = "query_src";
  private static final String TOPIC = "querymodetopic";
  private static final long POLL_INTERVAL_MS = TimeUnit.SECONDS.toMillis(1);
  private static final long CONSUME_TIMEOUT_MS = TimeUnit.SECONDS.toMillis(30);

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

    try (Statement stmt = connection.createStatement()) {
      stmt.execute("CREATE TABLE " + TABLE_NAME + " ("
          + "id SERIAL PRIMARY KEY, name VARCHAR(100))");
      stmt.execute("INSERT INTO " + TABLE_NAME + " (name) VALUES ('a'), ('b'), ('c')");
    }

    props = new HashMap<>();
    props.put("connector.class", "io.confluent.connect.jdbc.JdbcSourceConnector");
    props.put("tasks.max", "1");
    props.put(JdbcSourceConnectorConfig.CONNECTION_URL_CONFIG, postgres.getJdbcUrl());
    props.put(JdbcSourceConnectorConfig.CONNECTION_USER_CONFIG, postgres.getUsername());
    props.put(JdbcSourceConnectorConfig.CONNECTION_PASSWORD_CONFIG, postgres.getPassword());
    props.put(JdbcSourceConnectorConfig.MODE_CONFIG, JdbcSourceConnectorConfig.MODE_BULK);
    props.put(JdbcSourceConnectorConfig.QUERY_CONFIG, "SELECT id, name FROM " + TABLE_NAME);
    props.put(JdbcSourceConnectorConfig.TOPIC_PREFIX_CONFIG, TOPIC);
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
      stmt.execute("DROP TABLE IF EXISTS " + TABLE_NAME);
    }
  }

  @Test
  public void queryModeStreamsRows() throws Exception {
    connect.kafka().createTopic(TOPIC, 1);
    connect.configureConnector(CONNECTOR_NAME, props);
    waitForConnectorToStart(CONNECTOR_NAME, 1);

    ConsumerRecords<byte[], byte[]> records = connect.kafka().consume(3, CONSUME_TIMEOUT_MS, TOPIC);
    assertTrue("Query mode should stream the query's rows", records.count() >= 3);
  }

  @Test
  public void queryWithSuffixFiltersRows() throws Exception {
    // query.suffix is appended verbatim after the query. Use a WHERE suffix that actually changes
    // the result set (id 3 excluded) so the assertion detects a silently-ignored suffix; an
    // ORDER BY suffix would not, since it leaves the row set unchanged.
    props.put(JdbcSourceConnectorConfig.QUERY_SUFFIX_CONFIG, "WHERE id <= 2");
    connect.kafka().createTopic(TOPIC, 1);
    connect.configureConnector(CONNECTOR_NAME, props);
    waitForConnectorToStart(CONNECTOR_NAME, 1);

    // Bulk+query re-emits the result set every poll, so read a couple of polls' worth and assert
    // the excluded row never appears while the included rows do.
    ConsumerRecords<byte[], byte[]> records = connect.kafka().consume(4, CONSUME_TIMEOUT_MS, TOPIC);
    boolean sawIncluded = false;
    boolean sawExcluded = false;
    for (ConsumerRecord<byte[], byte[]> record : records.records(TOPIC)) {
      String value = new String(record.value(), StandardCharsets.UTF_8);
      if (value.contains("\"id\":1") || value.contains("\"id\":2")) {
        sawIncluded = true;
      }
      if (value.contains("\"id\":3")) {
        sawExcluded = true;
      }
    }
    assertTrue("query.suffix WHERE should still stream the included rows", sawIncluded);
    assertFalse("query.suffix 'WHERE id <= 2' must exclude id 3; an ignored suffix would leak it",
        sawExcluded);
  }
}
