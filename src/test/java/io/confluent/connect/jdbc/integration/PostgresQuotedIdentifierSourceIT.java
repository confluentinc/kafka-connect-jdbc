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
import io.confluent.connect.jdbc.JdbcSourceConnector;
import io.confluent.connect.jdbc.source.JdbcSourceConnectorConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.connect.runtime.ConnectorConfig;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.testcontainers.containers.PostgreSQLContainer;

import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;

@Category(IntegrationTest.class)
public class PostgresQuotedIdentifierSourceIT extends BaseConnectorIT {

  private static final String CONNECTOR_NAME = "postgres-quoted-identifier-source";
  private static final String TABLE_NAME = "MixedCase";
  private static final String TABLE_FQN = "public." + TABLE_NAME;
  private static final String TOPIC_PREFIX = "quoted-source-";
  private static final String TOPIC = TOPIC_PREFIX + TABLE_NAME;
  private static final Set<String> EXPECTED_NAMES =
      new HashSet<>(Arrays.asList("Ada", "Grace", "Linus"));
  private static final long POLL_INTERVAL_MS = TimeUnit.SECONDS.toMillis(1);
  private static final long CONSUME_TIMEOUT_MS = TimeUnit.SECONDS.toMillis(30);

  @ClassRule
  public static final PostgreSQLContainer<?> postgres = new PostgreSQLContainer<>("postgres:13")
      .withDatabaseName("testdb")
      .withUsername("test")
      .withPassword("test123");

  private static Connection connection;

  private Map<String, String> props;

  @BeforeClass
  public static void setupClass() throws SQLException {
    connection = DriverManager.getConnection(
        postgres.getJdbcUrl(),
        postgres.getUsername(),
        postgres.getPassword()
    );
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
      stmt.execute("CREATE TABLE \"" + TABLE_NAME + "\" ("
          + "\"Id\" SERIAL PRIMARY KEY, "
          + "\"Name\" VARCHAR(100))");
    }

    try (PreparedStatement stmt = connection.prepareStatement(
        "INSERT INTO \"" + TABLE_NAME + "\" (\"Name\") VALUES (?)")) {
      for (String name : EXPECTED_NAMES) {
        stmt.setString(1, name);
        stmt.addBatch();
      }
      stmt.executeBatch();
    }

    props = new HashMap<>();
    props.put(ConnectorConfig.CONNECTOR_CLASS_CONFIG, JdbcSourceConnector.class.getName());
    props.put(ConnectorConfig.NAME_CONFIG, CONNECTOR_NAME);
    props.put(ConnectorConfig.TASKS_MAX_CONFIG, "1");
    props.put(JdbcSourceConnectorConfig.CONNECTION_URL_CONFIG, postgres.getJdbcUrl());
    props.put(JdbcSourceConnectorConfig.CONNECTION_USER_CONFIG, postgres.getUsername());
    props.put(JdbcSourceConnectorConfig.CONNECTION_PASSWORD_CONFIG, postgres.getPassword());
    props.put(JdbcSourceConnectorConfig.DIALECT_NAME_CONFIG, "PostgreSqlDatabaseDialect");
    props.put(JdbcSourceConnectorConfig.MODE_CONFIG, JdbcSourceConnectorConfig.MODE_BULK);
    props.put(JdbcSourceConnectorConfig.TABLE_INCLUDE_LIST_CONFIG, TABLE_FQN);
    props.put(JdbcSourceConnectorConfig.TOPIC_PREFIX_CONFIG, TOPIC_PREFIX);
    props.put(JdbcSourceConnectorConfig.POLL_INTERVAL_MS_CONFIG, String.valueOf(POLL_INTERVAL_MS));
    props.put(JdbcSourceConnectorConfig.POLL_LINGER_MS_CONFIG, "0");
    props.put(JdbcSourceConnectorConfig.QUOTE_SQL_IDENTIFIERS_CONFIG, "always");
    props.put("value.converter", "org.apache.kafka.connect.json.JsonConverter");
    props.put("value.converter.schemas.enable", "false");
    props.put("key.converter", "org.apache.kafka.connect.storage.StringConverter");
  }

  @After
  public void tearDown() throws SQLException {
    stopConnect();
    try (Statement stmt = connection.createStatement()) {
      stmt.execute("DROP TABLE IF EXISTS \"" + TABLE_NAME + "\"");
    }
  }

  @Test
  public void shouldStreamRowsFromQuotedMixedCaseTable() throws Exception {
    connect.kafka().createTopic(TOPIC, 1);
    connect.configureConnector(CONNECTOR_NAME, props);
    waitForConnectorToStart(CONNECTOR_NAME, 1);

    ConsumerRecords<byte[], byte[]> records = connect.kafka().consume(3, CONSUME_TIMEOUT_MS, TOPIC);
    Set<String> seenNames = new HashSet<>();
    for (ConsumerRecord<byte[], byte[]> record : records.records(TOPIC)) {
      String value = new String(record.value(), StandardCharsets.UTF_8);
      for (String expectedName : EXPECTED_NAMES) {
        if (value.contains("\"Name\":\"" + expectedName + "\"")) {
          seenNames.add(expectedName);
        }
      }
    }

    assertEquals("Source bulk mode should stream every row from the quoted mixed-case table",
        EXPECTED_NAMES, seenNames);
  }
}
