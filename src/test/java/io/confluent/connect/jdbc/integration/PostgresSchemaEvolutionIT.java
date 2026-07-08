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
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.json.JsonConverter;
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
import static org.junit.Assert.assertTrue;

/**
 * Verifies the documented source-side schema evolution: a column added to the table while the
 * connector runs is detected, and records produced afterwards carry the new field in their Connect
 * schema. No integration test performed a mid-stream {@code ALTER TABLE ADD COLUMN} before.
 *
 * <p>(Note: {@code auto.evolve} is a sink concern. The source picks up new columns automatically by
 * re-reading table metadata on each query, which is what this pins.)
 */
@Category(IntegrationTest.class)
public class PostgresSchemaEvolutionIT extends BaseConnectorIT {

  @ClassRule
  public static final PostgreSQLContainer<?> postgres = new PostgreSQLContainer<>("postgres:13")
      .withDatabaseName("testdb")
      .withUsername("test")
      .withPassword("test123");

  private static final String CONNECTOR_NAME = "schema-evolution-source";
  private static final String TABLE_NAME = "evo_table";
  private static final String TOPIC_PREFIX = "evolution-";
  private static final String TOPIC = TOPIC_PREFIX + TABLE_NAME;
  private static final String NEW_COLUMN = "email";
  private static final long POLL_INTERVAL_MS = TimeUnit.SECONDS.toMillis(1);
  private static final long CONSUME_TIMEOUT_MS = TimeUnit.SECONDS.toMillis(60);

  private static Connection connection;

  private JsonConverter converter;
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
    converter = new JsonConverter();
    Map<String, String> converterConfig = new HashMap<>();
    converterConfig.put("schemas.enable", "true");
    converter.configure(converterConfig, false);

    startConnect();

    try (Statement stmt = connection.createStatement()) {
      stmt.execute("CREATE TABLE " + TABLE_NAME + " ("
          + "id SERIAL PRIMARY KEY, name VARCHAR(100))");
      stmt.execute("INSERT INTO " + TABLE_NAME + " (name) VALUES ('a'), ('b')");
    }

    props = new HashMap<>();
    props.put("connector.class", "io.confluent.connect.jdbc.JdbcSourceConnector");
    props.put("tasks.max", "1");
    props.put(JdbcSourceConnectorConfig.CONNECTION_URL_CONFIG, postgres.getJdbcUrl());
    props.put(JdbcSourceConnectorConfig.CONNECTION_USER_CONFIG, postgres.getUsername());
    props.put(JdbcSourceConnectorConfig.CONNECTION_PASSWORD_CONFIG, postgres.getPassword());
    props.put(JdbcSourceConnectorConfig.MODE_CONFIG, JdbcSourceConnectorConfig.MODE_INCREMENTING);
    // New-style column mapping to pair with the new-style include list (the two config styles cannot be mixed).
    props.put(JdbcSourceConnectorConfig.INCREMENTING_COLUMN_MAPPING_CONFIG, ".*" + TABLE_NAME + ":id");
    props.put(JdbcSourceConnectorConfig.TABLE_INCLUDE_LIST_CONFIG, ".*" + TABLE_NAME);
    props.put(JdbcSourceConnectorConfig.TOPIC_PREFIX_CONFIG, TOPIC_PREFIX);
    props.put(JdbcSourceConnectorConfig.VALIDATE_NON_NULL_CONFIG, "false");
    props.put(JdbcSourceConnectorConfig.POLL_INTERVAL_MS_CONFIG, String.valueOf(POLL_INTERVAL_MS));
    props.put(JdbcSourceConnectorConfig.POLL_LINGER_MS_CONFIG, "0");
    props.put("value.converter", "org.apache.kafka.connect.json.JsonConverter");
    props.put("value.converter.schemas.enable", "true");
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
  public void columnAddedMidStreamAppearsInLaterRecords() throws Exception {
    connect.kafka().createTopic(TOPIC, 1);
    connect.configureConnector(CONNECTOR_NAME, props);
    waitForConnectorToStart(CONNECTOR_NAME, 1);

    // Drain the two pre-evolution rows.
    connect.kafka().consume(2, CONSUME_TIMEOUT_MS, TOPIC);

    try (Statement stmt = connection.createStatement()) {
      stmt.execute("ALTER TABLE " + TABLE_NAME + " ADD COLUMN " + NEW_COLUMN + " VARCHAR(100)");
      stmt.execute("INSERT INTO " + TABLE_NAME + " (name, " + NEW_COLUMN + ") "
          + "VALUES ('c', 'c@example.com')");
    }

    // Incrementing mode emits each row once, so exactly three rows; one must carry the new field.
    ConsumerRecords<byte[], byte[]> all = connect.kafka().consume(3, CONSUME_TIMEOUT_MS, TOPIC);
    assertEquals("Incrementing mode should stream exactly the three rows once", 3, all.count());
    boolean sawNewColumn = false;
    for (ConsumerRecord<byte[], byte[]> record : all.records(TOPIC)) {
      SchemaAndValue schemaAndValue = converter.toConnectData(TOPIC, record.value());
      Struct value = (Struct) schemaAndValue.value();
      if (value.schema().field(NEW_COLUMN) != null) {
        sawNewColumn = true;
        break;
      }
    }
    assertTrue("A record produced after ALTER TABLE ADD COLUMN should carry the new field '"
        + NEW_COLUMN + "' in its schema", sawNewColumn);
  }
}
