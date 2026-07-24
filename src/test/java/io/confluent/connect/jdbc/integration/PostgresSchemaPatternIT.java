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
import org.apache.kafka.connect.runtime.rest.entities.ConnectorStateInfo;
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
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.apache.kafka.test.TestUtils.waitForCondition;
import static org.junit.Assert.assertFalse;

/**
 * Covers {@code schema.pattern} and the duplicate-unqualified-name hazard together, using the same
 * table name in two schemas.
 *
 * <p>The connector names topics by the unqualified table name and refuses to start when two
 * discovered tables share one (otherwise their rows would mix on one topic). So with no
 * {@code schema.pattern} the two {@code orders} tables must fail the connector, and setting
 * {@code schema.pattern} to one schema must narrow discovery to a single table and let it run. The
 * pair proves that server-side schema narrowing works and is the mitigation for the duplicate
 * hazard. No integration test set {@code schema.pattern} before.
 */
@Category(IntegrationTest.class)
public class PostgresSchemaPatternIT extends BaseConnectorIT {

  @ClassRule
  public static final PostgreSQLContainer<?> postgres = new PostgreSQLContainer<>("postgres:13")
      .withDatabaseName("testdb")
      .withUsername("test")
      .withPassword("test123");

  private static final String CONNECTOR_NAME = "schema-pattern-source";
  private static final String SCHEMA_ONE = "app1";
  private static final String SCHEMA_TWO = "app2";
  private static final String TABLE_NAME = "orders";
  private static final String TOPIC_PREFIX = "schemapattern-";
  private static final String TOPIC = TOPIC_PREFIX + TABLE_NAME;
  private static final long POLL_INTERVAL_MS = TimeUnit.SECONDS.toMillis(2);
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
      stmt.execute("CREATE SCHEMA " + SCHEMA_ONE);
      stmt.execute("CREATE SCHEMA " + SCHEMA_TWO);
      for (String schema : new String[] {SCHEMA_ONE, SCHEMA_TWO}) {
        stmt.execute("CREATE TABLE " + schema + "." + TABLE_NAME + " ("
            + "id SERIAL PRIMARY KEY, name VARCHAR(100))");
      }
      // app1 gets two rows, app2 gets five; app2 must never reach the topic when narrowed to app1.
      stmt.execute("INSERT INTO " + SCHEMA_ONE + "." + TABLE_NAME + " (name) "
          + "VALUES ('a1_0'), ('a1_1')");
      stmt.execute("INSERT INTO " + SCHEMA_TWO + "." + TABLE_NAME + " (name) "
          + "VALUES ('a2_0'), ('a2_1'), ('a2_2'), ('a2_3'), ('a2_4')");
    }

    props = new HashMap<>();
    props.put("connector.class", "io.confluent.connect.jdbc.JdbcSourceConnector");
    props.put("tasks.max", "1");
    props.put(JdbcSourceConnectorConfig.CONNECTION_URL_CONFIG, postgres.getJdbcUrl());
    props.put(JdbcSourceConnectorConfig.CONNECTION_USER_CONFIG, postgres.getUsername());
    props.put(JdbcSourceConnectorConfig.CONNECTION_PASSWORD_CONFIG, postgres.getPassword());
    props.put(JdbcSourceConnectorConfig.MODE_CONFIG, JdbcSourceConnectorConfig.MODE_BULK);
    props.put(JdbcSourceConnectorConfig.TABLE_INCLUDE_LIST_CONFIG, ".*" + TABLE_NAME);
    props.put(JdbcSourceConnectorConfig.TOPIC_PREFIX_CONFIG, TOPIC_PREFIX);
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
      stmt.execute("DROP SCHEMA " + SCHEMA_ONE + " CASCADE");
      stmt.execute("DROP SCHEMA " + SCHEMA_TWO + " CASCADE");
    }
  }

  @Test
  public void duplicateUnqualifiedNamesFailWithoutSchemaPattern() throws Exception {
    // No schema.pattern: app1.orders and app2.orders share the unqualified name "orders", so the
    // connector must refuse to start.
    connect.configureConnector(CONNECTOR_NAME, props);
    assertFailsWith(CONNECTOR_NAME, "duplicate unqualified table names");
  }

  @Test
  public void schemaPatternNarrowsDiscoveryToOneSchema() throws Exception {
    // Narrowing to app1 leaves a single "orders" table, so the connector runs and streams app1's rows.
    props.put(JdbcSourceConnectorConfig.SCHEMA_PATTERN_CONFIG, SCHEMA_ONE);
    connect.kafka().createTopic(TOPIC, 1);

    connect.configureConnector(CONNECTOR_NAME, props);
    waitForConnectorToStart(CONNECTOR_NAME, 1);

    // Bulk mode re-emits the result set every poll, so inspect two polls' worth of app1 records.
    ConsumerRecords<byte[], byte[]> records = connect.kafka().consume(4, CONSUME_TIMEOUT_MS, TOPIC);
    for (ConsumerRecord<byte[], byte[]> record : records.records(TOPIC)) {
      String value = new String(record.value(), StandardCharsets.UTF_8);
      assertFalse("schema.pattern=app1 must exclude app2 rows; got " + value,
          value.contains("a2_"));
    }
  }

  private void assertFailsWith(String connectorName, String errorSubstring)
      throws InterruptedException {
    waitForCondition(
        () -> {
          try {
            ConnectorStateInfo info = connect.connectorStatus(connectorName);
            if (info == null) {
              return false;
            }
            boolean connectorFailed = "FAILED".equals(info.connector().state())
                && info.connector().trace() != null
                && info.connector().trace().contains(errorSubstring);
            boolean taskFailed = info.tasks().stream().anyMatch(t ->
                "FAILED".equals(t.state()) && t.trace() != null
                    && t.trace().contains(errorSubstring));
            return connectorFailed || taskFailed;
          } catch (Exception e) {
            return false;
          }
        },
        CONNECTOR_STARTUP_DURATION_MS,
        "Connector or task did not fail with: " + errorSubstring);
  }
}
