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
import org.apache.kafka.connect.runtime.rest.entities.ConnectorStateInfo;
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

import static org.apache.kafka.test.TestUtils.waitForCondition;
import static org.junit.Assert.assertTrue;

/**
 * Exercises {@code quote.sql.identifiers} on the source side against a case-sensitive, mixed-case
 * table. Every existing source integration test uses lowercase table names, so the quoting path
 * that matters for ORM-created schemas was never exercised end to end.
 *
 * <p>PostgreSQL folds unquoted identifiers to lowercase, so a table created as {@code "MixedCase"}
 * only resolves when the generated SQL quotes it. With the default {@code quote.sql.identifiers =
 * always} the connector must read it; with {@code never} the generated SQL refers to the folded
 * {@code mixedcase}, which does not exist, so the task must fail.
 */
@Category(IntegrationTest.class)
public class PostgresQuotedIdentifierSourceIT extends BaseConnectorIT {

  @ClassRule
  public static final PostgreSQLContainer<?> postgres = new PostgreSQLContainer<>("postgres:13")
      .withDatabaseName("testdb")
      .withUsername("test")
      .withPassword("test123");

  private static final String CONNECTOR_NAME = "quoted-identifier-source";
  private static final String TABLE_NAME = "MixedCase";
  private static final String TOPIC_PREFIX = "quoted-";
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
      // Quoted, so the identifiers are stored case-sensitively as created.
      stmt.execute("CREATE TABLE \"" + TABLE_NAME + "\" ("
          + "\"Id\" SERIAL PRIMARY KEY, "
          + "\"Name\" VARCHAR(100))");
      stmt.execute("INSERT INTO \"" + TABLE_NAME + "\" (\"Name\") VALUES ('a'), ('b'), ('c')");
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
      stmt.execute("DROP TABLE IF EXISTS \"" + TABLE_NAME + "\"");
    }
  }

  @Test
  public void readsMixedCaseTableWithQuotingAlways() throws Exception {
    props.put(JdbcSourceConnectorConfig.QUOTE_SQL_IDENTIFIERS_CONFIG, "always");
    connect.kafka().createTopic(TOPIC, 1);

    connect.configureConnector(CONNECTOR_NAME, props);
    waitForConnectorToStart(CONNECTOR_NAME, 1);

    ConsumerRecords<byte[], byte[]> records = connect.kafka().consume(3, CONSUME_TIMEOUT_MS, TOPIC);
    assertTrue("Quoting=always should read the case-sensitive mixed-case table",
        records.count() >= 3);
  }

  @Test
  public void unquotedModeCannotReadMixedCaseTable() throws Exception {
    // With never, the generated SQL uses the unquoted name, which PostgreSQL folds to lowercase
    // "mixedcase" - a relation that does not exist - so the task must fail.
    props.put(JdbcSourceConnectorConfig.QUOTE_SQL_IDENTIFIERS_CONFIG, "never");
    connect.kafka().createTopic(TOPIC, 1);

    connect.configureConnector(CONNECTOR_NAME, props);
    assertTaskFailed(CONNECTOR_NAME);
  }

  private void assertTaskFailed(String connectorName) throws InterruptedException {
    waitForCondition(
        () -> {
          try {
            ConnectorStateInfo info = connect.connectorStatus(connectorName);
            if (info == null) {
              return false;
            }
            boolean connectorFailed = "FAILED".equals(info.connector().state());
            boolean taskFailed = info.tasks().stream()
                .anyMatch(t -> "FAILED".equals(t.state()));
            return connectorFailed || taskFailed;
          } catch (Exception e) {
            return false;
          }
        },
        CONNECTOR_STARTUP_DURATION_MS,
        "Connector or task did not fail under quote.sql.identifiers=never");
  }
}
