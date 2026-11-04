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

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;

/**
 * Regression test for table discovery with a literal two-part {@code table.include.list}
 * (the documented {@code schema.table} form, deliberately not a leading-{@code .*} regex).
 *
 * <p>pgjdbc 42.7.5+ returns the database name in {@code TABLE_CAT} from
 * {@code DatabaseMetaData.getTables(...)} where older drivers returned {@code null}. Without
 * the catalog strip in {@code PostgreSqlDatabaseDialect}, the discovered identifier becomes
 * three-part ({@code db.schema.table}) and a literal two-part include list no longer matches,
 * so the task fails with "not assigned a table nor a query".
 *
 * <p>Two cases are pinned: the documented two-part form must discover the table, and — as a
 * boundary guard — a three-part form must NOT match the discovered two-part identifier. The
 * latter documents that three-part include lists are unsupported today; if a future change
 * made them start matching, that is new, unverified behaviour and this test will flag it.
 * (Offset resumption is covered by the {@code *RestoreOffset*} unit tests in
 * {@code JdbcSourceTaskUpdateTest}; a same-driver restart here would not add coverage.)
 */
@Category(IntegrationTest.class)
public class PostgresLiteralIncludeListIT extends BaseConnectorIT {

  private static final String CONNECTOR_NAME = "postgres-literal-include-list";
  private static final String SCHEMA_NAME = "app";
  private static final String TABLE_NAME = "customers";
  private static final String TOPIC_PREFIX = "literal-";
  private static final String TOPIC = TOPIC_PREFIX + TABLE_NAME;
  private static final long POLLING_INTERVAL_MS = TimeUnit.SECONDS.toMillis(2);
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
    // The @ClassRule manages the container lifecycle; only the JDBC connection is set up here.
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
      stmt.execute("CREATE SCHEMA " + SCHEMA_NAME);
      stmt.execute("CREATE TABLE " + SCHEMA_NAME + "." + TABLE_NAME + " ("
          + "id SERIAL PRIMARY KEY, "
          + "name VARCHAR(100)"
          + ")");
    }

    props = new HashMap<>();
    props.put(ConnectorConfig.CONNECTOR_CLASS_CONFIG, JdbcSourceConnector.class.getName());
    props.put(ConnectorConfig.NAME_CONFIG, CONNECTOR_NAME);
    props.put(ConnectorConfig.TASKS_MAX_CONFIG, "1");
    props.put(JdbcSourceConnectorConfig.CONNECTION_URL_CONFIG, postgres.getJdbcUrl());
    props.put(JdbcSourceConnectorConfig.CONNECTION_USER_CONFIG, postgres.getUsername());
    props.put(JdbcSourceConnectorConfig.CONNECTION_PASSWORD_CONFIG, postgres.getPassword());
    props.put(JdbcSourceConnectorConfig.DIALECT_NAME_CONFIG, "PostgreSqlDatabaseDialect");
    props.put(JdbcSourceConnectorConfig.MODE_CONFIG, JdbcSourceConnectorConfig.MODE_INCREMENTING);
    props.put(JdbcSourceConnectorConfig.INCREMENTING_COLUMN_MAPPING_CONFIG,
        SCHEMA_NAME + "." + TABLE_NAME + ":id");
    props.put(JdbcSourceConnectorConfig.TOPIC_PREFIX_CONFIG, TOPIC_PREFIX);
    props.put(JdbcSourceConnectorConfig.POLL_INTERVAL_MS_CONFIG, String.valueOf(POLLING_INTERVAL_MS));
    props.put(JdbcSourceConnectorConfig.POLL_LINGER_MS_CONFIG, "0");
    props.put(JdbcSourceConnectorConfig.VALIDATE_NON_NULL_CONFIG, "false");
    // The literal, documented two-part form. A leading-.* regex would mask the regression
    // because it tolerates a catalog-prefixed identifier.
    props.put(JdbcSourceConnectorConfig.TABLE_INCLUDE_LIST_CONFIG, SCHEMA_NAME + "." + TABLE_NAME);
  }

  @After
  public void tearDown() throws SQLException {
    stopConnect();
    try (Statement stmt = connection.createStatement()) {
      stmt.execute("DROP SCHEMA " + SCHEMA_NAME + " CASCADE");
    }
  }

  @Test
  public void shouldDiscoverTableWithLiteralTwoPartIncludeList() throws Exception {
    insertRows(3);
    connect.kafka().createTopic(TOPIC, 1);

    connect.configureConnector(CONNECTOR_NAME, props);
    waitForConnectorToStart(CONNECTOR_NAME, 1);

    ConsumerRecords<byte[], byte[]> records =
        connect.kafka().consume(3, CONSUME_TIMEOUT_MS, TOPIC);
    assertEquals("Literal schema.table include list should discover the table and stream its rows",
        3, records.count());
  }

  @Test
  public void shouldFailWithThreePartIncludeList() throws Exception {
    insertRows(3);

    // A three-part db.schema.table include list cannot match the two-part schema.table the
    // dialect discovers, so no table is assigned and the task fails loudly. This pins the
    // boundary: if three-part names ever start matching, that is unverified behaviour.
    props.put(JdbcSourceConnectorConfig.TABLE_INCLUDE_LIST_CONFIG,
        postgres.getDatabaseName() + "." + SCHEMA_NAME + "." + TABLE_NAME);

    connect.configureConnector(CONNECTOR_NAME, props);
    assertTasksFailedWithTrace(CONNECTOR_NAME, 1, "not assigned a table nor a query");
  }

  private void insertRows(int count) throws SQLException {
    try (PreparedStatement stmt = connection.prepareStatement(
        "INSERT INTO " + SCHEMA_NAME + "." + TABLE_NAME + " (name) VALUES (?)")) {
      for (int i = 0; i < count; i++) {
        stmt.setString(1, "name_" + i);
        stmt.addBatch();
      }
      stmt.executeBatch();
    }
  }
}
