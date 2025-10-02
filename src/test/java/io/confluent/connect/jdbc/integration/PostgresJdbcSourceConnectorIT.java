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

import io.confluent.connect.jdbc.JdbcSourceConnector;
import io.confluent.connect.jdbc.source.JdbcSourceConnectorConfig;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.testcontainers.containers.PostgreSQLContainer;

import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * Example integration test for MySQL JDBC Source Connector testing all 4 modes:
 * 1. Bulk mode - fetches all data on each poll
 * 2. Incrementing mode - fetches data based on incrementing column
 * 3. Timestamp mode - fetches data based on timestamp column
 * 4. Timestamp+Incrementing mode - uses both timestamp and incrementing columns
 * 
 * This demonstrates how external projects can extend AbstractJdbcConnectorIT
 * to create their own database-specific integration tests.
 */
public class PostgresJdbcSourceConnectorIT extends AbstractJdbcSourceConnectorIT {
  @ClassRule
  public static PostgreSQLContainer<?> postgres = new PostgreSQLContainer<>("postgres:13")
      .withDatabaseName("testdb")
      .withUsername("test")
      .withPassword("test123");

  @Override
  protected boolean needsUpperCaseIdentifiers() {
    return false;
  }

  @BeforeClass
  public static void setupClass() throws SQLException {
    postgres.start();
    connection = DriverManager.getConnection(
        postgres.getJdbcUrl(),
        postgres.getUsername(),
        postgres.getPassword()
    );
  }

  @Before
  public void setup() throws SQLException {
    super.setup();
    props.put(JdbcSourceConnectorConfig.DB_TIMEZONE_CONFIG, java.util.TimeZone.getDefault().getID());
  }

  @AfterClass
  public static void teardownClass() throws SQLException {
    if (connection != null && !connection.isClosed()) {
      connection.close();
    }
    postgres.stop();
  }

  @Override
  protected DatabaseTestConfig getDatabaseConfig() {
    return new DatabaseTestConfig(
        postgres.getJdbcUrl(),
        postgres.getUsername(),
        postgres.getPassword(),
        "PostgreSqlDatabaseDialect"
    );
  }

  @Override
  protected Class<?> getSourceConnectorClass() {
    return JdbcSourceConnector.class;
  }

  @Override
  protected String getConnectorName() {
    return "postgres-source-connector";
  }

  @Override
  protected String getTopicPrefix() {
    return "test-postgres-";
  }

  @Override
  public void createTable(String tableName) throws SQLException {
    try (Statement stmt = connection.createStatement()) {
      stmt.execute("CREATE TABLE " + tableName + " (" +
                   "id SERIAL PRIMARY KEY, " +
                   "name VARCHAR(100), " +
                   "value VARCHAR(100), " +
                   "updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP" +
                   ")");
    }
  }

  @Override
  protected void updateRecordTimestamp(String tableName, int id) throws SQLException {
    try (Statement stmt = connection.createStatement()) {
      stmt.execute(String.format(
          "UPDATE %s SET name = 'name_%d_updated', updated_at = CURRENT_TIMESTAMP WHERE id = %d",
          tableName, id, id));
    }
  }
}
