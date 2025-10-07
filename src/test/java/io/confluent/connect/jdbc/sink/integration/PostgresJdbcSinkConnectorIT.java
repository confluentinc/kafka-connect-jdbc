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

package io.confluent.connect.jdbc.sink.integration;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.testcontainers.containers.PostgreSQLContainer;

import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * Integration test for PostgreSQL JDBC Sink Connector.
 *
 * <p>This class extends {@link AbstractJdbcSinkConnectorIT} and provides PostgreSQL-specific
 * implementations for all required abstract methods. It automatically inherits all 11 test methods
 * covering the major sink connector features.</p>
 *
 * <h3>Test Coverage (inherited from parent):</h3>
 * <ul>
 *   <li>Insert, upsert, and update modes</li>
 *   <li>Automatic table creation (auto.create)</li>
 *   <li>Automatic schema evolution (auto.evolve)</li>
 *   <li>Primary key modes including Kafka coordinates</li>
 *   <li>Field whitelisting</li>
 *   <li>Delete support via tombstone records</li>
 *   <li>Timestamp field conversion</li>
 * </ul>
 *
 * <h3>Usage as Template:</h3>
 * <p>This demonstrates how external projects can extend {@link AbstractJdbcSinkConnectorIT}
 * to create their own database-specific integration tests. Simply:</p>
 * <ol>
 *   <li>Extend AbstractJdbcSinkConnectorIT</li>
 *   <li>Implement the abstract methods with database-specific DDL</li>
 *   <li>Set up database connection in @BeforeClass</li>
 *   <li>All test methods are automatically inherited</li>
 * </ol>
 */
public class PostgresJdbcSinkConnectorIT extends AbstractJdbcSinkConnectorIT {

  @ClassRule
  public static PostgreSQLContainer<?> postgres = new PostgreSQLContainer<>("postgres:13")
      .withDatabaseName("testdb")
      .withUsername("test")
      .withPassword("test123");

  @BeforeClass
  public static void setupClass() throws SQLException {
    postgres.start();
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
  protected Class<?> getSinkConnectorClass() {
    return io.confluent.connect.jdbc.JdbcSinkConnector.class;
  }

  @Override
  protected String getConnectorName() {
    return "postgres-sink-connector";
  }

  @Override
  protected String normalizeIdentifier(String identifier) {
    // PostgreSQL uses lowercase identifiers
    return identifier.toLowerCase();
  }

  @Override
  protected void createTableWithPrimaryKey(String tableName) throws SQLException {
    try (Statement stmt = connection.createStatement()) {
      stmt.execute(String.format(
          "CREATE TABLE %s (" +
          "id INTEGER PRIMARY KEY, " +
          "name VARCHAR(100), " +
          "value INTEGER" +
          ")",
          tableName
      ));
    }
  }

  @Override
  protected void createTableForEvolveTest(String tableName) throws SQLException {
    try (Statement stmt = connection.createStatement()) {
      stmt.execute(String.format(
          "CREATE TABLE %s (" +
          "id INTEGER, " +
          "name VARCHAR(100)" +
          ")",
          tableName
      ));
    }
  }

  @Override
  protected void createTableForTimestampTest(String tableName) throws SQLException {
    try (Statement stmt = connection.createStatement()) {
      stmt.execute(String.format(
          "CREATE TABLE %s (" +
          "id INTEGER, " +
          "name VARCHAR(100), " +
          "created_at TIMESTAMP, " +
          "updated_at TIMESTAMP" +
          ")",
          tableName
      ));
    }
  }

  @Override
  protected void createTableForKafkaPkTest(String tableName) throws SQLException {
    try (Statement stmt = connection.createStatement()) {
      stmt.execute(String.format(
          "CREATE TABLE %s (" +
          "__connect_topic VARCHAR(255), " +
          "__connect_partition INTEGER, " +
          "__connect_offset BIGINT, " +
          "id INTEGER, " +
          "name VARCHAR(100), " +
          "PRIMARY KEY (__connect_topic, __connect_partition, __connect_offset)" +
          ")",
          tableName
      ));
    }
  }

  @Override
  protected void createTableForFieldsWhitelistTest(String tableName) throws SQLException {
    try (Statement stmt = connection.createStatement()) {
      stmt.execute(String.format(
          "CREATE TABLE %s (" +
          "id INTEGER, " +
          "name VARCHAR(100), " +
          "email VARCHAR(255), " +
          "age INTEGER" +
          ")",
          tableName
      ));
    }
  }

  @Override
  protected void createTableForDeleteTest(String tableName) throws SQLException {
    try (Statement stmt = connection.createStatement()) {
      stmt.execute(String.format(
          "CREATE TABLE %s (" +
          "id INTEGER PRIMARY KEY, " +
          "name VARCHAR(100)" +
          ")",
          tableName
      ));
    }
  }

  @Override
  protected boolean checkTableExists(String tableName) throws SQLException {
    DatabaseMetaData metaData = connection.getMetaData();
    try (ResultSet rs = metaData.getTables(null, null, tableName.toLowerCase(), new String[]{"TABLE"})) {
      return rs.next();
    }
  }

  @Override
  protected boolean checkColumnExists(String tableName, String columnName) throws SQLException {
    DatabaseMetaData metaData = connection.getMetaData();
    try (ResultSet rs = metaData.getColumns(null, null, tableName.toLowerCase(), columnName.toLowerCase())) {
      return rs.next();
    }
  }

  @Override
  protected void dropTableIfExists(String tableName) throws SQLException {
    try (Statement stmt = connection.createStatement()) {
      stmt.execute("DROP TABLE IF EXISTS " + tableName.toLowerCase());
    }
  }
}


