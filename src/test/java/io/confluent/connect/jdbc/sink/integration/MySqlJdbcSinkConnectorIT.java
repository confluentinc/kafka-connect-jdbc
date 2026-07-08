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

import ch.vorburger.mariadb4j.DB;
import ch.vorburger.mariadb4j.DBConfigurationBuilder;
import io.confluent.connect.jdbc.JdbcSinkConnector;
import org.junit.AfterClass;
import org.junit.BeforeClass;

import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * MySQL run of the shared sink connector feature matrix defined in
 * {@link AbstractJdbcSinkConnectorIT} (insert, upsert and update modes, auto.create, auto.evolve,
 * pk.mode=kafka, fields.whitelist, delete via tombstone, and timestamp.fields.list).
 *
 * <p>The base suite already ran against Postgres and SQL Server, but MySQL had no functional sink
 * integration test, so the MySQL upsert path (ON DUPLICATE KEY UPDATE) was only ever checked as a
 * generated SQL string. This subclass executes it end to end against a real server, using the
 * embedded MariaDB used elsewhere in the suite (no Docker).
 */
public class MySqlJdbcSinkConnectorIT extends AbstractJdbcSinkConnectorIT {

  private static DB db;
  private static String jdbcUrl;
  private static final String DB_NAME = "testdb";
  private static final String DB_USER = "root";
  private static final String DB_PASSWORD = "";

  @BeforeClass
  public static void setupClass() throws Exception {
    DBConfigurationBuilder configBuilder = DBConfigurationBuilder.newBuilder();
    configBuilder.setPort(0); // pick a free port
    db = DB.newEmbeddedDB(configBuilder.build());
    db.start();
    // Create the database over JDBC against the always-present "mysql" schema. mariadb4j's own
    // client-based createDB is unreliable on CI (it shells out to the mysql client).
    try (java.sql.Connection admin = DriverManager.getConnection(
            configBuilder.getURL("mysql"), DB_USER, DB_PASSWORD);
        Statement stmt = admin.createStatement()) {
      stmt.execute("CREATE DATABASE IF NOT EXISTS " + DB_NAME);
    }
    jdbcUrl = configBuilder.getURL(DB_NAME);
    connection = DriverManager.getConnection(jdbcUrl, DB_USER, DB_PASSWORD);
  }

  @AfterClass
  public static void teardownClass() throws Exception {
    if (connection != null && !connection.isClosed()) {
      connection.close();
    }
    if (db != null) {
      db.stop();
    }
  }

  @Override
  protected DatabaseTestConfig getDatabaseConfig() {
    return new DatabaseTestConfig(jdbcUrl, DB_USER, DB_PASSWORD, "MySqlDatabaseDialect");
  }

  @Override
  protected Class<?> getSinkConnectorClass() {
    return JdbcSinkConnector.class;
  }

  @Override
  protected String getConnectorName() {
    return "mysql-sink-connector";
  }

  @Override
  protected String normalizeIdentifier(String identifier) {
    // MySQL column names are case-insensitive and the suite's identifiers are already lowercase,
    // which matches the case-sensitive table names created on Linux CI.
    return identifier;
  }

  @Override
  protected void createTableWithPrimaryKey(String tableName) throws SQLException {
    try (Statement stmt = connection.createStatement()) {
      stmt.execute(String.format(
          "CREATE TABLE %s (id INTEGER PRIMARY KEY, name VARCHAR(100), value INTEGER)",
          tableName));
    }
  }

  @Override
  protected void createTableForEvolveTest(String tableName) throws SQLException {
    try (Statement stmt = connection.createStatement()) {
      stmt.execute(String.format(
          "CREATE TABLE %s (id INTEGER, name VARCHAR(100))", tableName));
    }
  }

  @Override
  protected void createTableForTimestampTest(String tableName) throws SQLException {
    // DATETIME rather than TIMESTAMP to avoid MySQL's implicit default/ON UPDATE behavior on the
    // first TIMESTAMP column and its narrower value range.
    try (Statement stmt = connection.createStatement()) {
      stmt.execute(String.format(
          "CREATE TABLE %s (id INTEGER, name VARCHAR(100), created_at DATETIME, updated_at DATETIME)",
          tableName));
    }
  }

  @Override
  protected void createTableForKafkaPkTest(String tableName) throws SQLException {
    try (Statement stmt = connection.createStatement()) {
      stmt.execute(String.format(
          "CREATE TABLE %s ("
              + "__connect_topic VARCHAR(255), "
              + "__connect_partition INTEGER, "
              + "__connect_offset BIGINT, "
              + "id INTEGER, "
              + "name VARCHAR(100), "
              + "PRIMARY KEY (__connect_topic, __connect_partition, __connect_offset))",
          tableName));
    }
  }

  @Override
  protected void createTableForFieldsWhitelistTest(String tableName) throws SQLException {
    try (Statement stmt = connection.createStatement()) {
      stmt.execute(String.format(
          "CREATE TABLE %s (id INTEGER, name VARCHAR(100), email VARCHAR(255), age INTEGER)",
          tableName));
    }
  }

  @Override
  protected void createTableForDeleteTest(String tableName) throws SQLException {
    try (Statement stmt = connection.createStatement()) {
      stmt.execute(String.format(
          "CREATE TABLE %s (id INTEGER PRIMARY KEY, name VARCHAR(100))", tableName));
    }
  }

  @Override
  protected boolean checkTableExists(String tableName) throws SQLException {
    DatabaseMetaData metaData = connection.getMetaData();
    try (ResultSet rs = metaData.getTables(
        connection.getCatalog(), null, tableName, new String[]{"TABLE"})) {
      return rs.next();
    }
  }

  @Override
  protected boolean checkColumnExists(String tableName, String columnName) throws SQLException {
    DatabaseMetaData metaData = connection.getMetaData();
    try (ResultSet rs = metaData.getColumns(
        connection.getCatalog(), null, tableName, columnName)) {
      return rs.next();
    }
  }

  @Override
  protected void dropTableIfExists(String tableName) throws SQLException {
    try (Statement stmt = connection.createStatement()) {
      stmt.execute("DROP TABLE IF EXISTS " + tableName);
    }
  }
}
