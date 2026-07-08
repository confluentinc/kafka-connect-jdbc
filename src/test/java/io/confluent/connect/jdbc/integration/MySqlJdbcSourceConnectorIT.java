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

import ch.vorburger.mariadb4j.DB;
import ch.vorburger.mariadb4j.DBConfigurationBuilder;
import io.confluent.connect.jdbc.JdbcSourceConnector;
import io.confluent.connect.jdbc.source.JdbcSourceConnectorConfig;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.TimeZone;

/**
 * MySQL run of the shared source connector mode matrix (bulk, incrementing, timestamp, and
 * timestamp+incrementing) defined in {@link AbstractJdbcSourceConnectorIT}.
 *
 * <p>Before this, only {@link PostgresJdbcSourceConnectorIT} exercised the mode matrix end to end,
 * so the MySQL dialect had no functional source coverage. A MySQL driver change of the same shape
 * as INC-11312 would have shipped undetected. This subclass runs the identical scenarios against a
 * real MySQL server using the embedded MariaDB used elsewhere in the suite (no Docker).
 */
public class MySqlJdbcSourceConnectorIT extends AbstractJdbcSourceConnectorIT {

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
    try (Connection admin = DriverManager.getConnection(
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

  @Before
  public void setup() throws SQLException {
    super.setup();
    // Pin the session zone so timestamp-mode offset math is deterministic on this host, matching
    // the Postgres run.
    props.put(JdbcSourceConnectorConfig.DB_TIMEZONE_CONFIG, TimeZone.getDefault().getID());
  }

  @Override
  protected boolean needsUpperCaseIdentifiers() {
    return false;
  }

  @Override
  protected DatabaseTestConfig getDatabaseConfig() {
    return new DatabaseTestConfig(jdbcUrl, DB_USER, DB_PASSWORD, "MySqlDatabaseDialect");
  }

  @Override
  protected Class<?> getSourceConnectorClass() {
    return JdbcSourceConnector.class;
  }

  @Override
  protected String getConnectorName() {
    return "mysql-source-connector";
  }

  @Override
  protected String getTopicPrefix() {
    return "test-mysql-";
  }

  @Override
  protected void createTable(String tableName) throws SQLException {
    try (Statement stmt = connection.createStatement()) {
      stmt.execute("CREATE TABLE " + tableName + " ("
          + "id INT AUTO_INCREMENT PRIMARY KEY, "
          + "name VARCHAR(100), "
          + "value VARCHAR(100), "
          + "updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP"
          + ")");
    }
  }
}
