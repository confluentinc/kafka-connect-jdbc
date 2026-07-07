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
import org.testcontainers.containers.FixedHostPortGenericContainer;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.TimeZone;

/**
 * SQL Server run of the shared source connector mode matrix (bulk, incrementing, timestamp, and
 * timestamp+incrementing) defined in {@link AbstractJdbcSourceConnectorIT}.
 *
 * <p>Before this, only {@link PostgresJdbcSourceConnectorIT} and {@link MySqlJdbcSourceConnectorIT}
 * exercised the mode matrix end to end. The SQL Server container pattern mirrors the existing
 * {@code MSSqlServerTableIT}. {@code DATETIME2} is used for the timestamp column deliberately:
 * {@code DATETIME} has only 3.33 ms resolution and makes timestamp mode loop on the most recent
 * row (see {@code MSSQLDateTimeIT}), whereas {@code DATETIME2} behaves like the other dialects.
 */
public class MsSqlJdbcSourceConnectorIT extends AbstractJdbcSourceConnectorIT {

  private static final String MSSQL_URL =
      "jdbc:sqlserver://0.0.0.0:1433;encrypt=true;trustServerCertificate=true";
  private static final String DB_USER = "sa";
  private static final String DB_PASSWORD = "reallyStrongPwd123";

  @ClassRule
  @SuppressWarnings("deprecation")
  public static final FixedHostPortGenericContainer<?> mssqlServer =
      new FixedHostPortGenericContainer<>("mcr.microsoft.com/mssql/server:2019-latest")
          .withEnv("ACCEPT_EULA", "Y")
          .withEnv("SA_PASSWORD", DB_PASSWORD)
          .withFixedExposedPort(1433, 1433);

  @BeforeClass
  public static void setupClass() throws Exception {
    Class.forName("com.microsoft.sqlserver.jdbc.SQLServerDriver");
    // The container's port opens before SQL Server is ready to accept logins, so retry the first
    // connection until the server is up.
    connection = openConnectionWithRetry();
  }

  private static Connection openConnectionWithRetry() throws Exception {
    SQLException lastError = null;
    for (int attempt = 0; attempt < 45; attempt++) {
      try {
        return DriverManager.getConnection(MSSQL_URL, DB_USER, DB_PASSWORD);
      } catch (SQLException e) {
        lastError = e;
        Thread.sleep(2000);
      }
    }
    throw lastError;
  }

  @AfterClass
  public static void teardownClass() throws SQLException {
    if (connection != null && !connection.isClosed()) {
      connection.close();
    }
  }

  @Before
  public void setup() throws SQLException {
    super.setup();
    props.put(JdbcSourceConnectorConfig.DB_TIMEZONE_CONFIG, TimeZone.getDefault().getID());
  }

  @Override
  protected boolean needsUpperCaseIdentifiers() {
    return false;
  }

  @Override
  protected DatabaseTestConfig getDatabaseConfig() {
    return new DatabaseTestConfig(MSSQL_URL, DB_USER, DB_PASSWORD, "SqlServerDatabaseDialect");
  }

  @Override
  protected Class<?> getSourceConnectorClass() {
    return JdbcSourceConnector.class;
  }

  @Override
  protected String getConnectorName() {
    return "sqlserver-source-connector";
  }

  @Override
  protected String getTopicPrefix() {
    return "test-sqlserver-";
  }

  @Override
  protected void createTable(String tableName) throws SQLException {
    try (Statement stmt = connection.createStatement()) {
      stmt.execute("CREATE TABLE " + tableName + " ("
          + "id INT IDENTITY(1,1) PRIMARY KEY, "
          + "name NVARCHAR(100), "
          + "value NVARCHAR(100), "
          + "updated_at DATETIME2 DEFAULT SYSUTCDATETIME()"
          + ")");
    }
  }

  @Override
  protected void updateRecordTimestamp(String tableName, int id) throws SQLException {
    try (Statement stmt = connection.createStatement()) {
      stmt.execute(String.format(
          "UPDATE %s SET name = 'name_%d_updated', updated_at = SYSUTCDATETIME() WHERE id = %d",
          tableName, id, id));
    }
  }
}
