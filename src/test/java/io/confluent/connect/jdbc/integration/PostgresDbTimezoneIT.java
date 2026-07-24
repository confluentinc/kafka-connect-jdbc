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
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.DriverPropertyInfo;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.Statement;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.TimeZone;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.logging.Logger;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Timestamp-mode offset resume coverage where the JVM default timezone, PostgreSQL session
 * timezone, and connector db.timezone are intentionally different.
 */
@Category(IntegrationTest.class)
public class PostgresDbTimezoneIT extends BaseConnectorIT {

  private static final String CONNECTOR_NAME = "postgres-db-timezone-source";
  private static final String TABLE_NAME = "db_timezone_resume";
  private static final String QUALIFIED_TABLE_NAME = "public." + TABLE_NAME;
  private static final String TOPIC_PREFIX = "dbtz-";
  private static final String TOPIC = TOPIC_PREFIX + TABLE_NAME;
  private static final long POLL_INTERVAL_MS = TimeUnit.SECONDS.toMillis(1);
  private static final long CONSUME_TIMEOUT_MS = TimeUnit.SECONDS.toMillis(60);
  private static final long OFFSET_FLUSH_WAIT_MS = TimeUnit.SECONDS.toMillis(3);
  private static final String JVM_TIMEZONE = "Asia/Kolkata";
  private static final String POSTGRES_SESSION_TIMEZONE = "UTC";
  private static final String CONNECTOR_DB_TIMEZONE = "America/Los_Angeles";
  private static final String FIRST_TIMESTAMP_TEXT = "2020-01-15 10:00:00.123";
  private static final String SECOND_TIMESTAMP_TEXT = "2020-01-15 10:00:01.123";
  private static final DateTimeFormatter TIMESTAMP_FORMATTER =
      DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS");
  private static final Pattern UPDATED_AT_PATTERN = Pattern.compile("\\\"updated_at\\\":(\\d+)");

  @ClassRule
  public static final PostgreSQLContainer<?> postgres = new PostgreSQLContainer<>("postgres:13")
      .withDatabaseName("testdb")
      .withUsername("test")
      .withPassword("test123");

  private static Connection connection;
  private static String jdbcUrl;
  private static TimeZone originalDefaultTimeZone;
  private static final List<Driver> deregisteredPostgresDrivers = new ArrayList<>();
  private static Driver utcSessionDriver;

  private Map<String, String> props;

  @BeforeClass
  public static void setupClass() throws Exception {
    originalDefaultTimeZone = TimeZone.getDefault();
    TimeZone.setDefault(TimeZone.getTimeZone(JVM_TIMEZONE));
    jdbcUrl = postgres.getJdbcUrl();

    try {
      deregisterRegisteredPostgresDrivers();
      utcSessionDriver = new UtcSessionPostgresDriver();
      DriverManager.registerDriver(utcSessionDriver);
      connection = DriverManager.getConnection(
          jdbcUrl,
          postgres.getUsername(),
          postgres.getPassword());

      assertSetupConnectionUsesUtcSession();
      assertJvmDefaultTimezone();
    } catch (Exception | Error e) {
      cleanupAfterSetupFailure(e);
    }
  }

  @AfterClass
  public static void teardownClass() throws SQLException {
    cleanupClassState(null);
  }

  @Before
  public void setup() throws SQLException {
    startConnect();

    try (Statement stmt = connection.createStatement()) {
      stmt.execute("CREATE TABLE " + QUALIFIED_TABLE_NAME + " ("
          + "id INTEGER PRIMARY KEY, "
          + "name VARCHAR(100), "
          + "updated_at TIMESTAMP NOT NULL"
          + ")");
    }

    props = new HashMap<>();
    props.put(ConnectorConfig.NAME_CONFIG, CONNECTOR_NAME);
    props.put(ConnectorConfig.CONNECTOR_CLASS_CONFIG, JdbcSourceConnector.class.getName());
    props.put(ConnectorConfig.TASKS_MAX_CONFIG, "1");
    props.put(JdbcSourceConnectorConfig.CONNECTION_URL_CONFIG, jdbcUrl);
    props.put(JdbcSourceConnectorConfig.CONNECTION_USER_CONFIG, postgres.getUsername());
    props.put(JdbcSourceConnectorConfig.CONNECTION_PASSWORD_CONFIG, postgres.getPassword());
    props.put(JdbcSourceConnectorConfig.DIALECT_NAME_CONFIG, "PostgreSqlDatabaseDialect");
    props.put(JdbcSourceConnectorConfig.MODE_CONFIG, JdbcSourceConnectorConfig.MODE_TIMESTAMP);
    props.put(JdbcSourceConnectorConfig.TIMESTAMP_COLUMN_MAPPING_CONFIG,
        QUALIFIED_TABLE_NAME + ":[updated_at]");
    props.put(JdbcSourceConnectorConfig.TABLE_INCLUDE_LIST_CONFIG, QUALIFIED_TABLE_NAME);
    props.put(JdbcSourceConnectorConfig.TOPIC_PREFIX_CONFIG, TOPIC_PREFIX);
    props.put(JdbcSourceConnectorConfig.DB_TIMEZONE_CONFIG, CONNECTOR_DB_TIMEZONE);
    props.put(JdbcSourceConnectorConfig.POLL_INTERVAL_MS_CONFIG, String.valueOf(POLL_INTERVAL_MS));
    props.put(JdbcSourceConnectorConfig.POLL_LINGER_MS_CONFIG, "0");
    props.put(JdbcSourceConnectorConfig.VALIDATE_NON_NULL_CONFIG, "false");
    props.put("value.converter", "org.apache.kafka.connect.json.JsonConverter");
    props.put("value.converter.schemas.enable", "false");
    props.put("key.converter", "org.apache.kafka.connect.storage.StringConverter");
  }

  @After
  public void tearDown() throws SQLException {
    stopConnect();
    try (Statement stmt = connection.createStatement()) {
      stmt.execute("DROP TABLE IF EXISTS " + QUALIFIED_TABLE_NAME);
    }
  }

  @Test
  public void shouldUseConfiguredDbTimezoneAcrossTimestampOffsetRestart() throws Exception {
    long firstExpectedEpochMillis = expectedEpochMillis(FIRST_TIMESTAMP_TEXT, CONNECTOR_DB_TIMEZONE);
    long secondExpectedEpochMillis = expectedEpochMillis(SECOND_TIMESTAMP_TEXT, CONNECTOR_DB_TIMEZONE);

    assertJvmDefaultTimezone();
    connect.kafka().createTopic(TOPIC, 1);
    insertRow(1, "before_restart", FIRST_TIMESTAMP_TEXT);

    connect.configureConnector(CONNECTOR_NAME, props);
    waitForConnectorToStart(CONNECTOR_NAME, 1);
    assertJvmDefaultTimezone();

    ConsumerRecords<byte[], byte[]> firstPoll = connect.kafka().consume(1, CONSUME_TIMEOUT_MS, TOPIC);
    assertEquals("Expected exactly one row before restart", 1, firstPoll.count());
    assertRecord(firstPoll.records(TOPIC).iterator().next(), 1, "before_restart",
        firstExpectedEpochMillis);

    Thread.sleep(OFFSET_FLUSH_WAIT_MS);
    connect.deleteConnector(CONNECTOR_NAME);

    insertRow(2, "after_restart", SECOND_TIMESTAMP_TEXT);

    connect.configureConnector(CONNECTOR_NAME, props);
    waitForConnectorToStart(CONNECTOR_NAME, 1);
    assertJvmDefaultTimezone();

    ConsumerRecords<byte[], byte[]> firstTwoFromEarliest =
        connect.kafka().consume(2, CONSUME_TIMEOUT_MS, TOPIC);
    assertEquals("Restarted connector should leave exactly two earliest records to inspect",
        2, firstTwoFromEarliest.count());

    Iterator<ConsumerRecord<byte[], byte[]>> iterator = firstTwoFromEarliest.records(TOPIC).iterator();
    assertRecord(iterator.next(), 1, "before_restart", firstExpectedEpochMillis);
    assertRecord(iterator.next(), 2, "after_restart", secondExpectedEpochMillis);
  }

  private static void assertSetupConnectionUsesUtcSession() throws SQLException {
    try (Statement stmt = connection.createStatement();
         ResultSet rs = stmt.executeQuery("SHOW TIME ZONE")) {
      assertTrue("SHOW TIME ZONE should return one row", rs.next());
      assertEquals("Setup connection must use a UTC PostgreSQL session timezone",
          POSTGRES_SESSION_TIMEZONE, rs.getString(1));
    }
  }

  private static void assertJvmDefaultTimezone() {
    assertEquals("Test JVM default timezone should stay pinned for the full class",
        JVM_TIMEZONE, TimeZone.getDefault().getID());
  }

  private static void deregisterRegisteredPostgresDrivers() throws SQLException {
    deregisteredPostgresDrivers.clear();
    Enumeration<Driver> drivers = DriverManager.getDrivers();
    while (drivers.hasMoreElements()) {
      Driver driver = drivers.nextElement();
      if (driver.getClass().getName().equals("org.postgresql.Driver")) {
        deregisteredPostgresDrivers.add(driver);
        DriverManager.deregisterDriver(driver);
      }
    }
  }

  private void insertRow(int id, String name, String timestampText) throws SQLException {
    try (PreparedStatement stmt = connection.prepareStatement(
        "INSERT INTO " + QUALIFIED_TABLE_NAME + " (id, name, updated_at) "
            + "VALUES (?, ?, CAST(? AS TIMESTAMP))")) {
      stmt.setInt(1, id);
      stmt.setString(2, name);
      stmt.setString(3, timestampText);
      stmt.executeUpdate();
    }
  }

  private void assertRecord(
      ConsumerRecord<byte[], byte[]> record,
      int expectedId,
      String expectedName,
      long expectedTimestampEpochMillis
  ) {
    String value = new String(record.value(), StandardCharsets.UTF_8);

    assertTrue("Expected row id " + expectedId + " in record " + value,
        value.contains("\"id\":" + expectedId));
    assertTrue("Expected row name " + expectedName + " in record " + value,
        value.contains("\"name\":\"" + expectedName + "\""));
    assertEquals("Timestamp should reflect connector db.timezone for record " + value,
        expectedTimestampEpochMillis, extractUpdatedAt(value));
  }

  private long extractUpdatedAt(String json) {
    Matcher matcher = UPDATED_AT_PATTERN.matcher(json);
    assertTrue("Expected updated_at epoch field in record " + json, matcher.find());
    return Long.parseLong(matcher.group(1));
  }

  private long expectedEpochMillis(String timestampText, String zoneId) {
    LocalDateTime localDateTime = LocalDateTime.parse(timestampText, TIMESTAMP_FORMATTER);
    return localDateTime.atZone(ZoneId.of(zoneId)).toInstant().toEpochMilli();
  }

  private static final class UtcSessionPostgresDriver implements Driver {
    private final Driver delegate = new org.postgresql.Driver();

    @Override
    public Connection connect(String url, Properties info) throws SQLException {
      if (!acceptsURL(url)) {
        return null;
      }

      Connection connection = delegate.connect(url, info);
      if (connection == null) {
        return null;
      }

      try {
        try (Statement statement = connection.createStatement()) {
          statement.execute("SET TIME ZONE 'UTC'");
        }
        return connection;
      } catch (SQLException e) {
        try {
          connection.close();
        } catch (SQLException closeFailure) {
          e.addSuppressed(closeFailure);
        }
        throw e;
      }
    }

    @Override
    public boolean acceptsURL(String url) throws SQLException {
      return delegate.acceptsURL(url);
    }

    @Override
    public DriverPropertyInfo[] getPropertyInfo(String url, Properties info) throws SQLException {
      return delegate.getPropertyInfo(url, info);
    }

    @Override
    public int getMajorVersion() {
      return delegate.getMajorVersion();
    }

    @Override
    public int getMinorVersion() {
      return delegate.getMinorVersion();
    }

    @Override
    public boolean jdbcCompliant() {
      return delegate.jdbcCompliant();
    }

    @Override
    public Logger getParentLogger() throws SQLFeatureNotSupportedException {
      return delegate.getParentLogger();
    }
  }

  private static void cleanupAfterSetupFailure(Throwable failure) throws Exception {
    cleanupClassState(failure);
    if (failure instanceof Exception) {
      throw (Exception) failure;
    }
    throw (Error) failure;
  }

  private static void cleanupClassState(Throwable failure) throws SQLException {
    SQLException cleanupFailure = null;

    cleanupFailure = closeConnection(cleanupFailure);
    cleanupFailure = deregisterUtcSessionDriver(cleanupFailure);
    cleanupFailure = reregisterPostgresDrivers(cleanupFailure);

    if (originalDefaultTimeZone != null) {
      TimeZone.setDefault(originalDefaultTimeZone);
      originalDefaultTimeZone = null;
    }

    if (failure != null) {
      if (cleanupFailure != null) {
        failure.addSuppressed(cleanupFailure);
      }
      return;
    }

    if (cleanupFailure != null) {
      throw cleanupFailure;
    }
  }

  private static SQLException closeConnection(SQLException cleanupFailure) {
    if (connection == null) {
      return cleanupFailure;
    }

    try {
      if (!connection.isClosed()) {
        connection.close();
      }
    } catch (SQLException e) {
      cleanupFailure = appendCleanupFailure(cleanupFailure, e);
    } finally {
      connection = null;
    }

    return cleanupFailure;
  }

  private static SQLException deregisterUtcSessionDriver(SQLException cleanupFailure) {
    if (utcSessionDriver == null) {
      return cleanupFailure;
    }

    try {
      if (isDriverRegistered(utcSessionDriver)) {
        DriverManager.deregisterDriver(utcSessionDriver);
      }
    } catch (SQLException e) {
      cleanupFailure = appendCleanupFailure(cleanupFailure, e);
    } finally {
      utcSessionDriver = null;
    }

    return cleanupFailure;
  }

  private static SQLException reregisterPostgresDrivers(SQLException cleanupFailure) {
    for (Driver driver : deregisteredPostgresDrivers) {
      try {
        if (!isDriverRegistered(driver)) {
          DriverManager.registerDriver(driver);
        }
      } catch (SQLException e) {
        cleanupFailure = appendCleanupFailure(cleanupFailure, e);
      }
    }
    deregisteredPostgresDrivers.clear();
    return cleanupFailure;
  }

  private static SQLException appendCleanupFailure(
      SQLException cleanupFailure,
      SQLException newFailure
  ) {
    if (cleanupFailure == null) {
      return newFailure;
    }
    cleanupFailure.addSuppressed(newFailure);
    return cleanupFailure;
  }

  private static boolean isDriverRegistered(Driver target) {
    Enumeration<Driver> drivers = DriverManager.getDrivers();
    while (drivers.hasMoreElements()) {
      if (drivers.nextElement() == target) {
        return true;
      }
    }
    return false;
  }
}
