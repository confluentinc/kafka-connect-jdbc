/*
 * Copyright [2024 - 2024] Confluent Inc.
 */

package io.confluent.connect.jdbc.integration;

import io.confluent.common.utils.IntegrationTest;
import io.confluent.connect.jdbc.source.JdbcSourceConnectorConfig;
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

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 * Integration test for PostgreSQL dialect.
 * Tests all major PostgreSQL data types end-to-end through the JDBC Source Connector.
 */
@Category(IntegrationTest.class)
public class PostgreSqlDialectIT extends BaseConnectorIT {
  
  @ClassRule
  public static PostgreSQLContainer<?> postgres = new PostgreSQLContainer<>("postgres:13")
      .withDatabaseName("testdb")
      .withUsername("test")
      .withPassword("test123");
  
  private static Connection connection;
  
  private static final String TABLE_NAME = "pg_test_types";
  private static final String CONNECTOR_NAME = "PostgreSqlSourceConnector";
  private static final String TOPIC_PREFIX = "postgres-test-";
  private static final long POLLING_INTERVAL_MS = 1000L;
  
  // Test values for each data type
  private static final Short smallintCol = 32767;
  private static final Integer integerCol = 2147483647;
  private static final Long bigintCol = 9223372036854775807L;
  private static final BigDecimal numericCol = new BigDecimal("123456789012345.67890");
  private static final BigDecimal decimalCol = new BigDecimal("98765.43210");
  private static final Float realCol = 3.14159f;
  private static final Double doubleCol = 2.718281828459045;
  
  private static final String charCol = "test      "; // CHAR(10) - padded
  private static final String varcharCol = "variable text";
  private static final String textCol = "This is a long text field";
  
  private static final byte[] byteaCol = new byte[]{(byte) 0xDE, (byte) 0xAD, (byte) 0xBE, (byte) 0xEF};
  
  private static final String uuidCol = "550e8400-e29b-41d4-a716-446655440000";
  private static final String jsonCol = "{\"key\": \"value\", \"number\": 123}";
  private static final String jsonbCol = "{\"key\": \"value\", \"number\": 456}";
  
  // Date/Time test values
  private static final String dateColStr = "2024-10-06";
  private static final String timeColStr = "12:30:45";
  private static final String timestampColStr = "2024-10-06 12:30:45.123456";
  
  // Expected values for validation
  private static final java.util.Date expectedDate = java.sql.Date.valueOf("2024-10-06");
  
  private static final Boolean booleanCol = true;
  
  private JsonConverter converter;
  private Map<String, String> props;
  
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
  
  @Before
  public void setup() throws Exception {
    initConverter();
    startConnect();
    populateProps();
    
    // Create table and insert test data
    createTableAndInsertData();
  }
  
  @After
  public void tearDown() {
    if (connect != null) {
      try {
        connect.deleteConnector(CONNECTOR_NAME);
      } catch (Exception e) {
        System.out.println("Failed to delete connector: " + e.getMessage());
      }
      connect.stop();
    }
    
    // Clean up table
    dropTable();
  }
  
  private void initConverter() {
    converter = new JsonConverter();
    Map<String, String> config = new HashMap<>();
    config.put("schemas.enable", "true");
    converter.configure(config, false);
  }

  private void populateProps() {
    props = new HashMap<>();
    props.put("connector.class", "io.confluent.connect.jdbc.JdbcSourceConnector");
    props.put("tasks.max", "1");
    props.put(JdbcSourceConnectorConfig.CONNECTION_URL_CONFIG, postgres.getJdbcUrl());
    props.put(JdbcSourceConnectorConfig.CONNECTION_USER_CONFIG, postgres.getUsername());
    props.put(JdbcSourceConnectorConfig.CONNECTION_PASSWORD_CONFIG, postgres.getPassword());
    props.put(JdbcSourceConnectorConfig.POLL_LINGER_MS_CONFIG, "0");
    props.put(JdbcSourceConnectorConfig.MODE_CONFIG, "bulk");
    props.put(JdbcSourceConnectorConfig.TOPIC_PREFIX_CONFIG, TOPIC_PREFIX);
    props.put(JdbcSourceConnectorConfig.TABLE_WHITELIST_CONFIG, TABLE_NAME);
    props.put(JdbcSourceConnectorConfig.TIMESTAMP_GRANULARITY_CONFIG, "nanos_long");
    props.put(JdbcSourceConnectorConfig.POLL_INTERVAL_MS_CONFIG, String.valueOf(POLLING_INTERVAL_MS));
    props.put("value.converter", "org.apache.kafka.connect.json.JsonConverter");
    props.put("value.converter.schemas.enable", "true");
    props.put("key.converter", "org.apache.kafka.connect.json.JsonConverter");
    props.put("key.converter.schemas.enable", "true");
  }
  
  private Connection getConnection() throws SQLException {
    return connection;
  }
  
  private void createTableAndInsertData() throws SQLException {
    try (Connection conn = getConnection(); Statement stmt = conn.createStatement()) {
      // Drop table if exists
      stmt.execute("DROP TABLE IF EXISTS " + TABLE_NAME);
      
      // Create table with all major PostgreSQL data types
      String createTableSql = "CREATE TABLE " + TABLE_NAME + " (\n" +
          "  id SERIAL PRIMARY KEY,\n" +
          // Numeric types
          "  smallint_col SMALLINT,\n" +
          "  integer_col INTEGER,\n" +
          "  bigint_col BIGINT,\n" +
          "  numeric_col NUMERIC(20, 5),\n" +
          "  decimal_col DECIMAL(10, 5),\n" +
          "  real_col REAL,\n" +
          "  double_col DOUBLE PRECISION,\n" +
          // String types
          "  char_col CHAR(10),\n" +
          "  varchar_col VARCHAR(100),\n" +
          "  text_col TEXT,\n" +
          // Binary type
          "  bytea_col BYTEA,\n" +
          // Special PostgreSQL types
          "  uuid_col UUID,\n" +
          "  json_col JSON,\n" +
          "  jsonb_col JSONB,\n" +
          // Date/Time types
          "  date_col DATE,\n" +
          "  time_col TIME,\n" +
          "  timestamp_col TIMESTAMP,\n" +
          // Boolean
          "  boolean_col BOOLEAN\n" +
          ")";
      
      stmt.execute(createTableSql);
      System.out.println("Created table: " + TABLE_NAME);
      
      // Insert test data
      String insertSql = "INSERT INTO " + TABLE_NAME + " (\n" +
          "  smallint_col, integer_col, bigint_col, numeric_col, decimal_col,\n" +
          "  real_col, double_col,\n" +
          "  char_col, varchar_col, text_col,\n" +
          "  bytea_col,\n" +
          "  uuid_col, json_col, jsonb_col,\n" +
          "  date_col, time_col, timestamp_col,\n" +
          "  boolean_col\n" +
          ") VALUES (\n" +
          "  " + smallintCol + ", " + integerCol + ", " + bigintCol + ", " + numericCol + ", " + decimalCol + ",\n" +
          "  " + realCol + ", " + doubleCol + ",\n" +
          "  '" + charCol.trim() + "', '" + varcharCol + "', '" + textCol + "',\n" +
          "  decode('DEADBEEF', 'hex'),\n" +  // Use decode() for clarity - avoids escaping issues
          "  '" + uuidCol + "', '" + jsonCol + "', '" + jsonbCol + "',\n" +
          "  '" + dateColStr + "', '" + timeColStr + "', '" + timestampColStr + "',\n" +
          "  true\n" +
          ")";
      
      stmt.execute(insertSql);
      System.out.println("Inserted test data into table: " + TABLE_NAME);
    }
  }
  
  private void dropTable() {
    try (Connection conn = getConnection(); Statement stmt = conn.createStatement()) {
      stmt.execute("DROP TABLE IF EXISTS " + TABLE_NAME);
      System.out.println("Dropped table: " + TABLE_NAME);
    } catch (SQLException e) {
      System.out.println("Failed to drop table: " + e.getMessage());
    }
  }
  
  @Test
  public void testPostgreSqlDialect() throws Exception {
    String topicName = TOPIC_PREFIX + TABLE_NAME;
    
    // Create topic
    connect.kafka().createTopic(topicName, 1);
    
    // Configure and start connector
    connect.configureConnector(CONNECTOR_NAME, props);
    
    // Wait for connector to start
    waitForConnectorToStart(CONNECTOR_NAME, 1);
    
    System.out.println("Connector started, waiting for records...");
    
    // Wait a bit for polling
    Thread.sleep(2000);
    
    // Consume records (expecting at least 1 record)
    System.out.println("Consuming records from topic: " + topicName);
    ConsumerRecords<byte[], byte[]> consumedRecords =
        connect.kafka().consume(1, 15000, topicName);

    assertNotNull("No records consumed", consumedRecords);
    assertTrue("Expected at least 1 record, got " + consumedRecords.count(),
        consumedRecords.count() >= 1);
    
    // Validate the consumed record
    consumedRecords.records(topicName).forEach(record -> {
      System.out.println("Validating record from topic: " + record.topic());
      SchemaAndValue schemaAndValue = converter.toConnectData(record.topic(), record.value());
      validateAllTypeValues((Struct) schemaAndValue.value());
    });
    
    System.out.println("PostgreSQL dialect integration test completed successfully!");
  }
  
  private void validateAllTypeValues(Struct valueStruct) {
    System.out.println("Validating all field values...");
    
    // Validate ID field exists
    assertNotNull("ID should not be null", valueStruct.get("id"));
    
    // Validate numeric types
    assertEquals("SMALLINT", smallintCol, valueStruct.get("smallint_col"));
    assertEquals("INTEGER", integerCol, valueStruct.get("integer_col"));
    assertEquals("BIGINT", bigintCol, valueStruct.get("bigint_col"));
    
    // Numeric and Decimal - compare as BigDecimal
    BigDecimal actualNumeric = (BigDecimal) valueStruct.get("numeric_col");
    assertNotNull("NUMERIC should not be null", actualNumeric);
    assertEquals("NUMERIC", 0, numericCol.compareTo(actualNumeric));
    
    BigDecimal actualDecimal = (BigDecimal) valueStruct.get("decimal_col");
    assertNotNull("DECIMAL should not be null", actualDecimal);
    assertEquals("DECIMAL", 0, decimalCol.compareTo(actualDecimal));
    
    // REAL and DOUBLE - allow small epsilon for floating point comparison
    Float actualReal = ((Number) valueStruct.get("real_col")).floatValue();
    assertNotNull("REAL should not be null", actualReal);
    assertEquals("REAL", realCol, actualReal, 0.00001);
    
    Double actualDouble = ((Number) valueStruct.get("double_col")).doubleValue();
    assertNotNull("DOUBLE should not be null", actualDouble);
    assertEquals("DOUBLE", doubleCol, actualDouble, 0.000000001);
    
    // String types
    assertEquals("CHAR", charCol, valueStruct.get("char_col"));
    assertEquals("VARCHAR", varcharCol, valueStruct.get("varchar_col"));
    assertEquals("TEXT", textCol, valueStruct.get("text_col"));
    
    // Binary type - BYTEA
    byte[] actualBytea = (byte[]) valueStruct.get("bytea_col");
    assertNotNull("BYTEA should not be null", actualBytea);
    assertArrayEquals("BYTEA values should match", byteaCol, actualBytea);
    System.out.println("BYTEA validated: " + actualBytea.length + " bytes");
    
    // PostgreSQL special types
    assertEquals("UUID", uuidCol, valueStruct.get("uuid_col"));
    System.out.println("UUID validated: " + uuidCol);
    
    // JSON/JSONB - returned as strings
    String actualJson = (String) valueStruct.get("json_col");
    assertNotNull("JSON should not be null", actualJson);
    assertTrue("JSON should contain expected data", 
        actualJson.contains("key") && actualJson.contains("value"));
    System.out.println("JSON value: " + actualJson);
    
    String actualJsonb = (String) valueStruct.get("jsonb_col");
    assertNotNull("JSONB should not be null", actualJsonb);
    assertTrue("JSONB should contain expected data", 
        actualJsonb.contains("key") && actualJsonb.contains("value"));
    System.out.println("JSONB value: " + actualJsonb);
    
    // Date/Time types - with proper validation
    // DATE - should be a java.util.Date
    Object dateValue = valueStruct.get("date_col");
    assertNotNull("DATE should not be null", dateValue);
    assertTrue("DATE should be java.util.Date but was: " + dateValue.getClass().getName(),
        dateValue instanceof java.util.Date);
    
    java.util.Date actualDate = (java.util.Date) dateValue;
    // Compare the date portion (year, month, day)
    assertEquals("DATE year should match", expectedDate.getYear(), actualDate.getYear());
    assertEquals("DATE month should match", expectedDate.getMonth(), actualDate.getMonth());
    assertEquals("DATE day should match", expectedDate.getDate(), actualDate.getDate());
    System.out.println("DATE validated: " + actualDate);
    
    // TIME - returned as java.util.Date (even with nanos_long config, connector returns Date for TIME)
    Object timeValue = valueStruct.get("time_col");
    assertNotNull("TIME should not be null", timeValue);
    System.out.println("TIME value: " + timeValue + " (type: " + timeValue.getClass().getName() + ")");
    assertTrue("TIME should be java.util.Date but was: " + timeValue.getClass().getName(),
        timeValue instanceof java.util.Date);
    
    java.util.Date actualTime = (java.util.Date) timeValue;
    // Check that hour, minute, and second match expected values in UTC
    java.util.Calendar utcCal = java.util.Calendar.getInstance(java.util.TimeZone.getTimeZone("UTC"));
    utcCal.setTime(actualTime);
    assertEquals("TIME hour should match in UTC", 12, utcCal.get(java.util.Calendar.HOUR_OF_DAY));
    assertEquals("TIME minute should match in UTC", 30, utcCal.get(java.util.Calendar.MINUTE));
    assertEquals("TIME second should match in UTC", 45, utcCal.get(java.util.Calendar.SECOND));

    System.out.println("TIME validated: " + utcCal.get(java.util.Calendar.HOUR_OF_DAY) + ":" + utcCal.get(java.util.Calendar.MINUTE) + ":" + utcCal.get(java.util.Calendar.SECOND));
    
    // TIMESTAMP - configured with nanos_long, so returned as Long (nanos since epoch)
    Object timestampValue = valueStruct.get("timestamp_col");
    assertNotNull("TIMESTAMP should not be null", timestampValue);
    System.out.println("TIMESTAMP value: " + timestampValue + " (type: " + timestampValue.getClass().getName() + ")");
    assertTrue("TIMESTAMP should be Long (nanos) but was: " + timestampValue.getClass().getName(),
        timestampValue instanceof Long);
    
    Long timestampNanos = (Long) timestampValue;
    // Validate that the timestampNanos matches the epoch nanos for the timestampColStr in UTC
    java.time.format.DateTimeFormatter formatter = java.time.format.DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSSSSS");
    java.time.LocalDateTime localDateTime = java.time.LocalDateTime.parse(timestampColStr, formatter);
    java.time.Instant expectedInstant = localDateTime.atZone(java.time.ZoneOffset.UTC).toInstant();
    long expectedEpochNanos = expectedInstant.getEpochSecond() * 1_000_000_000L + expectedInstant.getNano();
    assertEquals("TIMESTAMP epoch nanos should match expected value for " + timestampColStr, expectedEpochNanos, timestampNanos.longValue());
    System.out.println("TIMESTAMP validated as nanos: " + timestampNanos);
    
    // Boolean
    assertEquals("BOOLEAN", booleanCol, valueStruct.get("boolean_col"));
    
    System.out.println("All field values validated successfully!");
  }
}

