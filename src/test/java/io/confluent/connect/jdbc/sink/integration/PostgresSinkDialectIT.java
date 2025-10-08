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

import io.confluent.common.utils.IntegrationTest;
import io.confluent.connect.jdbc.integration.BaseConnectorIT;
import io.confluent.connect.jdbc.sink.JdbcSinkConfig;
import org.apache.kafka.connect.data.Date;
import org.apache.kafka.connect.data.Decimal;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.data.Time;
import org.apache.kafka.connect.data.Timestamp;
import org.apache.kafka.connect.json.JsonConverter;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.testcontainers.containers.PostgreSQLContainer;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

/**
 * Integration test for PostgreSQL Sink Dialect.
 * Tests that all major data types are correctly written from Kafka to PostgreSQL.
 * Also tests nullable columns and default values handling.
 */
@Category(IntegrationTest.class)
public class PostgresSinkDialectIT extends BaseConnectorIT {

  @ClassRule
  public static PostgreSQLContainer<?> postgres = new PostgreSQLContainer<>("postgres:13")
      .withDatabaseName("testdb")
      .withUsername("test")
      .withPassword("test123");

  private static final String CONNECTOR_NAME = "postgres_sink_connector";
  private static final String TOPIC_NAME = "postgres_sink_test_topic";
  private static final String TABLE_NAME = "sink_" + TOPIC_NAME;

  private Map<String, String> props;
  private JsonConverter jsonConverter;

  // Test values for PostgreSQL data types
  private static final short smallintVal = 32767;
  private static final int intVal = 2147483647;
  private static final long bigintVal = 9223372036854775807L;
  private static final BigDecimal decimalVal = new BigDecimal("12345.67");
  private static final float realVal = 123.456f;
  private static final double doublePrecisionVal = 789.012345;

  private static final String varcharVal = "test varchar";
  private static final String textVal = "This is a text field";
  private static final String jsonVal = "{\"key\": \"value\", \"number\": 123}";

  private static final byte[] byteaVal = new byte[]{(byte) 0xDE, (byte) 0xAD, (byte) 0xBE, (byte) 0xEF};
  private static final boolean booleanVal = true;

  // Date/Time test values
  private static final java.util.Date dateVal;
  private static final java.util.Date timeVal;
  private static final java.util.Date timestampVal;

  static {
    // Date at midnight UTC
    java.util.Calendar cal = java.util.Calendar.getInstance(java.util.TimeZone.getTimeZone("UTC"));
    cal.set(2025, java.util.Calendar.OCTOBER, 6, 0, 0, 0);
    cal.set(java.util.Calendar.MILLISECOND, 0);
    dateVal = cal.getTime();

    // Time - just the time component
    cal.set(1970, java.util.Calendar.JANUARY, 1, 14, 30, 45);
    cal.set(java.util.Calendar.MILLISECOND, 0);
    timeVal = cal.getTime();

    // Timestamp - date and time
    cal.set(2025, java.util.Calendar.OCTOBER, 6, 14, 30, 45);
    cal.set(java.util.Calendar.MILLISECOND, 123);
    timestampVal = cal.getTime();
  }

  @Before
  public void setup() throws Exception {
    startConnect();
    initConverter();
    populateProps();
  }

  private void initConverter() {
    jsonConverter = new JsonConverter();
    Map<String, String> config = new HashMap<>();
    config.put("schemas.enable", "true");
    jsonConverter.configure(config, false);
  }

  @After
  public void tearDown() throws Exception {
    if (connect != null) {
      try {
        connect.deleteConnector(CONNECTOR_NAME);
      } catch (Exception e) {
        System.out.println("Failed to delete connector: " + e.getMessage());
      }
    }

    // Clean up table
    dropTable();

    if (connect != null) {
      connect.stop();
    }
  }

  private void populateProps() {
    props = new HashMap<>();
    props.put("connector.class", "io.confluent.connect.jdbc.JdbcSinkConnector");
    props.put("tasks.max", "1");
    props.put(JdbcSinkConfig.CONNECTION_URL, postgres.getJdbcUrl());
    props.put(JdbcSinkConfig.CONNECTION_USER, postgres.getUsername());
    props.put("connection.password", postgres.getPassword());
    props.put("topics", TOPIC_NAME);
    props.put("auto.create", "true");
    props.put("auto.evolve", "true");
    props.put("insert.mode", "insert");
    props.put("table.name.format", "sink_${topic}");
    props.put("pk.mode", "none");
    props.put("key.converter", "org.apache.kafka.connect.json.JsonConverter");
    props.put("value.converter", "org.apache.kafka.connect.json.JsonConverter");
  }

  private Connection getConnection() throws SQLException {
    return DriverManager.getConnection(
        postgres.getJdbcUrl(),
        postgres.getUsername(),
        postgres.getPassword()
    );
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
  public void testPostgresSinkWithAllTypes() throws Exception {
    // Create Kafka topic
    connect.kafka().createTopic(TOPIC_NAME, 1);

    // Create schema with PostgreSQL types
    Schema schema = createSchemaWithAllTypes();

    // Create and produce a record
    Struct struct = createStructWithAllTypes(schema);
    produceRecord(schema, struct);

    // Configure and start sink connector
    connect.configureConnector(CONNECTOR_NAME, props);
    waitForConnectorToStart(CONNECTOR_NAME, 1);

    // Wait for record to be written to PostgreSQL
    waitForCommittedRecords(
        CONNECTOR_NAME,
        Collections.singleton(TOPIC_NAME),
        1,
        1,
        TimeUnit.MINUTES.toMillis(2)
    );

    System.out.println("Record committed to PostgreSQL, verifying data...");

    // Verify table structure and data
    verifyTableAndData();

    System.out.println("PostgreSQL sink dialect integration test completed successfully!");
  }

  private Schema createSchemaWithAllTypes() {
    return SchemaBuilder.struct().name("com.example.PostgresAllTypes")
        // Numeric types
        .field("smallint_col", Schema.INT16_SCHEMA)
        .field("int_col", Schema.INT32_SCHEMA)
        .field("bigint_col", Schema.INT64_SCHEMA)
        .field("decimal_col", Decimal.schema(2))
        .field("real_col", Schema.FLOAT32_SCHEMA)
        .field("double_col", Schema.FLOAT64_SCHEMA)
        // String types
        .field("varchar_col", Schema.STRING_SCHEMA)
        .field("text_col", Schema.STRING_SCHEMA)
        .field("json_col", Schema.STRING_SCHEMA)
        // Binary type
        .field("bytea_col", Schema.BYTES_SCHEMA)
        // Date/Time types
        .field("date_col", Date.SCHEMA)
        .field("time_col", Time.SCHEMA)
        .field("timestamp_col", Timestamp.SCHEMA)
        // Boolean
        .field("boolean_col", Schema.BOOLEAN_SCHEMA)
        // Array types (PostgreSQL-specific)
        .field("int_array_col", SchemaBuilder.array(Schema.INT32_SCHEMA).build())
        .field("text_array_col", SchemaBuilder.array(Schema.STRING_SCHEMA).build())
        .build();
  }

  private Struct createStructWithAllTypes(Schema schema) {
    return new Struct(schema)
        .put("smallint_col", smallintVal)
        .put("int_col", intVal)
        .put("bigint_col", bigintVal)
        .put("decimal_col", decimalVal)
        .put("real_col", realVal)
        .put("double_col", doublePrecisionVal)
        .put("varchar_col", varcharVal)
        .put("text_col", textVal)
        .put("json_col", jsonVal)
        .put("bytea_col", byteaVal)
        .put("date_col", dateVal)
        .put("time_col", timeVal)
        .put("timestamp_col", timestampVal)
        .put("boolean_col", booleanVal)
        .put("int_array_col", Arrays.asList(1, 2, 3, 4, 5))
        .put("text_array_col", Arrays.asList("apple", "banana", "cherry"));
  }

  private void produceRecord(Schema schema, Struct struct) {
    String kafkaValue = new String(jsonConverter.fromConnectData(TOPIC_NAME, schema, struct));
    connect.kafka().produce(TOPIC_NAME, null, kafkaValue);
    System.out.println("Produced record to topic: " + TOPIC_NAME);
  }

  private void verifyTableAndData() throws SQLException {
    try (Connection conn = getConnection(); Statement stmt = conn.createStatement()) {

      // Verify table exists
      try (ResultSet rs = conn.getMetaData().getTables(null, null, TABLE_NAME, new String[]{"TABLE"})) {
        assertTrue("Table " + TABLE_NAME + " should exist", rs.next());
        System.out.println("✓ Table exists: " + TABLE_NAME);
      }

      // Query the data
      String query = "SELECT * FROM " + TABLE_NAME;
      try (ResultSet rs = stmt.executeQuery(query)) {
        assertTrue("Should have at least one row", rs.next());
        System.out.println("✓ Row exists in table");

        // Verify numeric types
        assertEquals("SMALLINT", smallintVal, rs.getShort("smallint_col"));
        assertEquals("INT", intVal, rs.getInt("int_col"));
        assertEquals("BIGINT", bigintVal, rs.getLong("bigint_col"));

        BigDecimal actualDecimal = rs.getBigDecimal("decimal_col");
        assertNotNull("DECIMAL should not be null", actualDecimal);
        assertEquals("DECIMAL", 0, decimalVal.compareTo(actualDecimal));

        assertEquals("REAL", realVal, rs.getFloat("real_col"), 0.001);
        assertEquals("DOUBLE PRECISION", doublePrecisionVal, rs.getDouble("double_col"), 0.000001);

        System.out.println("✓ All numeric types verified");

        // String types
        assertEquals("VARCHAR", varcharVal, rs.getString("varchar_col"));
        assertEquals("TEXT", textVal, rs.getString("text_col"));
        assertEquals("JSON", jsonVal, rs.getString("json_col"));
        System.out.println("✓ All string types verified");

        // Binary type
        byte[] actualBytea = rs.getBytes("bytea_col");
        assertNotNull("BYTEA should not be null", actualBytea);
        assertArrayEquals("BYTEA values should match", byteaVal, actualBytea);
        System.out.println("✓ Binary type verified");

        // Date/Time types
        java.sql.Date actualDate = rs.getDate("date_col");
        assertNotNull("DATE should not be null", actualDate);
        System.out.println("✓ DATE: " + actualDate);

        java.sql.Time actualTime = rs.getTime("time_col");
        assertNotNull("TIME should not be null", actualTime);
        System.out.println("✓ TIME: " + actualTime);

        java.sql.Timestamp actualTimestamp = rs.getTimestamp("timestamp_col");
        assertNotNull("TIMESTAMP should not be null", actualTimestamp);
        System.out.println("✓ TIMESTAMP: " + actualTimestamp);

        // Boolean
        boolean actualBoolean = rs.getBoolean("boolean_col");
        assertEquals("BOOLEAN", booleanVal, actualBoolean);
        System.out.println("✓ BOOLEAN verified");

        // Array types (PostgreSQL-specific)
        java.sql.Array intArray = rs.getArray("int_array_col");
        assertNotNull("INT ARRAY should not be null", intArray);
        Integer[] actualIntArray = (Integer[]) intArray.getArray();
        assertArrayEquals("INT ARRAY should match", new Integer[]{1, 2, 3, 4, 5}, actualIntArray);
        System.out.println("✓ INT ARRAY verified");

        java.sql.Array textArray = rs.getArray("text_array_col");
        assertNotNull("TEXT ARRAY should not be null", textArray);
        String[] actualTextArray = (String[]) textArray.getArray();
        assertArrayEquals("TEXT ARRAY should match", new String[]{"apple", "banana", "cherry"}, actualTextArray);
        System.out.println("✓ TEXT ARRAY verified");

        System.out.println("✓ All data verified successfully!");
      }

      // Verify column types in table metadata
      System.out.println("\n=== Table Structure ===");
      try (ResultSet rs = conn.getMetaData().getColumns(null, null, TABLE_NAME, null)) {
        while (rs.next()) {
          String columnName = rs.getString("COLUMN_NAME");
          String columnType = rs.getString("TYPE_NAME");
          int columnSize = rs.getInt("COLUMN_SIZE");
          int nullable = rs.getInt("NULLABLE");
          String isNullable = nullable == DatabaseMetaData.columnNullable ? "NULL" : "NOT NULL";

          System.out.println(String.format("  %-25s %-20s Size: %-6d %s",
              columnName, columnType, columnSize, isNullable));
        }
      }
      System.out.println("======================\n");
    }
  }

  @Test
  public void testNullableColumns() throws Exception {
    // Create Kafka topic
    String topicName = "nullable_test_topic";
    String tableName = "sink_" + topicName;
    connect.kafka().createTopic(topicName, 1);

    // Create schema with optional (nullable) fields
    Schema schema = SchemaBuilder.struct().name("com.example.NullableTest")
        .field("id", Schema.INT32_SCHEMA)  // non-nullable
        .field("optional_string", Schema.OPTIONAL_STRING_SCHEMA)  // nullable
        .field("optional_int", Schema.OPTIONAL_INT32_SCHEMA)  // nullable
        .field("optional_boolean", Schema.OPTIONAL_BOOLEAN_SCHEMA)  // nullable
        .field("optional_double", Schema.OPTIONAL_FLOAT64_SCHEMA)  // nullable
        .field("optional_bytes", Schema.OPTIONAL_BYTES_SCHEMA)  // nullable
        .build();

    // Create first record with all fields populated
    Struct struct1 = new Struct(schema)
        .put("id", 1)
        .put("optional_string", "test value")
        .put("optional_int", 42)
        .put("optional_boolean", true)
        .put("optional_double", 3.14)
        .put("optional_bytes", new byte[]{0x01, 0x02, 0x03});

    // Create second record with null optional fields
    Struct struct2 = new Struct(schema)
        .put("id", 2)
        .put("optional_string", null)
        .put("optional_int", null)
        .put("optional_boolean", null)
        .put("optional_double", null)
        .put("optional_bytes", null);

    // Produce records
    String kafkaValue1 = new String(jsonConverter.fromConnectData(topicName, schema, struct1));
    String kafkaValue2 = new String(jsonConverter.fromConnectData(topicName, schema, struct2));
    connect.kafka().produce(topicName, null, kafkaValue1);
    connect.kafka().produce(topicName, null, kafkaValue2);
    System.out.println("Produced records with nullable fields to topic: " + topicName);

    // Update props for this test
    Map<String, String> testProps = new HashMap<>(props);
    testProps.put("topics", topicName);

    // Configure and start sink connector
    connect.configureConnector(CONNECTOR_NAME + "_nullable", testProps);
    waitForConnectorToStart(CONNECTOR_NAME + "_nullable", 1);

    // Wait for records to be written to PostgreSQL
    waitForCommittedRecords(
        CONNECTOR_NAME + "_nullable",
        Collections.singleton(topicName),
        2,
        1,
        TimeUnit.MINUTES.toMillis(2)
    );

    System.out.println("Records committed to PostgreSQL, verifying nullable columns...");

    // Verify table structure and data
    try (Connection conn = getConnection(); Statement stmt = conn.createStatement()) {
      // Verify column nullability in metadata
      DatabaseMetaData metaData = conn.getMetaData();
      System.out.println("\n=== Verifying Column Nullability ===");
      try (ResultSet rs = metaData.getColumns(null, null, tableName, null)) {
        while (rs.next()) {
          String columnName = rs.getString("COLUMN_NAME");
          int nullable = rs.getInt("NULLABLE");
          String isNullable = nullable == DatabaseMetaData.columnNullable ? "NULL" : "NOT NULL";
          System.out.println(String.format("  %-25s %s", columnName, isNullable));

          // Verify nullable constraints
          if (columnName.equals("id")) {
            assertEquals("id should be NOT NULL", DatabaseMetaData.columnNoNulls, nullable);
          } else if (columnName.startsWith("optional_")) {
            assertEquals(columnName + " should be nullable", DatabaseMetaData.columnNullable, nullable);
          }
        }
      }
      System.out.println("======================\n");

      // Verify data - first record should have all values
      String query = "SELECT * FROM " + tableName + " WHERE id = 1";
      try (ResultSet rs = stmt.executeQuery(query)) {
        assertTrue("Should have row with id=1", rs.next());
        assertEquals(1, rs.getInt("id"));
        assertEquals("test value", rs.getString("optional_string"));
        assertEquals(42, rs.getInt("optional_int"));
        assertTrue(rs.getBoolean("optional_boolean"));
        assertEquals(3.14, rs.getDouble("optional_double"), 0.001);
        assertArrayEquals(new byte[]{0x01, 0x02, 0x03}, rs.getBytes("optional_bytes"));
        System.out.println("✓ First record with non-null values verified");
      }

      // Verify data - second record should have nulls
      query = "SELECT * FROM " + tableName + " WHERE id = 2";
      try (ResultSet rs = stmt.executeQuery(query)) {
        assertTrue("Should have row with id=2", rs.next());
        assertEquals(2, rs.getInt("id"));
        assertNull("optional_string should be null", rs.getString("optional_string"));
        rs.getInt("optional_int");
        assertTrue("optional_int should be null", rs.wasNull());
        rs.getBoolean("optional_boolean");
        assertTrue("optional_boolean should be null", rs.wasNull());
        rs.getDouble("optional_double");
        assertTrue("optional_double should be null", rs.wasNull());
        assertNull("optional_bytes should be null", rs.getBytes("optional_bytes"));
        System.out.println("✓ Second record with null values verified");
      }
    }

    // Cleanup
    connect.deleteConnector(CONNECTOR_NAME + "_nullable");
    dropTableByName(tableName);
    System.out.println("Nullable columns test completed successfully!");
  }

  @Test
  public void testColumnsWithDefaultValues() throws Exception {
    // Create Kafka topic
    String topicName = "default_value_test_topic";
    String tableName = "sink_" + topicName;
    connect.kafka().createTopic(topicName, 1);

    // Create schema with optional fields that have default values
    Schema schema = SchemaBuilder.struct().name("com.example.DefaultValueTest")
        .field("id", Schema.INT32_SCHEMA)
        .field("string_with_default", SchemaBuilder.string().defaultValue("default_string").optional().build())
        .field("int_with_default", SchemaBuilder.int32().defaultValue(100).optional().build())
        .field("boolean_with_default", SchemaBuilder.bool().defaultValue(false).optional().build())
        .field("double_with_default", SchemaBuilder.float64().defaultValue(99.99).optional().build())
        .build();

    // Create first record with all fields populated
    Struct struct1 = new Struct(schema)
        .put("id", 1)
        .put("string_with_default", "custom value")
        .put("int_with_default", 42)
        .put("boolean_with_default", true)
        .put("double_with_default", 3.14);

    // Create second record without setting optional fields (should use defaults)
    Struct struct2 = new Struct(schema)
        .put("id", 2);
    // Note: We're not setting the optional fields, so Connect will use the schema defaults

    // Produce records
    String kafkaValue1 = new String(jsonConverter.fromConnectData(topicName, schema, struct1));
    String kafkaValue2 = new String(jsonConverter.fromConnectData(topicName, schema, struct2));
    connect.kafka().produce(topicName, null, kafkaValue1);
    connect.kafka().produce(topicName, null, kafkaValue2);
    System.out.println("Produced records with default values to topic: " + topicName);

    // Update props for this test
    Map<String, String> testProps = new HashMap<>(props);
    testProps.put("topics", topicName);

    // Configure and start sink connector
    connect.configureConnector(CONNECTOR_NAME + "_defaults", testProps);
    waitForConnectorToStart(CONNECTOR_NAME + "_defaults", 1);

    // Wait for records to be written to PostgreSQL
    waitForCommittedRecords(
        CONNECTOR_NAME + "_defaults",
        Collections.singleton(topicName),
        2,
        1,
        TimeUnit.MINUTES.toMillis(2)
    );

    System.out.println("Records committed to PostgreSQL, verifying default values...");

    // Verify table structure and data
    try (Connection conn = getConnection(); Statement stmt = conn.createStatement()) {
      // Verify column defaults in metadata
      DatabaseMetaData metaData = conn.getMetaData();
      System.out.println("\n=== Verifying Column Defaults ===");
      try (ResultSet rs = metaData.getColumns(null, null, tableName, null)) {
        while (rs.next()) {
          String columnName = rs.getString("COLUMN_NAME");
          String columnDefault = rs.getString("COLUMN_DEF");
          System.out.println(String.format("  %-25s Default: %s", columnName, columnDefault));
        }
      }
      System.out.println("======================\n");

      // Verify data - first record should have custom values
      String query = "SELECT * FROM " + tableName + " WHERE id = 1";
      try (ResultSet rs = stmt.executeQuery(query)) {
        assertTrue("Should have row with id=1", rs.next());
        assertEquals(1, rs.getInt("id"));
        assertEquals("custom value", rs.getString("string_with_default"));
        assertEquals(42, rs.getInt("int_with_default"));
        assertTrue(rs.getBoolean("boolean_with_default"));
        assertEquals(3.14, rs.getDouble("double_with_default"), 0.001);
        System.out.println("✓ First record with custom values verified");
      }

      // Verify data - second record should have default values from schema
      query = "SELECT * FROM " + tableName + " WHERE id = 2";
      try (ResultSet rs = stmt.executeQuery(query)) {
        assertTrue("Should have row with id=2", rs.next());
        assertEquals(2, rs.getInt("id"));
        assertEquals("default_string", rs.getString("string_with_default"));
        assertEquals(100, rs.getInt("int_with_default"));
        assertEquals(false, rs.getBoolean("boolean_with_default"));
        assertEquals(99.99, rs.getDouble("double_with_default"), 0.001);
        System.out.println("✓ Second record with default values verified");
      }
    }

    // Cleanup
    connect.deleteConnector(CONNECTOR_NAME + "_defaults");
    dropTableByName(tableName);
    System.out.println("Default values test completed successfully!");
  }

  @Test
  public void testMixedNullableAndDefaultValues() throws Exception {
    // Create Kafka topic
    String topicName = "mixed_nullable_default_topic";
    String tableName = "sink_" + topicName;
    connect.kafka().createTopic(topicName, 1);

    // Create schema with mix of nullable and default value fields
    Schema schema = SchemaBuilder.struct().name("com.example.MixedTest")
        .field("id", Schema.INT32_SCHEMA)  // non-nullable, no default
        .field("nullable_no_default", Schema.OPTIONAL_STRING_SCHEMA)  // nullable, no default
        .field("nullable_with_default", SchemaBuilder.string().defaultValue("default_text").optional().build())  // nullable with default
        .field("required_field", Schema.STRING_SCHEMA)  // non-nullable, no default
        .build();

    // Record with all fields
    Struct struct1 = new Struct(schema)
        .put("id", 1)
        .put("nullable_no_default", "value1")
        .put("nullable_with_default", "custom1")
        .put("required_field", "required1");

    // Record with null nullable field and default used
    Struct struct2 = new Struct(schema)
        .put("id", 2)
        .put("nullable_no_default", null)
        // nullable_with_default will use schema default
        .put("required_field", "required2");

    // Produce records
    String kafkaValue1 = new String(jsonConverter.fromConnectData(topicName, schema, struct1));
    String kafkaValue2 = new String(jsonConverter.fromConnectData(topicName, schema, struct2));
    connect.kafka().produce(topicName, null, kafkaValue1);
    connect.kafka().produce(topicName, null, kafkaValue2);
    System.out.println("Produced mixed nullable/default records to topic: " + topicName);

    // Update props for this test
    Map<String, String> testProps = new HashMap<>(props);
    testProps.put("topics", topicName);

    // Configure and start sink connector
    connect.configureConnector(CONNECTOR_NAME + "_mixed", testProps);
    waitForConnectorToStart(CONNECTOR_NAME + "_mixed", 1);

    // Wait for records to be written to PostgreSQL
    waitForCommittedRecords(
        CONNECTOR_NAME + "_mixed",
        Collections.singleton(topicName),
        2,
        1,
        TimeUnit.MINUTES.toMillis(2)
    );

    System.out.println("Records committed to PostgreSQL, verifying mixed nullable/default behavior...");

    // Verify table structure and data
    try (Connection conn = getConnection(); Statement stmt = conn.createStatement()) {
      // Verify first record
      String query = "SELECT * FROM " + tableName + " WHERE id = 1";
      try (ResultSet rs = stmt.executeQuery(query)) {
        assertTrue("Should have row with id=1", rs.next());
        assertEquals("value1", rs.getString("nullable_no_default"));
        assertEquals("custom1", rs.getString("nullable_with_default"));
        assertEquals("required1", rs.getString("required_field"));
        System.out.println("✓ First record verified");
      }

      // Verify second record
      query = "SELECT * FROM " + tableName + " WHERE id = 2";
      try (ResultSet rs = stmt.executeQuery(query)) {
        assertTrue("Should have row with id=2", rs.next());
        assertNull("nullable_no_default should be null", rs.getString("nullable_no_default"));
        assertEquals("default_text", rs.getString("nullable_with_default"));
        assertEquals("required2", rs.getString("required_field"));
        System.out.println("✓ Second record verified with null and default values");
      }
    }

    // Cleanup
    connect.deleteConnector(CONNECTOR_NAME + "_mixed");
    dropTableByName(tableName);
    System.out.println("Mixed nullable/default test completed successfully!");
  }

  @Test
  public void testPostgresArrayTypesWithNullable() throws Exception {
    // Create Kafka topic
    String topicName = "array_nullable_topic";
    String tableName = "sink_" + topicName;
    connect.kafka().createTopic(topicName, 1);

    // Create schema with optional array fields
    Schema schema = SchemaBuilder.struct().name("com.example.ArrayNullableTest")
        .field("id", Schema.INT32_SCHEMA)
        .field("int_array", SchemaBuilder.array(Schema.INT32_SCHEMA).optional().build())
        .field("text_array", SchemaBuilder.array(Schema.STRING_SCHEMA).optional().build())
        .build();

    // Record with array values
    Struct struct1 = new Struct(schema)
        .put("id", 1)
        .put("int_array", Arrays.asList(10, 20, 30))
        .put("text_array", Arrays.asList("one", "two", "three"));

    // Record with null arrays
    Struct struct2 = new Struct(schema)
        .put("id", 2)
        .put("int_array", null)
        .put("text_array", null);

    // Produce records
    String kafkaValue1 = new String(jsonConverter.fromConnectData(topicName, schema, struct1));
    String kafkaValue2 = new String(jsonConverter.fromConnectData(topicName, schema, struct2));
    connect.kafka().produce(topicName, null, kafkaValue1);
    connect.kafka().produce(topicName, null, kafkaValue2);
    System.out.println("Produced records with nullable arrays to topic: " + topicName);

    // Update props for this test
    Map<String, String> testProps = new HashMap<>(props);
    testProps.put("topics", topicName);

    // Configure and start sink connector
    connect.configureConnector(CONNECTOR_NAME + "_array_nullable", testProps);
    waitForConnectorToStart(CONNECTOR_NAME + "_array_nullable", 1);

    // Wait for records to be written to PostgreSQL
    waitForCommittedRecords(
        CONNECTOR_NAME + "_array_nullable",
        Collections.singleton(topicName),
        2,
        1,
        TimeUnit.MINUTES.toMillis(2)
    );

    System.out.println("Records committed to PostgreSQL, verifying nullable arrays...");

    // Verify data
    try (Connection conn = getConnection(); Statement stmt = conn.createStatement()) {
      // First record with array values
      String query = "SELECT * FROM " + tableName + " WHERE id = 1";
      try (ResultSet rs = stmt.executeQuery(query)) {
        assertTrue("Should have row with id=1", rs.next());
        java.sql.Array intArray = rs.getArray("int_array");
        assertNotNull("int_array should not be null", intArray);
        Integer[] actualIntArray = (Integer[]) intArray.getArray();
        assertArrayEquals(new Integer[]{10, 20, 30}, actualIntArray);

        java.sql.Array textArray = rs.getArray("text_array");
        assertNotNull("text_array should not be null", textArray);
        String[] actualTextArray = (String[]) textArray.getArray();
        assertArrayEquals(new String[]{"one", "two", "three"}, actualTextArray);
        System.out.println("✓ First record with array values verified");
      }

      // Second record with null arrays
      query = "SELECT * FROM " + tableName + " WHERE id = 2";
      try (ResultSet rs = stmt.executeQuery(query)) {
        assertTrue("Should have row with id=2", rs.next());
        assertNull("int_array should be null", rs.getArray("int_array"));
        assertNull("text_array should be null", rs.getArray("text_array"));
        System.out.println("✓ Second record with null arrays verified");
      }
    }

    // Cleanup
    connect.deleteConnector(CONNECTOR_NAME + "_array_nullable");
    dropTableByName(tableName);
    System.out.println("Array nullable test completed successfully!");
  }

  private void dropTableByName(String tableName) {
    try (Connection conn = getConnection(); Statement stmt = conn.createStatement()) {
      stmt.execute("DROP TABLE IF EXISTS " + tableName);
      System.out.println("Dropped table: " + tableName);
    } catch (SQLException e) {
      System.out.println("Failed to drop table " + tableName + ": " + e.getMessage());
    }
  }
}

