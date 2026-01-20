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
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.json.JsonConverter;
import org.apache.kafka.connect.runtime.ConnectorConfig;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

/**
 * Abstract base class for JDBC sink connector integration tests.
 * Provides common test logic for all important sink connector features:
 *
 * <h3>Test Coverage:</h3>
 * <ul>
 *   <li><b>testInsertMode()</b> - Tests insert.mode=insert configuration</li>
 *   <li><b>testUpsertMode()</b> - Tests insert.mode=upsert configuration</li>
 *   <li><b>testUpdateMode()</b> - Tests insert.mode=update configuration</li>
 *   <li><b>testAutoCreateTrue()</b> - Tests auto.create=true (automatic table creation)</li>
 *   <li><b>testAutoCreateFalse()</b> - Tests auto.create=false (no auto creation)</li>
 *   <li><b>testAutoEvolveTrue()</b> - Tests auto.evolve=true (automatic schema evolution)</li>
 *   <li><b>testAutoEvolveFalse()</b> - Tests auto.evolve=false (no auto evolution)</li>
 *   <li><b>testPkModeKafka()</b> - Tests pk.mode=kafka (Kafka coordinates as primary key)</li>
 *   <li><b>testFieldsWhitelist()</b> - Tests fields.whitelist configuration</li>
 *   <li><b>testDeleteEnabled()</b> - Tests delete.enabled configuration (tombstone records)</li>
 *   <li><b>testTimestampFieldsList()</b> - Tests timestamp.fields.list configuration</li>
 * </ul>
 */
@Category(IntegrationTest.class)
public abstract class AbstractJdbcSinkConnectorIT extends BaseConnectorIT {

  private static final Logger log = LoggerFactory.getLogger(AbstractJdbcSinkConnectorIT.class);

  protected static final long COMMIT_WAIT_MS = TimeUnit.SECONDS.toMillis(30);

  // Test table and topic names -- same name for simplicity.
  protected static final String TEST_TABLE_NAME = "test_table";
  protected static final String TEST_TOPIC_NAME = "test_table";

  protected Map<String, String> props;
  protected static Connection connection;
  protected JsonConverter jsonConverter;

  @Before
  public void setup() throws SQLException {
    startConnect();

    jsonConverter = jsonConverter();

    // Base connector properties
    props = new HashMap<>();
    props.put(ConnectorConfig.CONNECTOR_CLASS_CONFIG, getSinkConnectorClass().getName());
    props.put(ConnectorConfig.NAME_CONFIG, getConnectorName());
    props.put(ConnectorConfig.TASKS_MAX_CONFIG, "1");
    props.put(ConnectorConfig.KEY_CONVERTER_CLASS_CONFIG, "org.apache.kafka.connect.json.JsonConverter");
    props.put(ConnectorConfig.VALUE_CONVERTER_CLASS_CONFIG, "org.apache.kafka.connect.json.JsonConverter");
    props.put(JdbcSinkConfig.CONNECTION_URL, getDatabaseConfig().getJdbcUrl());
    props.put(JdbcSinkConfig.CONNECTION_USER, getDatabaseConfig().getUsername());
    props.put(JdbcSinkConfig.CONNECTION_PASSWORD, getDatabaseConfig().getPassword());
    props.put(JdbcSinkConfig.DIALECT_NAME_CONFIG, getDatabaseConfig().getDialectName());
    props.put("topics", TEST_TOPIC_NAME);
    props.put("confluent.topic.bootstrap.servers", connect.kafka().bootstrapServers());
    props.put("confluent.topic.replication.factor", "1");

    // Create Kafka topic
    connect.kafka().createTopic(TEST_TOPIC_NAME, 1);
  }

  @After
  public void tearDown() throws SQLException {
    stopConnect();
    if (connection != null) {
      // Drop all test tables to ensure clean state for next test
      try (Statement stmt = connection.createStatement()) {
        dropTableIfExists(TEST_TABLE_NAME);
        dropTableIfExists("auto_created_table");
        dropTableIfExists("evolve_test_table");
        dropTableIfExists("timestamp_test_table");
        dropTableIfExists("kafka_pk_table");
        dropTableIfExists("whitelist_table");
        dropTableIfExists("delete_test_table");
      } catch (SQLException e) {
        log.error("Error dropping tables", e);
      }
      // Don't close connection here - it's shared across tests
    }
  }

  @Test
  public void testInsertMode() throws Exception {
    // Create table with primary key
    String tableName = normalizeIdentifier(TEST_TABLE_NAME);
    createTableWithPrimaryKey(tableName);

    // Configure connector for insert mode
    props.put(JdbcSinkConfig.INSERT_MODE, "insert");
    props.put(JdbcSinkConfig.PK_MODE, "record_value");
    props.put(JdbcSinkConfig.PK_FIELDS, normalizeIdentifier("id"));
    props.put(JdbcSinkConfig.AUTO_CREATE, "false");

    // Start connector
    connect.configureConnector(getConnectorName(), props);
    waitForConnectorToStart(getConnectorName(), 1);

    // Produce records to Kafka
    Schema schema = SchemaBuilder.struct()
        .field("id", Schema.INT32_SCHEMA)
        .field("name", Schema.STRING_SCHEMA)
        .field("value", Schema.INT32_SCHEMA)
        .build();

    Struct record1 = new Struct(schema)
        .put("id", 1)
        .put("name", "Alice")
        .put("value", 100);

    Struct record2 = new Struct(schema)
        .put("id", 2)
        .put("name", "Bob")
        .put("value", 200);

    produceRecord(schema, record1);
    produceRecord(schema, record2);

    // Wait for records to be committed
    waitForCommittedRecords(getConnectorName(), Collections.singleton(TEST_TOPIC_NAME), 2, 1, COMMIT_WAIT_MS);

    // Verify records were inserted
    try (Statement stmt = connection.createStatement()) {
      try (ResultSet rs = stmt.executeQuery("SELECT * FROM " + tableName + " ORDER BY " + normalizeIdentifier("id"))) {
        assertTrue("Should have first record", rs.next());
        assertEquals(1, rs.getInt(normalizeIdentifier("id")));
        assertEquals("Alice", rs.getString(normalizeIdentifier("name")));
        assertEquals(100, rs.getInt(normalizeIdentifier("value")));

        assertTrue("Should have second record", rs.next());
        assertEquals(2, rs.getInt(normalizeIdentifier("id")));
        assertEquals("Bob", rs.getString(normalizeIdentifier("name")));
        assertEquals(200, rs.getInt(normalizeIdentifier("value")));

        assertFalse("Should only have 2 records", rs.next());
      }
    }
  }

  @Test
  public void testUpsertMode() throws Exception {
    // Create table with primary key
    String tableName = normalizeIdentifier(TEST_TABLE_NAME);
    createTableWithPrimaryKey(tableName);

    // Configure connector for upsert mode
    props.put(JdbcSinkConfig.INSERT_MODE, "upsert");
    props.put(JdbcSinkConfig.PK_MODE, "record_value");
    props.put(JdbcSinkConfig.PK_FIELDS, normalizeIdentifier("id"));
    props.put(JdbcSinkConfig.AUTO_CREATE, "false");

    // Start connector
    connect.configureConnector(getConnectorName(), props);
    waitForConnectorToStart(getConnectorName(), 1);

    // Produce initial records
    Schema schema = SchemaBuilder.struct()
        .field("id", Schema.INT32_SCHEMA)
        .field("name", Schema.STRING_SCHEMA)
        .field("value", Schema.INT32_SCHEMA)
        .build();

    Struct record1 = new Struct(schema)
        .put("id", 1)
        .put("name", "Alice")
        .put("value", 100);

    produceRecord(schema, record1);

    waitForCommittedRecords(getConnectorName(), Collections.singleton(TEST_TOPIC_NAME), 1, 1, COMMIT_WAIT_MS);

    // Verify initial insert
    try (Statement stmt = connection.createStatement()) {
      try (ResultSet rs = stmt.executeQuery("SELECT * FROM " + tableName + " WHERE " + normalizeIdentifier("id") + " = 1")) {
        assertTrue("Should have first record", rs.next());
        assertEquals("Alice", rs.getString(normalizeIdentifier("name")));
        assertEquals(100, rs.getInt(normalizeIdentifier("value")));
      }
    }

    // Produce updated record with same primary key
    Struct record1Updated = new Struct(schema)
        .put("id", 1)
        .put("name", "Alice Updated")
        .put("value", 150);

    produceRecord(schema, record1Updated);

    waitForCommittedRecords(getConnectorName(), Collections.singleton(TEST_TOPIC_NAME), 2, 1, COMMIT_WAIT_MS);

    // Verify upsert updated the existing record
    try (Statement stmt = connection.createStatement()) {
      try (ResultSet rs = stmt.executeQuery("SELECT * FROM " + tableName + " WHERE " + normalizeIdentifier("id") + " = 1")) {
        assertTrue("Should have updated record", rs.next());
        assertEquals("Alice Updated", rs.getString(normalizeIdentifier("name")));
        assertEquals(150, rs.getInt(normalizeIdentifier("value")));
        assertFalse("Should only have one record with id=1", rs.next());
      }
    }
  }

  @Test
  public void testUpdateMode() throws Exception {
    // Create table with primary key and insert initial data
    String tableName = normalizeIdentifier(TEST_TABLE_NAME);
    createTableWithPrimaryKey(tableName);

    // Insert initial data directly
    try (Statement stmt = connection.createStatement()) {
      stmt.execute(String.format(
          "INSERT INTO %s (%s, %s, %s) VALUES (1, 'Alice', 100)",
          tableName,
          normalizeIdentifier("id"),
          normalizeIdentifier("name"),
          normalizeIdentifier("value")
      ));
    }

    // Configure connector for update mode
    props.put(JdbcSinkConfig.INSERT_MODE, "update");
    props.put(JdbcSinkConfig.PK_MODE, "record_value");
    props.put(JdbcSinkConfig.PK_FIELDS, normalizeIdentifier("id"));
    props.put(JdbcSinkConfig.AUTO_CREATE, "false");

    // Start connector
    connect.configureConnector(getConnectorName(), props);
    waitForConnectorToStart(getConnectorName(), 1);

    // Produce update for existing record
    Schema schema = SchemaBuilder.struct()
        .field("id", Schema.INT32_SCHEMA)
        .field("name", Schema.STRING_SCHEMA)
        .field("value", Schema.INT32_SCHEMA)
        .build();

    Struct updatedRecord = new Struct(schema)
        .put("id", 1)
        .put("name", "Alice Modified")
        .put("value", 200);

    produceRecord(schema, updatedRecord);

    waitForCommittedRecords(getConnectorName(), Collections.singleton(TEST_TOPIC_NAME), 1, 1, COMMIT_WAIT_MS);

    // Verify record was updated
    try (Statement stmt = connection.createStatement()) {
      try (ResultSet rs = stmt.executeQuery("SELECT * FROM " + tableName + " WHERE " + normalizeIdentifier("id") + " = 1")) {
        assertTrue("Should have updated record", rs.next());
        assertEquals("Alice Modified", rs.getString(normalizeIdentifier("name")));
        assertEquals(200, rs.getInt(normalizeIdentifier("value")));
        assertFalse("Should only have one record", rs.next());
      }
    }
  }

  @Test
  public void testAutoCreateTrue() throws Exception {
    String tableName = normalizeIdentifier("auto_created_table");

    // Configure connector with auto.create=true
    props.put(JdbcSinkConfig.INSERT_MODE, "insert");
    props.put(JdbcSinkConfig.PK_MODE, "none");
    props.put(JdbcSinkConfig.AUTO_CREATE, "true");
    props.put("topics", "auto_created_table");

    // Create separate topic for this test
    connect.kafka().createTopic("auto_created_table", 1);

    // Start connector
    connect.configureConnector(getConnectorName(), props);
    waitForConnectorToStart(getConnectorName(), 1);

    // Produce record - table should be auto-created
    Schema schema = SchemaBuilder.struct()
        .field("id", Schema.INT32_SCHEMA)
        .field("name", Schema.STRING_SCHEMA)
        .field("amount", Schema.FLOAT64_SCHEMA)
        .build();

    Struct record = new Struct(schema)
        .put("id", 1)
        .put("name", "Test")
        .put("amount", 99.99);

    String kafkaValue = new String(jsonConverter.fromConnectData("auto_created_table", schema, record));
    connect.kafka().produce("auto_created_table", null, kafkaValue);

    waitForCommittedRecords(getConnectorName(), Collections.singleton("auto_created_table"), 1, 1, COMMIT_WAIT_MS);

    // Verify table was created and record inserted
    try (Statement stmt = connection.createStatement()) {
      try (ResultSet rs = stmt.executeQuery("SELECT * FROM " + tableName)) {
        assertTrue("Should have record in auto-created table", rs.next());
        assertEquals(1, rs.getInt(normalizeIdentifier("id")));
        assertEquals("Test", rs.getString(normalizeIdentifier("name")));
        assertEquals(99.99, rs.getDouble(normalizeIdentifier("amount")), 0.01);
      }
    }
  }

  @Test
  public void testAutoCreateFalse() throws Exception {
    String tableName = normalizeIdentifier(TEST_TABLE_NAME);

    // Don't create the table beforehand

    // Configure connector with auto.create=false
    props.put(JdbcSinkConfig.INSERT_MODE, "insert");
    props.put(JdbcSinkConfig.PK_MODE, "none");
    props.put(JdbcSinkConfig.AUTO_CREATE, "false");

    // Start connector
    connect.configureConnector(getConnectorName(), props);
    waitForConnectorToStart(getConnectorName(), 1);

    // Produce record - should fail because table doesn't exist
    Schema schema = SchemaBuilder.struct()
        .field("id", Schema.INT32_SCHEMA)
        .field("name", Schema.STRING_SCHEMA)
        .build();

    Struct record = new Struct(schema)
        .put("id", 1)
        .put("name", "Test");

    produceRecord(schema, record);

    // Give it some time to attempt processing
    Thread.sleep(5000);

    // Verify table was NOT created
    boolean tableExists = checkTableExists(tableName);
    assertFalse("Table should not be auto-created when auto.create=false", tableExists);
  }

  @Test
  public void testAutoEvolveTrue() throws Exception {
    String tableName = normalizeIdentifier("evolve_test_table");

    // Create table with initial schema (only 2 columns)
    createTableForEvolveTest(tableName);

    // Configure connector with auto.evolve=true
    props.put(JdbcSinkConfig.INSERT_MODE, "insert");
    props.put(JdbcSinkConfig.PK_MODE, "none");
    props.put(JdbcSinkConfig.AUTO_CREATE, "false");
    props.put(JdbcSinkConfig.AUTO_EVOLVE, "true");
    props.put("topics", "evolve_test_table");

    // Create separate topic for this test
    connect.kafka().createTopic("evolve_test_table", 1);

    // Start connector
    connect.configureConnector(getConnectorName(), props);
    waitForConnectorToStart(getConnectorName(), 1);

    // Produce record with additional field
    Schema schema = SchemaBuilder.struct()
        .field("id", Schema.INT32_SCHEMA)
        .field("name", Schema.STRING_SCHEMA)
        .field("email", SchemaBuilder.string().optional().build()) // New field
        .build();

    Struct record = new Struct(schema)
        .put("id", 1)
        .put("name", "Alice")
        .put("email", "alice@example.com");

    String kafkaValue = new String(jsonConverter.fromConnectData("evolve_test_table", schema, record));
    connect.kafka().produce("evolve_test_table", null, kafkaValue);

    waitForCommittedRecords(getConnectorName(), Collections.singleton("evolve_test_table"), 1, 1, COMMIT_WAIT_MS);

    // Verify new column was added and record inserted
    try (Statement stmt = connection.createStatement()) {
      try (ResultSet rs = stmt.executeQuery("SELECT * FROM " + tableName)) {
        assertTrue("Should have record", rs.next());
        assertEquals(1, rs.getInt(normalizeIdentifier("id")));
        assertEquals("Alice", rs.getString(normalizeIdentifier("name")));
        assertEquals("alice@example.com", rs.getString(normalizeIdentifier("email")));
      }
    }
  }

  @Test
  public void testAutoEvolveFalse() throws Exception {
    String tableName = normalizeIdentifier("evolve_test_table");

    // Create table with initial schema (only 2 columns)
    createTableForEvolveTest(tableName);

    // Configure connector with auto.evolve=false
    props.put(JdbcSinkConfig.INSERT_MODE, "insert");
    props.put(JdbcSinkConfig.PK_MODE, "none");
    props.put(JdbcSinkConfig.AUTO_CREATE, "false");
    props.put(JdbcSinkConfig.AUTO_EVOLVE, "false");
    props.put("topics", "evolve_test_table");
    props.put(JdbcSinkConfig.MAX_RETRIES, "3");

    // Create separate topic for this test
    connect.kafka().createTopic("evolve_test_table", 1);

    // Start connector
    connect.configureConnector(getConnectorName(), props);
    waitForConnectorToStart(getConnectorName(), 1);

    // Produce record with additional field
    Schema schema = SchemaBuilder.struct()
        .field("id", Schema.INT32_SCHEMA)
        .field("name", Schema.STRING_SCHEMA)
        .field("extra_field", SchemaBuilder.string().optional().build()) // Field not in table
        .build();

    Struct record = new Struct(schema)
        .put("id", 1)
        .put("name", "Alice")
        .put("extra_field", "should_fail");

    String kafkaValue = new String(jsonConverter.fromConnectData("evolve_test_table", schema, record));
    connect.kafka().produce("evolve_test_table", null, kafkaValue);

    // Give it time to fail
    Thread.sleep(8000);

    // Verify the extra column was NOT added
    boolean hasExtraColumn = checkColumnExists(tableName, normalizeIdentifier("extra_field"));
    assertFalse("Extra column should not be added when auto.evolve=false", hasExtraColumn);
  }

  @Test
  public void testPkModeKafka() throws Exception {
    String tableName = normalizeIdentifier("kafka_pk_table");

    // Create table with Kafka coordinates as primary key
    createTableForKafkaPkTest(tableName);

    // Configure connector with pk.mode=kafka
    props.put(JdbcSinkConfig.INSERT_MODE, "insert");
    props.put(JdbcSinkConfig.PK_MODE, "kafka");
    props.put(JdbcSinkConfig.AUTO_CREATE, "false");
    props.put("topics", "kafka_pk_table");

    // Create separate topic for this test
    connect.kafka().createTopic("kafka_pk_table", 1);

    // Start connector
    connect.configureConnector(getConnectorName(), props);
    waitForConnectorToStart(getConnectorName(), 1);

    // Produce records
    Schema schema = SchemaBuilder.struct()
        .field("id", Schema.INT32_SCHEMA)
        .field("name", Schema.STRING_SCHEMA)
        .build();

    Struct record = new Struct(schema)
        .put("id", 1)
        .put("name", "TestKafkaPK");

    String kafkaValue = new String(jsonConverter.fromConnectData("kafka_pk_table", schema, record));
    connect.kafka().produce("kafka_pk_table", null, kafkaValue);

    waitForCommittedRecords(getConnectorName(), Collections.singleton("kafka_pk_table"), 1, 1, COMMIT_WAIT_MS);

    // Verify Kafka coordinates were stored as primary key
    try (Statement stmt = connection.createStatement()) {
      try (ResultSet rs = stmt.executeQuery("SELECT * FROM " + tableName)) {
        assertTrue("Should have record", rs.next());
        assertEquals(1, rs.getInt(normalizeIdentifier("id")));
        assertEquals("TestKafkaPK", rs.getString(normalizeIdentifier("name")));

        // Verify Kafka coordinate columns exist and have values
        assertEquals("kafka_pk_table", rs.getString(normalizeIdentifier("__connect_topic")));
        assertEquals(0, rs.getInt(normalizeIdentifier("__connect_partition")));
        assertTrue("Offset should be >= 0", rs.getLong(normalizeIdentifier("__connect_offset")) >= 0);
      }
    }
  }

  @Test
  public void testFieldsWhitelist() throws Exception {
    String tableName = normalizeIdentifier("whitelist_table");

    // Create table with all potential columns
    createTableForFieldsWhitelistTest(tableName);

    // Configure connector with fields.whitelist - only allow id and name
    props.put(JdbcSinkConfig.INSERT_MODE, "insert");
    props.put(JdbcSinkConfig.PK_MODE, "none");
    props.put(JdbcSinkConfig.AUTO_CREATE, "false");
    props.put(JdbcSinkConfig.FIELDS_WHITELIST, normalizeIdentifier("id") + "," + normalizeIdentifier("name"));
    props.put("topics", "whitelist_table");

    // Create separate topic for this test
    connect.kafka().createTopic("whitelist_table", 1);

    // Start connector
    connect.configureConnector(getConnectorName(), props);
    waitForConnectorToStart(getConnectorName(), 1);

    // Produce record with more fields than whitelist
    Schema schema = SchemaBuilder.struct()
        .field("id", Schema.INT32_SCHEMA)
        .field("name", Schema.STRING_SCHEMA)
        .field("email", Schema.STRING_SCHEMA)
        .field("age", Schema.INT32_SCHEMA)
        .build();

    Struct record = new Struct(schema)
        .put("id", 1)
        .put("name", "Alice")
        .put("email", "alice@example.com")
        .put("age", 30);

    String kafkaValue = new String(jsonConverter.fromConnectData("whitelist_table", schema, record));
    connect.kafka().produce("whitelist_table", null, kafkaValue);

    waitForCommittedRecords(getConnectorName(), Collections.singleton("whitelist_table"), 1, 1, COMMIT_WAIT_MS);

    // Verify only whitelisted fields were inserted
    try (Statement stmt = connection.createStatement()) {
      try (ResultSet rs = stmt.executeQuery("SELECT * FROM " + tableName)) {
        assertTrue("Should have record", rs.next());
        assertEquals(1, rs.getInt(normalizeIdentifier("id")));
        assertEquals("Alice", rs.getString(normalizeIdentifier("name")));

        // Verify non-whitelisted fields are null
        assertNull("Email should be null", rs.getObject(normalizeIdentifier("email")));
        assertNull("Age should be null", rs.getObject(normalizeIdentifier("age")));
      }
    }
  }

  @Test
  public void testDeleteEnabled() throws Exception {
    String tableName = normalizeIdentifier("delete_test_table");

    // Ensure clean state - drop table if it exists from previous runs
    dropTableIfExists(tableName);

    // Create table for delete test
    createTableForDeleteTest(tableName);

    // Configure connector with delete.enabled=true
    // Note: pk.mode=record_key with delete.enabled requires the key to identify the record
    // When using a struct key, pk.fields can be omitted (uses all key fields) or specified
    props.put(JdbcSinkConfig.INSERT_MODE, "upsert");
    props.put(JdbcSinkConfig.PK_MODE, "record_key");
    // pk.fields not needed when key schema has matching field names
    props.put(JdbcSinkConfig.DELETE_ENABLED, "true");
    props.put(JdbcSinkConfig.AUTO_CREATE, "false");
    props.put("topics", "delete_test_table");

    // Create separate topic for this test
    connect.kafka().createTopic("delete_test_table", 1);

    // Start connector
    connect.configureConnector(getConnectorName(), props);
    waitForConnectorToStart(getConnectorName(), 1);

    // First, insert a record using a struct key with JsonConverter
    // Key schema contains only the primary key field(s)
    // Value schema contains only non-primary key fields
    Schema keySchema = SchemaBuilder.struct()
        .field("id", Schema.INT32_SCHEMA)
        .build();

    Schema valueSchema = SchemaBuilder.struct()
        .field("name", Schema.STRING_SCHEMA)
        .build();

    Struct key = new Struct(keySchema)
        .put("id", 1);

    Struct value = new Struct(valueSchema)
        .put("name", "ToBeDeleted");

    // Serialize both key and value with JsonConverter
    String kafkaKey = new String(jsonConverter.fromConnectData("delete_test_table", keySchema, key));
    String kafkaValue = new String(jsonConverter.fromConnectData("delete_test_table", valueSchema, value));
    connect.kafka().produce("delete_test_table", kafkaKey, kafkaValue);

    waitForCommittedRecords(getConnectorName(), Collections.singleton("delete_test_table"), 1, 1, COMMIT_WAIT_MS);

    // Verify record was inserted
    try (Statement stmt = connection.createStatement()) {
      try (ResultSet rs = stmt.executeQuery("SELECT COUNT(*) FROM " + tableName + " WHERE " + normalizeIdentifier("id") + " = 1")) {
        assertTrue(rs.next());
        assertEquals("Should have 1 record", 1, rs.getInt(1));
      }
    }

    // Now send a tombstone (null value) with the same key to delete the record
    connect.kafka().produce("delete_test_table", kafkaKey, null);

    waitForCommittedRecords(getConnectorName(), Collections.singleton("delete_test_table"), 2, 1, COMMIT_WAIT_MS);

    // Verify record was deleted
    try (Statement stmt = connection.createStatement()) {
      try (ResultSet rs = stmt.executeQuery("SELECT COUNT(*) FROM " + tableName + " WHERE " + normalizeIdentifier("id") + " = 1")) {
        assertTrue(rs.next());
        assertEquals("Record should be deleted", 0, rs.getInt(1));
      }
    }
  }

  @Test
  public void testTimestampFieldsList() throws Exception {
    String tableName = normalizeIdentifier("timestamp_test_table");

    // Create table for timestamp test
    createTableForTimestampTest(tableName);

    // Configure connector with timestamp.fields.list
    props.put(JdbcSinkConfig.INSERT_MODE, "insert");
    props.put(JdbcSinkConfig.PK_MODE, "none");
    props.put(JdbcSinkConfig.AUTO_CREATE, "false");
    props.put(JdbcSinkConfig.TIMESTAMP_FIELDS_LIST, normalizeIdentifier("created_at") + "," + normalizeIdentifier("updated_at"));
    props.put("topics", "timestamp_test_table");

    // Create separate topic for this test
    connect.kafka().createTopic("timestamp_test_table", 1);

    // Start connector
    connect.configureConnector(getConnectorName(), props);
    waitForConnectorToStart(getConnectorName(), 1);

    // Produce record with timestamp fields as Long (epoch microseconds)
    // Note: timestamp.fields.list treats INT64 values as microseconds (or nanoseconds based on timestamp.precision.mode)
    Schema schema = SchemaBuilder.struct()
        .field("id", Schema.INT32_SCHEMA)
        .field("name", Schema.STRING_SCHEMA)
        .field("created_at", Schema.INT64_SCHEMA)
        .field("updated_at", Schema.INT64_SCHEMA)
        .build();

    long timestampMicros = System.currentTimeMillis() * 1000; // Convert milliseconds to microseconds
    Struct record = new Struct(schema)
        .put("id", 1)
        .put("name", "TimestampTest")
        .put("created_at", timestampMicros)
        .put("updated_at", timestampMicros + 1000000); // Add 1 second in microseconds

    String kafkaValue = new String(jsonConverter.fromConnectData("timestamp_test_table", schema, record));
    connect.kafka().produce("timestamp_test_table", null, kafkaValue);

    waitForCommittedRecords(getConnectorName(), Collections.singleton("timestamp_test_table"), 1, 1, COMMIT_WAIT_MS);

    // Verify timestamps were stored correctly
    try (Statement stmt = connection.createStatement()) {
      try (ResultSet rs = stmt.executeQuery("SELECT * FROM " + tableName)) {
        assertTrue("Should have record", rs.next());
        assertEquals(1, rs.getInt(normalizeIdentifier("id")));
        assertEquals("TimestampTest", rs.getString(normalizeIdentifier("name")));

        // Verify timestamp fields were converted properly (not null at minimum)
        assertNotNull("created_at should not be null", rs.getObject(normalizeIdentifier("created_at")));
        assertNotNull("updated_at should not be null", rs.getObject(normalizeIdentifier("updated_at")));
      }
    }
  }

  // Abstract methods to be implemented by concrete test classes

  /**
   * Get the database configuration for this connector
   */
  protected abstract DatabaseTestConfig getDatabaseConfig();

  /**
   * Get the sink connector class for this database
   */
  protected abstract Class<?> getSinkConnectorClass();

  /**
   * Get the connector name for this test
   */
  protected abstract String getConnectorName();

  /**
   * Normalize identifier based on database requirements (uppercase/lowercase)
   */
  protected abstract String normalizeIdentifier(String identifier);

  /**
   * Create a table with primary key for testing insert/update/upsert modes.
   *
   * Required schema:
   * - id INTEGER PRIMARY KEY
   * - name VARCHAR(100)
   * - value INTEGER
   *
   * @param tableName the name of the table to create (already normalized for the database)
   */
  protected abstract void createTableWithPrimaryKey(String tableName) throws SQLException;

  /**
   * Create a table for testing auto.evolve feature with initial schema.
   * This table should have only 2 columns initially. The test will add a third column
   * to verify auto.evolve functionality.
   *
   * Required schema:
   * - id INTEGER
   * - name VARCHAR(100)
   *
   * @param tableName the name of the table to create (already normalized for the database)
   */
  protected abstract void createTableForEvolveTest(String tableName) throws SQLException;

  /**
   * Create a table for testing timestamp.fields.list feature.
   * The created_at and updated_at columns will receive INT64 values in microseconds
   * from Kafka records and should be stored as TIMESTAMP types in the database.
   *
   * Required schema:
   * - id INTEGER
   * - name VARCHAR(100)
   * - created_at TIMESTAMP (database-specific timestamp type)
   * - updated_at TIMESTAMP (database-specific timestamp type)
   *
   * @param tableName the name of the table to create (already normalized for the database)
   */
  protected abstract void createTableForTimestampTest(String tableName) throws SQLException;

  /**
   * Create a table for testing pk.mode=kafka feature.
   * This table should have columns for Kafka coordinates as primary key,
   * plus the actual data columns.
   *
   * Required schema:
   * - __connect_topic VARCHAR(255) - part of composite primary key
   * - __connect_partition INTEGER - part of composite primary key
   * - __connect_offset BIGINT - part of composite primary key
   * - id INTEGER
   * - name VARCHAR(100)
   * - PRIMARY KEY (__connect_topic, __connect_partition, __connect_offset)
   *
   * @param tableName the name of the table to create (already normalized for the database)
   */
  protected abstract void createTableForKafkaPkTest(String tableName) throws SQLException;

  /**
   * Create a table for testing fields.whitelist feature.
   * This table should have all columns that might be in the Kafka records,
   * but the test will only insert values into whitelisted columns.
   *
   * Required schema:
   * - id INTEGER
   * - name VARCHAR(100)
   * - email VARCHAR(255) - nullable
   * - age INTEGER - nullable
   *
   * @param tableName the name of the table to create (already normalized for the database)
   */
  protected abstract void createTableForFieldsWhitelistTest(String tableName) throws SQLException;

  /**
   * Create a table for testing delete.enabled feature.
   * This test uses pk.mode=record_key with a struct key containing an integer id.
   *
   * <p>How it works:</p>
   * <ul>
   *   <li>Key schema: {id: INT32} - contains only the primary key field</li>
   *   <li>Value schema: {name: STRING} - contains only non-primary key fields</li>
   *   <li>Kafka record key {id: 1} maps to the id column</li>
   *   <li>Tombstone (null value) with key {id: 1} deletes the record where id=1</li>
   * </ul>
   *
   * Required schema:
   * - id INTEGER PRIMARY KEY (receives integer value from the Kafka record key)
   * - name VARCHAR(100) (from value schema)
   *
   * @param tableName the name of the table to create (already normalized for the database)
   */
  protected abstract void createTableForDeleteTest(String tableName) throws SQLException;

  /**
   * Check if a table exists in the database
   */
  protected abstract boolean checkTableExists(String tableName) throws SQLException;

  /**
   * Check if a column exists in a table
   */
  protected abstract boolean checkColumnExists(String tableName, String columnName) throws SQLException;

  /**
   * Drop table if it exists (for cleanup)
   */
  protected abstract void dropTableIfExists(String tableName) throws SQLException;

  // Helper methods

  protected void produceRecord(Schema schema, Struct struct) {
    String kafkaValue = new String(jsonConverter.fromConnectData(TEST_TOPIC_NAME, schema, struct));
    connect.kafka().produce(TEST_TOPIC_NAME, null, kafkaValue);
  }

  /**
   * Configuration class for database-specific test settings
   */
  public static class DatabaseTestConfig {
    private final String jdbcUrl;
    private final String username;
    private final String password;
    private final String dialectName;

    public DatabaseTestConfig(String jdbcUrl, String username, String password, String dialectName) {
      this.jdbcUrl = jdbcUrl;
      this.username = username;
      this.password = password;
      this.dialectName = dialectName;
    }

    public String getJdbcUrl() {
      return jdbcUrl;
    }

    public String getUsername() {
      return username;
    }

    public String getPassword() {
      return password;
    }

    public String getDialectName() {
      return dialectName;
    }
  }
}


