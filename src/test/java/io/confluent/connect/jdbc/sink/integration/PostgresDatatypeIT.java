/*
 * Copyright 2020 Confluent Inc.
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

import com.fasterxml.jackson.databind.ObjectMapper;

import java.math.BigDecimal;
import java.sql.Array;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Instant;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.List;
import java.util.TreeMap;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import io.confluent.common.utils.IntegrationTest;
import io.confluent.connect.jdbc.data.Json;
import io.confluent.connect.jdbc.data.VariableScaleDecimal;
import io.confluent.connect.jdbc.integration.BaseConnectorIT;
import io.confluent.connect.jdbc.sink.JdbcSinkConfig;
import io.confluent.connect.jdbc.JdbcSourceConnector;
import io.confluent.connect.jdbc.source.JdbcSourceConnectorConfig;
import io.confluent.connect.jdbc.source.JdbcSourceTask;
import io.confluent.connect.jdbc.source.JdbcSourceTaskConfig;

import io.zonky.test.db.postgres.junit.EmbeddedPostgresRules;
import io.zonky.test.db.postgres.junit.SingleInstancePostgresRule;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.connect.data.Date;
import org.apache.kafka.connect.data.Decimal;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.data.Time;
import org.apache.kafka.connect.data.Timestamp;
import org.apache.kafka.connect.json.JsonConverter;
import org.apache.kafka.connect.runtime.ConnectorConfig;
import org.apache.kafka.connect.runtime.errors.ToleranceType;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.storage.StringConverter;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static io.confluent.connect.jdbc.sink.JdbcSinkConfig.MAX_RETRIES;
import static org.apache.kafka.connect.runtime.ConnectorConfig.ERRORS_TOLERANCE_CONFIG;
import static org.apache.kafka.connect.runtime.SinkConnectorConfig.DLQ_TOPIC_NAME_CONFIG;
import static org.apache.kafka.connect.runtime.SinkConnectorConfig.DLQ_TOPIC_REPLICATION_FACTOR_CONFIG;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;


/**
 * Integration tests for writing to Postgres with UUID columns.
 */
@Category(IntegrationTest.class)
public class PostgresDatatypeIT extends BaseConnectorIT {

  private static final Logger LOG = LoggerFactory.getLogger(PostgresDatatypeIT.class);

  @Rule
  public SingleInstancePostgresRule pg = EmbeddedPostgresRules.singleInstance();

  private String tableName;
  private JsonConverter jsonConverter;
  private Map<String, String> props;

  @Before
  public void before() {
    startConnect();
    jsonConverter = jsonConverter();
    props = baseSinkProps();

    tableName = "test";
    String jdbcURL = String
        .format("jdbc:postgresql://localhost:%s/postgres", pg.getEmbeddedPostgres().getPort());
    props.put(JdbcSinkConfig.CONNECTION_URL, jdbcURL);
    props.put(JdbcSinkConfig.CONNECTION_USER, "postgres");
    props.put("pk.mode", "none");
    props.put("topics", tableName);

    // create topic in Kafka
    connect.kafka().createTopic(tableName, 1);
  }

  @After
  public void after() throws SQLException {
    try (Connection c = pg.getEmbeddedPostgres().getPostgresDatabase().getConnection()) {
      try (Statement s = c.createStatement()) {
        s.execute("DROP TABLE IF EXISTS " + tableName);
        s.execute("DROP TABLE IF EXISTS " + SRC_TABLE);
        s.execute("DROP TABLE IF EXISTS " + DST_TABLE);
      }
      LOG.info("Dropped table");
    } finally {
      pg = null;
      stopConnect();
    }
  }

  /**
   * Verifies that even when the connector encounters exceptions that would cause a connection
   * with an invalid transaction, the connector sends only the errant record to the error
   * reporter and establishes a valid transaction for subsequent correct records to be sent to
   * the actual database.
   */
  @Test
  public void testPrimaryKeyConstraintsSendsToErrorReporter() throws Exception {
    props.put(ERRORS_TOLERANCE_CONFIG, ToleranceType.ALL.value());
    props.put(DLQ_TOPIC_NAME_CONFIG, DLQ_TOPIC_NAME);
    props.put(DLQ_TOPIC_REPLICATION_FACTOR_CONFIG, "1");
    props.put(MAX_RETRIES, "0");

    createTableWithPrimaryKey();
    connect.configureConnector("jdbc-sink-connector", props);
    waitForConnectorToStart("jdbc-sink-connector", 1);

    final Schema schema = SchemaBuilder.struct().name("com.example.Person")
        .field("firstname", Schema.STRING_SCHEMA)
        .field("lastname", Schema.STRING_SCHEMA)
        .build();
    final Struct firstStruct = new Struct(schema)
        .put("firstname", "Christina")
        .put("lastname", "Brams");

    produceRecord(schema, firstStruct);
    // Send the same record for a PK collision
    produceRecord(schema, firstStruct);

    // Now, create and send another normal record
    Struct secondStruct = new Struct(schema)
        .put("firstname", "Brams")
        .put("lastname", "Christina");

    produceRecord(schema, secondStruct);

    waitForCommittedRecords("jdbc-sink-connector", Collections.singleton(tableName), 3, 1,
        TimeUnit.MINUTES.toMillis(3));

    ConsumerRecords<byte[], byte[]> records = connect.kafka().consume(1, CONSUME_MAX_DURATION_MS,
        DLQ_TOPIC_NAME);

    assertEquals(1, records.count());
  }

  @Test
  public void testRecordSchemaMoreFieldsThanTableSendsToErrorReporter() throws Exception {
    props.put(ERRORS_TOLERANCE_CONFIG, ToleranceType.ALL.value());
    props.put(DLQ_TOPIC_NAME_CONFIG, DLQ_TOPIC_NAME);
    props.put(DLQ_TOPIC_REPLICATION_FACTOR_CONFIG, "1");

    createTableWithLessFields();
    connect.configureConnector("jdbc-sink-connector", props);
    waitForConnectorToStart("jdbc-sink-connector", 1);

    final Schema schema = SchemaBuilder.struct().name("com.example.Person")
        .field("firstname", Schema.STRING_SCHEMA)
        .field("lastname", Schema.STRING_SCHEMA)
        .field("jsonid", Schema.STRING_SCHEMA)
        .field("userid", Schema.STRING_SCHEMA)
        .build();
    final Struct struct = new Struct(schema)
        .put("firstname", "Christina")
        .put("lastname", "Brams")
        .put("jsonid", "5")
        .put("userid", UUID.randomUUID().toString());

    produceRecord(schema, struct);

    waitForCommittedRecords("jdbc-sink-connector", Collections.singleton(tableName), 1, 1,
        TimeUnit.MINUTES.toMillis(2));

    ConsumerRecords<byte[], byte[]> records = connect.kafka().consume(1, CONSUME_MAX_DURATION_MS,
        DLQ_TOPIC_NAME);

    assertEquals(1, records.count());
  }

  @Test
  public void testWriteToTableWithUuidColumn() throws Exception {
    createTableWithUuidColumns();
    connect.configureConnector("jdbc-sink-connector", props);
    waitForConnectorToStart("jdbc-sink-connector", 1);

    final Schema schema = SchemaBuilder.struct().name("com.example.Person")
                                       .field("firstname", Schema.STRING_SCHEMA)
                                       .field("lastname", Schema.STRING_SCHEMA)
                                       .field("jsonid", Schema.STRING_SCHEMA)
                                       .field("userid", Schema.STRING_SCHEMA)
                                       .build();
    final Struct struct = new Struct(schema)
        .put("firstname", "Christina")
        .put("lastname", "Brams")
        .put("jsonid", "5")
        .put("userid", UUID.randomUUID().toString());

    produceRecord(schema, struct);

    waitForCommittedRecords("jdbc-sink-connector", Collections.singleton(tableName), 1, 1,
        TimeUnit.MINUTES.toMillis(2));

    try (Connection c = pg.getEmbeddedPostgres().getPostgresDatabase().getConnection()) {
      try (Statement s = c.createStatement()) {
        try (ResultSet rs = s.executeQuery("SELECT * FROM " + tableName)) {
          assertTrue(rs.next());
          assertEquals(struct.getString("firstname"), rs.getString("firstname"));
          assertEquals(struct.getString("lastname"), rs.getString("lastname"));
          assertEquals(struct.getString("jsonid"), rs.getString("jsonid"));
          assertEquals(struct.getString("userid"), rs.getString("userid"));
        }
      }
    }
  }

  @Test
  public void testWriteToTableWithIntArrayColumn() throws SQLException, InterruptedException {
    createTableWithIntArrayColumns();
    connect.configureConnector("jdbc-sink-connector", props);
    waitForConnectorToStart("jdbc-sink-connector", 1);

    final Schema schema = SchemaBuilder.struct().name("com.example.Person")
        .field("firstname", Schema.STRING_SCHEMA)
        .field("lastname", Schema.STRING_SCHEMA)
        .field("friends", SchemaBuilder.array(Schema.INT32_SCHEMA).build())
        .field("friendnames", SchemaBuilder.array(Schema.STRING_SCHEMA).build())
        .build();

    final Struct struct = new Struct(schema)
        .put("firstname", "Christina")
        .put("lastname", "Brams")
        .put("friends", Arrays.asList(10, 6221))
        .put("friendnames", Arrays.asList("Lucas", "Tom"));
    produceRecord(schema, struct);

    waitForCommittedRecords("jdbc-sink-connector", Collections.singleton(tableName), 1, 1,
        TimeUnit.MINUTES.toMillis(2));
    try (Connection c = pg.getEmbeddedPostgres().getPostgresDatabase().getConnection()) {
      try (Statement s = c.createStatement()) {
        try (ResultSet rs = s.executeQuery("SELECT * FROM " + tableName)) {
          assertTrue(rs.next());
          assertEquals(struct.getString("firstname"), rs.getString("firstname"));
          assertEquals(struct.getString("lastname"), rs.getString("lastname"));
          assertJDBCArray(rs, "friends", struct);
          assertJDBCArray(rs, "friendnames", struct);
        }
      }
    }
  }

  @Test
  public void testWriteToTableWithIntArrayColumnMissingFields() throws SQLException, InterruptedException {
    createTableWithIntArrayColumnsMissing();
    props.put(JdbcSinkConfig.AUTO_EVOLVE, "true");
    connect.configureConnector("jdbc-sink-connector", props);
    waitForConnectorToStart("jdbc-sink-connector", 1);

    final Schema schema = SchemaBuilder.struct().name("com.example.Person")
        .field("firstname", Schema.STRING_SCHEMA)
        .field("lastname", Schema.STRING_SCHEMA)
        .field("friends", SchemaBuilder.array(Schema.INT32_SCHEMA).optional().build())
        .field("friendnames", SchemaBuilder.array(Schema.STRING_SCHEMA).optional().build())
        .build();

    final Struct struct = new Struct(schema)
        .put("firstname", "Christina")
        .put("lastname", "Brams")
        .put("friends", Arrays.asList(10, 6221))
        .put("friendnames", Arrays.asList("Lucas", "Tom"));
    produceRecord(schema, struct);

    waitForCommittedRecords("jdbc-sink-connector", Collections.singleton(tableName), 1, 1,
        TimeUnit.MINUTES.toMillis(2));
    try (Connection c = pg.getEmbeddedPostgres().getPostgresDatabase().getConnection()) {
      try (Statement s = c.createStatement()) {
        try (ResultSet rs = s.executeQuery("SELECT * FROM " + tableName)) {
          assertTrue(rs.next());
          assertEquals(struct.getString("firstname"), rs.getString("firstname"));
          assertEquals(struct.getString("lastname"), rs.getString("lastname"));
          assertJDBCArray(rs, "friends", struct);
          assertJDBCArray(rs, "friendnames", struct);
        }
      }
    }
  }

  @Test
  public void testWriteToTableWithComplexArrayColumns() throws Exception {
    createTableWithComplexArrayColumns();
    props.put(JdbcSinkConfig.SQL_COMPLEX_TYPES_ENABLE, "true");
    connect.configureConnector("jdbc-sink-connector", props);
    waitForConnectorToStart("jdbc-sink-connector", 1);

    final Schema numeric = VariableScaleDecimal.optionalSchema();
    final Schema schema = SchemaBuilder.struct().name("com.example.ComplexArrays")
        .field("nums", SchemaBuilder.array(numeric).build())
        .field("docs", SchemaBuilder.array(Json.optionalSchema()).build())
        .build();
    final Struct struct = new Struct(schema)
        .put("nums", Arrays.asList(
            VariableScaleDecimal.fromLogical(numeric, new BigDecimal("1.50")),
            VariableScaleDecimal.fromLogical(numeric, new BigDecimal("3.14159"))))
        .put("docs", Arrays.asList("{\"k\": \"v\"}", "{\"a\": 1}"));
    produceRecord(schema, struct);

    waitForCommittedRecords("jdbc-sink-connector", Collections.singleton(tableName), 1, 1,
        TimeUnit.MINUTES.toMillis(2));

    try (Connection c = pg.getEmbeddedPostgres().getPostgresDatabase().getConnection()) {
      try (Statement s = c.createStatement()) {
        try (ResultSet rs = s.executeQuery("SELECT * FROM " + tableName)) {
          assertTrue(rs.next());

          // numeric[] via VariableScaleDecimal: per-value scale is preserved
          Object[] nums = (Object[]) rs.getArray("nums").getArray();
          assertEquals(0, new BigDecimal("1.50").compareTo((BigDecimal) nums[0]));
          assertEquals(0, new BigDecimal("3.14159").compareTo((BigDecimal) nums[1]));

          // jsonb[] via the Json logical type
          Object[] docs = (Object[]) rs.getArray("docs").getArray();
          assertTrue(docs[0].toString().contains("\"k\""));
          assertTrue(docs[1].toString().contains("\"a\""));
        }
      }
    }
  }

  @Test
  public void testTableCreatedAfterManualDeletion() throws Exception {
    props.put(JdbcSinkConfig.AUTO_CREATE, "true");
    props.put(DLQ_TOPIC_NAME_CONFIG, DLQ_TOPIC_NAME);
    props.put(DLQ_TOPIC_REPLICATION_FACTOR_CONFIG, "1");
    props.put(MAX_RETRIES, "0");
    connect.configureConnector("jdbc-sink-connector", props);
    waitForConnectorToStart("jdbc-sink-connector", 1);

    final Schema schema = SchemaBuilder.struct().name("com.example.Person")
            .field("firstname", Schema.STRING_SCHEMA)
            .field("lastname", Schema.STRING_SCHEMA)
            .build();
    final Struct firstStruct = new Struct(schema)
            .put("firstname", "Christina")
            .put("lastname", "Brams");
    final Struct secondStruct = new Struct(schema)
            .put("firstname", "Jerry")
            .put("lastname", "Mcguire");

    produceRecord(schema, firstStruct);

    waitForCommittedRecords("jdbc-sink-connector", Collections.singleton(tableName), 1, 1,
            TimeUnit.MINUTES.toMillis(2));

    try (Connection c = pg.getEmbeddedPostgres().getPostgresDatabase().getConnection()) {
      try (Statement s = c.createStatement()) {
        try (ResultSet rs = s.executeQuery("SELECT * FROM " + tableName)) {
          assertTrue(rs.next());
          assertEquals(firstStruct.getString("firstname"), rs.getString("firstname"));
          assertEquals(firstStruct.getString("lastname"), rs.getString("lastname"));
        }
      }
    }

    deleteTable();

    produceRecord(schema, secondStruct);

    waitForCommittedRecords("jdbc-sink-connector", Collections.singleton(tableName), 2, 1,
            TimeUnit.MINUTES.toMillis(2));

    try (Connection c = pg.getEmbeddedPostgres().getPostgresDatabase().getConnection()) {
      try (Statement s = c.createStatement()) {
        try (ResultSet rs = s.executeQuery("SELECT * FROM " + tableName)) {
          assertTrue(rs.next());
          assertEquals(secondStruct.getString("firstname"), rs.getString("firstname"));
          assertEquals(secondStruct.getString("lastname"), rs.getString("lastname"));
        }
      }
    }
  }

  @Test
  public void testTableCreatedWithArrayDefaults() throws Exception {
    props.put(JdbcSinkConfig.AUTO_CREATE, "true");
    props.put(DLQ_TOPIC_NAME_CONFIG, DLQ_TOPIC_NAME);
    props.put(DLQ_TOPIC_REPLICATION_FACTOR_CONFIG, "1");
    props.put(MAX_RETRIES, "0");
    connect.configureConnector("jdbc-sink-connector", props);
    waitForConnectorToStart("jdbc-sink-connector", 1);

    final Schema schema = SchemaBuilder.struct().name("com.example.Person")
            .field("firstname", Schema.STRING_SCHEMA)
            .field("lastname", Schema.STRING_SCHEMA)
            .field("hobbies", SchemaBuilder.array(Schema.STRING_SCHEMA).defaultValue(Arrays.asList("Fencing","Horse Riding")).build())
            .build();
    final Struct firstStruct = new Struct(schema)
            .put("firstname", "Christina")
            .put("lastname", "Brams")
            .put("hobbies", Arrays.asList("Skiing","Swimming"));
    final Struct secondStruct = new Struct(schema)
            .put("firstname", "Jerry")
            .put("lastname", "Mcguire");

    produceRecord(schema, firstStruct);
    produceRecord(schema, secondStruct);

    waitForCommittedRecords("jdbc-sink-connector", Collections.singleton(tableName), 1, 1,
            TimeUnit.MINUTES.toMillis(2));

    try (Connection c = pg.getEmbeddedPostgres().getPostgresDatabase().getConnection()) {
      try (Statement s = c.createStatement()) {
        try (ResultSet rs = s.executeQuery("SELECT * FROM " + tableName + " ORDER BY firstname")) {
          assertTrue(rs.next());
          assertEquals(firstStruct.getString("firstname"), rs.getString("firstname"));
          assertEquals(firstStruct.getString("lastname"), rs.getString("lastname"));

          Array sqlArray = rs.getArray("hobbies");
          List<String> actualHobbies = Arrays.asList((String[]) sqlArray.getArray());
          assertEquals(firstStruct.getArray("hobbies"), actualHobbies);

          // test the case where default values for array column should be picked
          assertTrue(rs.next());
          assertEquals(secondStruct.getString("firstname"), rs.getString("firstname"));
          assertEquals(secondStruct.getString("lastname"), rs.getString("lastname"));

          sqlArray = rs.getArray("hobbies");
          actualHobbies = Arrays.asList((String[]) sqlArray.getArray());
          assertEquals(Arrays.asList("Fencing", "Horse Riding"), actualHobbies);
        }
      }
    }
  }

  @Test
  public void testQuoteIdentifierNeverConfig() throws Exception {
    String mixedCaseTopicName = "TestTopic";

    connect.kafka().createTopic(mixedCaseTopicName, 1);

    props.put(JdbcSinkConfig.AUTO_CREATE, "true");
    props.put(JdbcSinkConfig.QUOTE_SQL_IDENTIFIERS_CONFIG, "NEVER");
    props.put("topics", mixedCaseTopicName);

    connect.configureConnector("jdbc-sink-connector", props);

    waitForConnectorToStart("jdbc-sink-connector", 1);

    final Schema schema = SchemaBuilder.struct().name("com.example.Person")
        .field("firstname", Schema.STRING_SCHEMA)
        .field("lastname", Schema.STRING_SCHEMA)
        .build();
    final Struct struct = new Struct(schema)
        .put("firstname", "Christina")
        .put("lastname", "Brams");

    String kafkaValue = new String(jsonConverter.fromConnectData(mixedCaseTopicName, schema, struct));
    connect.kafka().produce(mixedCaseTopicName, null, kafkaValue);

    waitForCommittedRecords("jdbc-sink-connector", Collections.singleton(mixedCaseTopicName), 1, 1,
        TimeUnit.MINUTES.toMillis(2));

    String autoCreatedTableName = mixedCaseTopicName.toLowerCase();
    try (Connection c = pg.getEmbeddedPostgres().getPostgresDatabase().getConnection()) {
      try (Statement s = c.createStatement()) {
        try (ResultSet rs = s.executeQuery(String.format("SELECT * FROM \"%s\"", autoCreatedTableName))) {
          assertTrue(rs.next());
          assertEquals(struct.getString("firstname"), rs.getString("firstname"));
          assertEquals(struct.getString("lastname"), rs.getString("lastname"));
        }
      }
    }
  }

  @Test
  public void testQuoteIdentifierAlwaysConfig() throws Exception {
    String mixedCaseTopicName = "TestTopic";

    connect.kafka().createTopic(mixedCaseTopicName, 1);

    props.put(JdbcSinkConfig.AUTO_CREATE, "true");
    props.put(JdbcSinkConfig.QUOTE_SQL_IDENTIFIERS_CONFIG, "ALWAYS");
    props.put("topics", mixedCaseTopicName);

    connect.configureConnector("jdbc-sink-connector", props);

    waitForConnectorToStart("jdbc-sink-connector", 1);

    final Schema schema = SchemaBuilder.struct().name("com.example.Person")
        .field("firstname", Schema.STRING_SCHEMA)
        .field("lastname", Schema.STRING_SCHEMA)
        .build();
    final Struct struct = new Struct(schema)
        .put("firstname", "Christina")
        .put("lastname", "Brams");

    String kafkaValue = new String(jsonConverter.fromConnectData(mixedCaseTopicName, schema, struct));
    connect.kafka().produce(mixedCaseTopicName, null, kafkaValue);

    waitForCommittedRecords("jdbc-sink-connector", Collections.singleton(mixedCaseTopicName), 1, 1,
        TimeUnit.MINUTES.toMillis(2));

    try (Connection c = pg.getEmbeddedPostgres().getPostgresDatabase().getConnection()) {
      try (Statement s = c.createStatement()) {
        try (ResultSet rs = s.executeQuery(String.format("SELECT * FROM \"%s\"", mixedCaseTopicName))) {
          assertTrue(rs.next());
          assertEquals(struct.getString("firstname"), rs.getString("firstname"));
          assertEquals(struct.getString("lastname"), rs.getString("lastname"));
        }
      }
    }
  }

  @Test
  public void testDbTimezoneDateConfig() throws Exception {
    String topicName = "testtopic";

    connect.kafka().createTopic(topicName, 1);

    props.put(JdbcSinkConfig.AUTO_CREATE, "true");
    props.put(JdbcSinkConfig.DB_TIMEZONE_CONFIG, "America/New_York");
    props.put(JdbcSinkConfig.DATE_TIMEZONE_CONFIG, "UTC");
    props.put("topics", topicName);

    connect.configureConnector("jdbc-sink-connector", props);

    waitForConnectorToStart("jdbc-sink-connector", 1);

    final Schema schema = SchemaBuilder.struct().name("com.example.Person")
        .field("firstname", Schema.STRING_SCHEMA)
        .field("lastname", Schema.STRING_SCHEMA)
        .field("date", Date.SCHEMA)
        .build();
    final Struct struct = new Struct(schema)
        .put("firstname", "Christina")
        .put("lastname", "Brams")
        .put("date", new java.util.Date(0));

    String kafkaValue = new String(jsonConverter.fromConnectData(topicName, schema, struct));
    connect.kafka().produce(topicName, null, kafkaValue);

    waitForCommittedRecords("jdbc-sink-connector", Collections.singleton(topicName), 1, 1,
        TimeUnit.MINUTES.toMillis(2));

    try (Connection c = pg.getEmbeddedPostgres().getPostgresDatabase().getConnection()) {
      try (Statement s = c.createStatement()) {
        try (ResultSet rs = s.executeQuery(String.format("SELECT * FROM \"%s\"", topicName))) {
          assertTrue(rs.next());
          assertEquals(struct.getString("firstname"), rs.getString("firstname"));
          assertEquals(struct.getString("lastname"), rs.getString("lastname"));
          assertEquals(struct.get("date"), rs.getDate("date", java.util.Calendar.getInstance(java.util.TimeZone.getTimeZone("UTC"))));
        }
      }
    }
  }

  private void assertJDBCArray(ResultSet rs, String fieldName, Struct struct) throws SQLException {
    Array array = rs.getArray(fieldName);
    assertNotNull(array);
    assertEquals(struct.getArray(fieldName), Arrays.asList((Object[])array.getArray()));
  }

  private void createTableWithIntArrayColumnsMissing() throws SQLException {
    LOG.info("Creating table {} with UUID column", tableName);
    try (Connection c = pg.getEmbeddedPostgres().getPostgresDatabase().getConnection()) {
      c.setAutoCommit(false);
      try (Statement s = c.createStatement()) {
        String sql = String.format(
            "CREATE TABLE %s(firstName TEXT, lastName TEXT, jsonid json)",
            tableName
        );
        LOG.info("Executing statement: {}", sql);
        s.execute(sql);
        c.commit();
      }
    }
    LOG.info("Created table {} with UUID column", tableName);
  }

  private void createTableWithComplexArrayColumns() throws SQLException {
    try (Connection c = pg.getEmbeddedPostgres().getPostgresDatabase().getConnection()) {
      try (Statement s = c.createStatement()) {
        s.execute(String.format(
            "CREATE TABLE %s(nums numeric[], docs jsonb[])", tableName));
      }
    }
  }

  private void createTableWithIntArrayColumns() throws SQLException {
    LOG.info("Creating table {} with UUID column", tableName);
    try (Connection c = pg.getEmbeddedPostgres().getPostgresDatabase().getConnection()) {
      c.setAutoCommit(false);
      try (Statement s = c.createStatement()) {
        String sql = String.format(
            "CREATE TABLE %s(firstName TEXT, lastName TEXT, jsonid json, friends int[], friendnames text[])",
            tableName
        );
        LOG.info("Executing statement: {}", sql);
        s.execute(sql);
        c.commit();
      }
    }
    LOG.info("Created table {} with UUID column", tableName);
  }

  private void createTable(String columnsSql) throws SQLException {
    try (Connection c = pg.getEmbeddedPostgres().getPostgresDatabase().getConnection()) {
      c.setAutoCommit(false);
      try (Statement s = c.createStatement()) {
        String sql = String.format(
            columnsSql,
            tableName
        );
        LOG.info("Executing statement: {}", sql);
        s.execute(sql);
        c.commit();
      }
    }
  }

  private void deleteTable() throws SQLException {
    try (Connection c = pg.getEmbeddedPostgres().getPostgresDatabase().getConnection()) {
      try (Statement s = c.createStatement()) {
        String sql = String.format("DROP TABLE %s", tableName);
        LOG.info("Executing statement: {}", sql);
        s.execute("DROP TABLE " + tableName);
        LOG.info("Dropped table");
      }
    }
  }

  private void createTableWithUuidColumns() throws SQLException {
    LOG.info("Creating table {} with UUID column", tableName);
    createTable("CREATE TABLE %s(firstName TEXT, lastName TEXT, jsonid json, userid UUID)");
    LOG.info("Created table {} with UUID column", tableName);
  }

  private void createTableWithLessFields() throws SQLException {
    LOG.info("Creating table {} with less fields", tableName);
    createTable("CREATE TABLE %s(firstName TEXT, jsonid json, userid UUID)");
    LOG.info("Created table {} with less fields", tableName);
  }

  private void createTableWithPrimaryKey() throws SQLException {
    LOG.info("Creating table {} with a primary key", tableName);
    createTable("CREATE TABLE %s(firstName TEXT PRIMARY KEY, lastName TEXT)");
    LOG.info("Created table {} with a primary key", tableName);
  }

  private void produceRecord(Schema schema, Struct struct) {
    String kafkaValue = new String(jsonConverter.fromConnectData(tableName, schema, struct));
    connect.kafka().produce(tableName, null, kafkaValue);
  }

  // ---------- arrays, read from a real Postgres ----------

  /** An optional Connect ARRAY of the given element schema, the shape the source always emits. */
  private static Schema arrayOf(Schema elementSchema) {
    return SchemaBuilder.array(elementSchema).optional().build();
  }

  /** Parse JSON text so a jsonb value can be compared structurally rather than byte-for-byte. */
  private static Object parseJson(String text) {
    try {
      return new ObjectMapper().readValue(text, Object.class);
    } catch (Exception e) {
      throw new AssertionError("not parseable JSON: " + text, e);
    }
  }

  /** Epoch millis of a UTC instant, used to build temporal expectations independently. */
  private static long utcMillis(String isoInstant) {
    return Instant.parse(isoInstant).toEpochMilli();
  }

  /**
   * Every primitive element family in one row, schema and value asserted together. PostgreSQL
   * blank-pads {@code char(n)} and the driver does not trim, so the padding must survive.
   */
  @Test
  public void testPrimitiveArraysEmitTypedElements() throws Exception {
    execute("CREATE TABLE " + tableName + "(id int, i2 int2[], i4 int4[], i8 int8[], "
            + "f4 float4[], f8 float8[], b bool[], t text[], v varchar(10)[], c char(10)[])",
        "INSERT INTO " + tableName + " VALUES (1, '{1,2}', '{10,20}', '{100,200}', "
            + "'{1.5,2.5}', '{1.25,2.5}', '{true,false}', '{a,b}', '{ab,cd}', '{cone,ctwo}')");

    Struct row = pollOneRow(complexTypesSourceProps("postgres"));

    new SchemaAndValueField("i2", arrayOf(Schema.OPTIONAL_INT16_SCHEMA),
        Arrays.asList((short) 1, (short) 2)).assertFor(row);
    new SchemaAndValueField("i4", arrayOf(Schema.OPTIONAL_INT32_SCHEMA),
        Arrays.asList(10, 20)).assertFor(row);
    new SchemaAndValueField("i8", arrayOf(Schema.OPTIONAL_INT64_SCHEMA),
        Arrays.asList(100L, 200L)).assertFor(row);
    new SchemaAndValueField("f4", arrayOf(Schema.OPTIONAL_FLOAT32_SCHEMA),
        Arrays.asList(1.5f, 2.5f)).assertFor(row);
    new SchemaAndValueField("f8", arrayOf(Schema.OPTIONAL_FLOAT64_SCHEMA),
        Arrays.asList(1.25d, 2.5d)).assertFor(row);
    new SchemaAndValueField("b", arrayOf(Schema.OPTIONAL_BOOLEAN_SCHEMA),
        Arrays.asList(true, false)).assertFor(row);
    new SchemaAndValueField("t", arrayOf(Schema.OPTIONAL_STRING_SCHEMA),
        Arrays.asList("a", "b")).assertFor(row);
    new SchemaAndValueField("v", arrayOf(Schema.OPTIONAL_STRING_SCHEMA),
        Arrays.asList("ab", "cd")).assertFor(row);
    // char(10) is blank-padded to exactly 10 characters and must not be trimmed.
    new SchemaAndValueField("c", arrayOf(Schema.OPTIONAL_STRING_SCHEMA),
        Arrays.asList("cone      ", "ctwo      ")).assertFor(row);
  }

  /**
   * A {@code numeric[]} carries one scale per element, which is why the element schema is
   * VariableScaleDecimal: a fixed {@code Decimal(scale=N)} would force them all to one scale.
   */
  @Test
  public void testNumericArrayPreservesPerElementScale() throws Exception {
    execute("CREATE TABLE " + tableName + "(id int, n numeric[])",
        "INSERT INTO " + tableName + " VALUES (1, '{1.1,2.22,3.333}')");

    Struct row = pollOneRow(complexTypesSourceProps("postgres"));

    Schema element = VariableScaleDecimal.optionalSchema();
    assertEquals("Schema for n", arrayOf(element), row.schema().field("n").schema());

    List<?> values = (List<?>) row.get("n");
    assertEquals(3, values.size());
    // BigDecimal.equals compares scale as well as value, so these assertions pin both.
    assertEquals(new BigDecimal("1.1"), VariableScaleDecimal.toLogical((Struct) values.get(0)));
    assertEquals(new BigDecimal("2.22"), VariableScaleDecimal.toLogical((Struct) values.get(1)));
    assertEquals(new BigDecimal("3.333"), VariableScaleDecimal.toLogical((Struct) values.get(2)));
  }

  /**
   * {@code json[]} text is passed through untouched; {@code jsonb[]} is re-canonicalized by
   * PostgreSQL on write, so its elements are compared parsed.
   */
  @Test
  public void testJsonArraysEmitLogicalJsonElements() throws Exception {
    execute("CREATE TABLE " + tableName + "(id int, j json[], jb jsonb[])",
        "INSERT INTO " + tableName + " VALUES (1, "
            + "ARRAY['{\"a\": 1}'::json, '[1, 2]'::json], "
            + "ARRAY['{\"a\": 1}'::jsonb, '[1, 2]'::jsonb])");

    Struct row = pollOneRow(complexTypesSourceProps("postgres"));

    new SchemaAndValueField("j", arrayOf(Json.optionalSchema()),
        Arrays.asList("{\"a\": 1}", "[1, 2]")).assertFor(row);

    assertEquals("Schema for jb", arrayOf(Json.optionalSchema()),
        row.schema().field("jb").schema());
    List<?> jsonb = (List<?>) row.get("jb");
    assertEquals(2, jsonb.size());
    assertEquals(Collections.singletonMap("a", 1), parseJson((String) jsonb.get(0)));
    assertEquals(Arrays.asList(1, 2), parseJson((String) jsonb.get(1)));
  }

  /**
   * Temporal elements resolve as the scalar path does under the default UTC {@code db.timezone},
   * with {@code timestamptz} normalized to the configured zone rather than the JVM default.
   * Expectations come from {@code java.time}, an oracle independent of the connector.
   */
  @Test
  public void testTemporalArraysHonorScalarZoneRules() throws Exception {
    execute("CREATE TABLE " + tableName + "(id int, d date[], t time[], ts timestamp[], "
            + "tstz timestamptz[])",
        "INSERT INTO " + tableName + " VALUES (1, "
            + "ARRAY['2020-04-01'::date, '2020-04-02'::date], "
            + "ARRAY['12:34:56'::time, '00:00:00'::time], "
            + "ARRAY['2020-04-01 12:34:56'::timestamp, '2020-04-02 01:02:03'::timestamp], "
            + "ARRAY['2020-04-01 12:34:56+02'::timestamptz])");

    Struct row = pollOneRow(complexTypesSourceProps("postgres"));

    new SchemaAndValueField("d", arrayOf(Date.builder().optional().build()),
        Arrays.asList(new java.util.Date(utcMillis("2020-04-01T00:00:00Z")),
            new java.util.Date(utcMillis("2020-04-02T00:00:00Z")))).assertFor(row);
    new SchemaAndValueField("t", arrayOf(Time.builder().optional().build()),
        Arrays.asList(new java.util.Date(utcMillis("1970-01-01T12:34:56Z")),
            new java.util.Date(utcMillis("1970-01-01T00:00:00Z")))).assertFor(row);
    // Default timestamp.granularity is connect_logical, so elements are Connect Timestamps.
    new SchemaAndValueField("ts", arrayOf(Timestamp.builder().optional().build()),
        Arrays.asList(new java.sql.Timestamp(utcMillis("2020-04-01T12:34:56Z")),
            new java.sql.Timestamp(utcMillis("2020-04-02T01:02:03Z")))).assertFor(row);
    // +02:00 is normalized to UTC, so 12:34:56+02 becomes 10:34:56Z.
    new SchemaAndValueField("tstz", arrayOf(Timestamp.builder().optional().build()),
        Collections.singletonList(
            new java.sql.Timestamp(utcMillis("2020-04-01T10:34:56Z")))).assertFor(row);
  }

  /**
   * The three kinds of nothing, which must stay distinguishable: SQL NULL column, empty array, and
   * a NULL element inside a populated array.
   */
  @Test
  public void testArrayNullMatrix() throws Exception {
    execute("CREATE TABLE " + tableName + "(id int, i int4[])",
        "INSERT INTO " + tableName + " VALUES (1, NULL)",
        "INSERT INTO " + tableName + " VALUES (2, '{}')",
        "INSERT INTO " + tableName + " VALUES (3, '{1,NULL,3}')");

    List<Struct> rows = pollRows(complexTypesSourceProps("postgres"));
    assertEquals(3, rows.size());

    Schema expected = arrayOf(Schema.OPTIONAL_INT32_SCHEMA);
    new SchemaAndValueField("i", expected, null).assertFor(rows.get(0));
    new SchemaAndValueField("i", expected, Collections.emptyList()).assertFor(rows.get(1));
    new SchemaAndValueField("i", expected, Arrays.asList(1, null, 3)).assertFor(rows.get(2));
  }

  /**
   * {@code int[]} and {@code int[][]} share a type OID, so a one-dimensional column accepts a nested
   * value and JDBC metadata cannot tell them apart. Nested elements become null, preserving the
   * outer cardinality and the column. Also asserted for a string element, where a pass-through
   * mapping would emit the JVM array {@code toString()} and look like data.
   */
  @Test
  public void testMultiDimensionalArrayEmitsNullPerElement() throws Exception {
    execute("CREATE TABLE " + tableName + "(id int, i int4[], t text[])",
        "INSERT INTO " + tableName + " VALUES (1, '{{1,2},{3,4}}', '{{a,b},{c,d}}')");

    Struct row = pollOneRow(complexTypesSourceProps("postgres"));

    new SchemaAndValueField("i", arrayOf(Schema.OPTIONAL_INT32_SCHEMA),
        Arrays.asList(null, null)).assertFor(row);
    new SchemaAndValueField("t", arrayOf(Schema.OPTIONAL_STRING_SCHEMA),
        Arrays.asList(null, null)).assertFor(row);
  }

  /** An unsupported element type drops its column and leaves neighbouring columns untouched. */
  @Test
  public void testUnsupportedArrayElementTypesAreSkipped() throws Exception {
    execute("CREATE TABLE " + tableName + "(id int, u uuid[], ba bytea[], i int4[])",
        "INSERT INTO " + tableName + " VALUES (1, "
            + "ARRAY['0d3ec2e0-9c6a-4b1e-9f1a-7c3a2b5d6e7f'::uuid], "
            + "ARRAY['\\x0102'::bytea], '{7}')");

    Struct row = pollOneRow(complexTypesSourceProps("postgres"));

    assertFieldAbsent(row, "u");
    assertFieldAbsent(row, "ba");
    new SchemaAndValueField("i", arrayOf(Schema.OPTIONAL_INT32_SCHEMA),
        Collections.singletonList(7)).assertFor(row);
  }

  /** Backward compatibility: with the default flag an array column is dropped, as before. */
  @Test
  public void testArraysDroppedWhenComplexTypesDisabled() throws Exception {
    execute("CREATE TABLE " + tableName + "(id int, i int4[])",
        "INSERT INTO " + tableName + " VALUES (1, '{1,2}')");

    Map<String, String> sourceProps = complexTypesSourceProps("postgres");
    sourceProps.remove(JdbcSourceConnectorConfig.SQL_COMPLEX_TYPES_ENABLE_CONFIG);
    Struct row = pollOneRow(sourceProps);

    assertFieldAbsent(row, "i");
    assertEquals(1, row.get("id"));
  }

  // ---------- arrays under each temporal configuration ----------

  /**
   * Under every {@code timestamp.granularity} an array element must resolve to what a scalar column
   * of the same type resolves to, in schema and value. Asserting against a scalar column in the
   * same row rather than a hard-coded rendering catches drift and needs no knowledge of the string
   * formats; absolute values are pinned separately for the two numeric modes.
   */
  @Test
  public void testTimestampArrayAcrossGranularityModes() throws Exception {
    execute("CREATE TABLE " + tableName + "(id int, ts timestamp[], scalar_ts timestamp)",
        "INSERT INTO " + tableName + " VALUES (1, ARRAY['2020-04-01 12:34:56'::timestamp], "
            + "'2020-04-01 12:34:56'::timestamp)");
    long micros = utcMillis("2020-04-01T12:34:56Z") * 1000L;

    for (String granularity : new String[]{"connect_logical", "micros_long", "micros_string",
        "micros_iso_datetime_string", "nanos_long", "nanos_string", "nanos_iso_datetime_string"}) {
      Struct row = pollOneRow(complexTypesSourceProps("postgres",
          JdbcSourceConnectorConfig.TIMESTAMP_GRANULARITY_CONFIG, granularity));

      Schema scalarSchema = row.schema().field("scalar_ts").schema();
      Schema elementSchema = row.schema().field("ts").schema().valueSchema();
      assertEquals("element schema must match the scalar schema for " + granularity,
          scalarSchema, elementSchema);
      assertEquals("array schema for " + granularity, arrayOf(scalarSchema),
          row.schema().field("ts").schema());
      assertEquals("element value must match the scalar value for " + granularity,
          Collections.singletonList(row.get("scalar_ts")), row.get("ts"));
    }

    // Pin the absolute values for the two modes whose representation is unambiguous.
    Struct microsLong = pollOneRow(complexTypesSourceProps("postgres",
        JdbcSourceConnectorConfig.TIMESTAMP_GRANULARITY_CONFIG, "micros_long"));
    new SchemaAndValueField("ts", arrayOf(Schema.OPTIONAL_INT64_SCHEMA),
        Collections.singletonList(micros)).assertFor(microsLong);

    Struct nanosLong = pollOneRow(complexTypesSourceProps("postgres",
        JdbcSourceConnectorConfig.TIMESTAMP_GRANULARITY_CONFIG, "nanos_long"));
    new SchemaAndValueField("ts", arrayOf(Schema.OPTIONAL_INT64_SCHEMA),
        Collections.singletonList(micros * 1000L)).assertFor(nanosLong);
  }

  /**
   * {@code db.timezone} interprets a zoneless {@code timestamp} element, while {@code date} keeps
   * using the date zone — the precedence the scalar path applies.
   */
  @Test
  public void testTemporalArraysUnderNonUtcDbTimezone() throws Exception {
    execute("CREATE TABLE " + tableName + "(id int, d date[], ts timestamp[])",
        "INSERT INTO " + tableName + " VALUES (1, ARRAY['2020-04-01'::date], "
            + "ARRAY['2020-04-01 12:34:56'::timestamp])");

    Struct row = pollOneRow(complexTypesSourceProps("postgres",
        JdbcSourceConnectorConfig.DB_TIMEZONE_CONFIG, "America/New_York"));

    // 2020-04-01 is EDT (UTC-4), so the same wall clock is four hours later in UTC.
    new SchemaAndValueField("ts", arrayOf(Timestamp.builder().optional().build()),
        Collections.singletonList(
            new java.sql.Timestamp(utcMillis("2020-04-01T16:34:56Z")))).assertFor(row);
    // date is read with the date zone, which is unaffected by db.timezone.
    new SchemaAndValueField("d", arrayOf(Date.builder().optional().build()),
        Collections.singletonList(
            new java.util.Date(utcMillis("2020-04-01T00:00:00Z")))).assertFor(row);
  }

  /**
   * The Julian and proleptic Gregorian calendars diverge only before 1582, so
   * {@code date.calendar.system} changes a pre-1582 element and leaves a modern one identical.
   */
  @Test
  public void testTemporalArraysUnderProlepticGregorianCalendar() throws Exception {
    execute("CREATE TABLE " + tableName + "(id int, ts timestamp[])",
        "INSERT INTO " + tableName + " VALUES (1, ARRAY['1500-01-01 00:00:00'::timestamp, "
            + "'2020-04-01 12:34:56'::timestamp])");

    List<?> legacy = (List<?>) pollOneRow(complexTypesSourceProps("postgres")).get("ts");
    List<?> proleptic = (List<?>) pollOneRow(complexTypesSourceProps("postgres",
        JdbcSourceConnectorConfig.DATE_CALENDAR_SYSTEM_CONFIG, "PROLEPTIC_GREGORIAN")).get("ts");

    assertNotEquals("a pre-1582 element must differ between calendar systems",
        legacy.get(0), proleptic.get(0));
    assertEquals("a modern element must be identical under both calendar systems",
        legacy.get(1), proleptic.get(1));
    assertEquals(new java.sql.Timestamp(utcMillis("2020-04-01T12:34:56Z")), legacy.get(1));
  }

  // ---------- arrays, sink side ----------

  /**
   * The element type of an array column. {@code data_type} is just {@code ARRAY} for every array,
   * so the element type comes from {@code udt_name} (leading underscore, e.g. {@code _int4}).
   */
  private String columnUdtName(String table, String column) throws SQLException {
    try (Connection c = pg.getEmbeddedPostgres().getPostgresDatabase().getConnection();
         Statement s = c.createStatement();
         ResultSet rs = s.executeQuery(
             "SELECT data_type, udt_name FROM information_schema.columns WHERE table_name = '"
                 + table + "' AND column_name = '" + column + "'")) {
      assertTrue("table " + table + " has no column " + column, rs.next());
      assertEquals("data_type for " + column, "ARRAY", rs.getString(1));
      return rs.getString(2);
    }
  }

  /**
   * The DDL half that auto-create depends on. Asserted separately from the bind half because DDL
   * succeeding proves nothing about writing: a {@code Decimal} element created a valid
   * {@code numeric[]} column that no insert could populate until its element binding was added.
   */
  @Test
  public void testAutoCreateArrayColumnTypes() throws Exception {
    props.put(JdbcSinkConfig.AUTO_CREATE, "true");
    props.put(JdbcSinkConfig.SQL_COMPLEX_TYPES_ENABLE, "true");
    connect.configureConnector("jdbc-sink-connector", props);
    waitForConnectorToStart("jdbc-sink-connector", 1);

    Schema decimal = Decimal.builder(2).optional().build();
    Schema variableScale = VariableScaleDecimal.optionalSchema();
    Schema hstoreMap = SchemaBuilder
        .map(Schema.STRING_SCHEMA, Schema.OPTIONAL_STRING_SCHEMA).optional().build();
    final Schema schema = SchemaBuilder.struct().name("com.example.AllArrays")
        .field("a_i16", arrayOf(Schema.OPTIONAL_INT16_SCHEMA))
        .field("a_i32", arrayOf(Schema.OPTIONAL_INT32_SCHEMA))
        .field("a_i64", arrayOf(Schema.OPTIONAL_INT64_SCHEMA))
        .field("a_f32", arrayOf(Schema.OPTIONAL_FLOAT32_SCHEMA))
        .field("a_f64", arrayOf(Schema.OPTIONAL_FLOAT64_SCHEMA))
        .field("a_bool", arrayOf(Schema.OPTIONAL_BOOLEAN_SCHEMA))
        .field("a_text", arrayOf(Schema.OPTIONAL_STRING_SCHEMA))
        .field("a_json", arrayOf(Json.optionalSchema()))
        .field("a_decimal", arrayOf(decimal))
        .field("a_varscale", arrayOf(variableScale))
        .field("a_date", arrayOf(Date.builder().optional().build()))
        .field("a_time", arrayOf(Time.builder().optional().build()))
        .field("a_ts", arrayOf(Timestamp.builder().optional().build()))
        .field("a_hstore", arrayOf(hstoreMap))
        .build();

    java.util.Date epoch = new java.util.Date(0L);
    produceRecord(schema, new Struct(schema)
        .put("a_i16", Collections.singletonList((short) 1))
        .put("a_i32", Collections.singletonList(2))
        .put("a_i64", Collections.singletonList(3L))
        .put("a_f32", Collections.singletonList(1.5f))
        .put("a_f64", Collections.singletonList(2.5d))
        .put("a_bool", Collections.singletonList(true))
        .put("a_text", Collections.singletonList("x"))
        .put("a_json", Collections.singletonList("{\"a\": 1}"))
        .put("a_decimal", Collections.singletonList(new BigDecimal("1.20")))
        .put("a_varscale", Collections.singletonList(
            VariableScaleDecimal.fromLogical(variableScale, new BigDecimal("3.456"))))
        .put("a_date", Collections.singletonList(epoch))
        .put("a_time", Collections.singletonList(epoch))
        .put("a_ts", Collections.singletonList(epoch))
        .put("a_hstore", Collections.singletonList(
            Collections.singletonMap("env", "prod"))));

    waitForCommittedRecords("jdbc-sink-connector", Collections.singleton(tableName), 1, 1,
        TimeUnit.MINUTES.toMillis(2));

    Map<String, String> expectedUdtNames = new LinkedHashMap<>();
    expectedUdtNames.put("a_i16", "_int2");
    expectedUdtNames.put("a_i32", "_int4");
    expectedUdtNames.put("a_i64", "_int8");
    expectedUdtNames.put("a_f32", "_float4");
    expectedUdtNames.put("a_f64", "_float8");
    expectedUdtNames.put("a_bool", "_bool");
    expectedUdtNames.put("a_text", "_text");
    expectedUdtNames.put("a_json", "_jsonb");
    expectedUdtNames.put("a_decimal", "_numeric");
    expectedUdtNames.put("a_varscale", "_numeric");
    expectedUdtNames.put("a_date", "_date");
    expectedUdtNames.put("a_time", "_time");
    expectedUdtNames.put("a_ts", "_timestamp");
    expectedUdtNames.put("a_hstore", "_jsonb");
    for (Map.Entry<String, String> entry : expectedUdtNames.entrySet()) {
      assertEquals("element type of " + entry.getKey(), entry.getValue(),
          columnUdtName(tableName, entry.getKey()));
    }
  }

  /**
   * Backward compatibility: primitive-element arrays predate the complex-types flag, so they must
   * still auto-create and write with it at its default. Only the new element types are gated.
   */
  @Test
  public void testPrimitiveArraySinkUnaffectedByComplexTypesFlag() throws Exception {
    props.put(JdbcSinkConfig.AUTO_CREATE, "true");
    connect.configureConnector("jdbc-sink-connector", props);
    waitForConnectorToStart("jdbc-sink-connector", 1);

    final Schema schema = SchemaBuilder.struct().name("com.example.PrimitiveArrays")
        .field("nums", arrayOf(Schema.OPTIONAL_INT32_SCHEMA))
        .field("words", arrayOf(Schema.OPTIONAL_STRING_SCHEMA))
        .build();
    produceRecord(schema, new Struct(schema)
        .put("nums", Arrays.asList(1, 2, 42))
        .put("words", Arrays.asList("a", "b")));

    waitForCommittedRecords("jdbc-sink-connector", Collections.singleton(tableName), 1, 1,
        TimeUnit.MINUTES.toMillis(2));

    assertEquals("_int4", columnUdtName(tableName, "nums"));
    assertEquals("_text", columnUdtName(tableName, "words"));
    try (Connection c = pg.getEmbeddedPostgres().getPostgresDatabase().getConnection();
         Statement s = c.createStatement();
         ResultSet rs = s.executeQuery("SELECT nums::text, words::text FROM " + tableName)) {
      assertTrue(rs.next());
      assertEquals("{1,2,42}", rs.getString(1));
      assertEquals("{a,b}", rs.getString(2));
    }
  }

  // ---------- array round trips: source -> Kafka -> sink ----------

  /**
   * Assert destination arrays by casting to text in SQL, keeping expectations exact and independent
   * of the test JVM zone and of how the driver would decode them on read-back.
   */
  private void assertDestArrayText(String selectList, String... expected) throws SQLException {
    try (Connection c = pg.getEmbeddedPostgres().getPostgresDatabase().getConnection();
         Statement s = c.createStatement();
         ResultSet rs = s.executeQuery(
             "SELECT " + selectList + " FROM " + DST_TABLE + " ORDER BY id")) {
      assertTrue("destination table has no rows", rs.next());
      for (int i = 0; i < expected.length; i++) {
        assertEquals("column " + (i + 1) + " of " + selectList, expected[i], rs.getString(i + 1));
      }
    }
  }

  /**
   * Every primitive element family end to end. The three string-like source types all converge on
   * {@code text[]}, and {@code char(n)} padding must survive the whole pipeline — hence the quoted
   * expected text, since PostgreSQL quotes array elements containing spaces.
   */
  @Test
  public void testPrimitiveArrayRoundTrip() throws Exception {
    execute("CREATE TABLE " + SRC_TABLE + "(id int PRIMARY KEY, i2 int2[], i4 int4[], i8 int8[], "
            + "f4 float4[], f8 float8[], b bool[], t text[], v varchar(10)[], c char(10)[])",
        "INSERT INTO " + SRC_TABLE + " VALUES (1, '{1,2}', '{1,2,42}', '{100,200}', "
            + "'{1.5,2.5}', '{1.25,2.5}', '{true,false}', '{a,b}', '{ab,cd}', '{cone,ctwo}')");

    runRoundTrip(1,
        Collections.singletonMap(
            JdbcSourceConnectorConfig.SQL_COMPLEX_TYPES_ENABLE_CONFIG, "true"),
        Collections.singletonMap(JdbcSinkConfig.SQL_COMPLEX_TYPES_ENABLE, "true"));

    Map<String, String> expectedUdtNames = new LinkedHashMap<>();
    expectedUdtNames.put("i2", "_int2");
    expectedUdtNames.put("i4", "_int4");
    expectedUdtNames.put("i8", "_int8");
    expectedUdtNames.put("f4", "_float4");
    expectedUdtNames.put("f8", "_float8");
    expectedUdtNames.put("b", "_bool");
    expectedUdtNames.put("t", "_text");
    // varchar and char(n) elements are STRING on the topic, so both land in text[].
    expectedUdtNames.put("v", "_text");
    expectedUdtNames.put("c", "_text");
    for (Map.Entry<String, String> entry : expectedUdtNames.entrySet()) {
      assertEquals("element type of " + entry.getKey(), entry.getValue(),
          columnUdtName(DST_TABLE, entry.getKey()));
    }

    assertDestArrayText(
        "i2::text, i4::text, i8::text, f4::text, f8::text, b::text, t::text, v::text, c::text",
        "{1,2}", "{1,2,42}", "{100,200}", "{1.5,2.5}", "{1.25,2.5}", "{t,f}", "{a,b}", "{ab,cd}",
        // char(10) padding survives; Postgres quotes array elements containing spaces.
        "{\"cone      \",\"ctwo      \"}");
  }

  /**
   * The complex element types end to end. The destination text form shows each numeric element
   * retaining its own scale, which a fixed-scale element schema could not represent.
   */
  @Test
  public void testComplexArrayRoundTrip() throws Exception {
    execute("CREATE TABLE " + SRC_TABLE + "(id int PRIMARY KEY, n numeric[], j json[], "
            + "jb jsonb[])",
        "INSERT INTO " + SRC_TABLE + " VALUES (1, '{1.1,2.22,3.333}', "
            + "ARRAY['{\"a\": 1}'::json, '[1, 2]'::json], "
            + "ARRAY['{\"a\": 1}'::jsonb, '[1, 2]'::jsonb])");

    runRoundTrip(1,
        Collections.singletonMap(
            JdbcSourceConnectorConfig.SQL_COMPLEX_TYPES_ENABLE_CONFIG, "true"),
        Collections.singletonMap(JdbcSinkConfig.SQL_COMPLEX_TYPES_ENABLE, "true"));

    assertEquals("numeric[] must land in a native numeric[] column",
        "_numeric", columnUdtName(DST_TABLE, "n"));
    // Both json variants carry the Json logical type, so both are provisioned as jsonb[].
    assertEquals("json[] must land in a native jsonb[] column",
        "_jsonb", columnUdtName(DST_TABLE, "j"));
    assertEquals("jsonb[] must land in a native jsonb[] column",
        "_jsonb", columnUdtName(DST_TABLE, "jb"));
    // Per-element scale survives: 1.1 keeps scale 1, 2.22 scale 2, 3.333 scale 3.
    assertDestArrayText("n::text", "{1.1,2.22,3.333}");
    // Elements are real jsonb, so the operators work on them.
    assertDestArrayText("j[1]->>'a', jsonb_typeof(j[2]), jb[1]->>'a', jsonb_typeof(jb[2])",
        "1", "array", "1", "array");
  }

  /**
   * The three temporal families end to end, asserted as destination text so the values are exact.
   *
   * <p>The non-UTC {@code db.timezone} is deliberate: under UTC this would still pass if the sink
   * bound raw {@code java.util.Date} elements instead of formatting them, since
   * {@code Date.toString()} renders correctly when the JVM zone happens to match.
   *
   * <p>{@code date.timezone=UTC} is required, not incidental: a source always reads {@code date} at
   * UTC while a sink writes it at {@code date.timezone}, which defaults to {@code db.timezone}, so
   * the default would shift every date by the offset. That asymmetry is connector-wide, applies to
   * scalar {@code date} too, and is not changed here.
   */
  @Test
  public void testTemporalArrayRoundTrip() throws Exception {
    execute("CREATE TABLE " + SRC_TABLE + "(id int PRIMARY KEY, d date[], t time[], "
            + "ts timestamp[], tstz timestamptz[])",
        "INSERT INTO " + SRC_TABLE + " VALUES (1, "
            + "ARRAY['2020-04-01'::date, '2020-04-02'::date], "
            + "ARRAY['12:34:56'::time], "
            + "ARRAY['2020-04-01 12:34:56'::timestamp], "
            + "ARRAY['2020-04-01 12:34:56+02'::timestamptz])");

    Map<String, String> sourceExtras = new HashMap<>();
    sourceExtras.put(JdbcSourceConnectorConfig.SQL_COMPLEX_TYPES_ENABLE_CONFIG, "true");
    sourceExtras.put(JdbcSourceConnectorConfig.DB_TIMEZONE_CONFIG, "America/New_York");
    Map<String, String> sinkExtras = new HashMap<>();
    sinkExtras.put(JdbcSinkConfig.SQL_COMPLEX_TYPES_ENABLE, "true");
    sinkExtras.put(JdbcSinkConfig.DB_TIMEZONE_CONFIG, "America/New_York");
    sinkExtras.put(JdbcSinkConfig.DATE_TIMEZONE_CONFIG, "UTC");

    runRoundTrip(1, sourceExtras, sinkExtras);

    assertEquals("_date", columnUdtName(DST_TABLE, "d"));
    assertEquals("_time", columnUdtName(DST_TABLE, "t"));
    assertEquals("_timestamp", columnUdtName(DST_TABLE, "ts"));
    // timestamptz is a plain Connect Timestamp, so it lands in timestamp[] without the offset,
    // matching the connector's scalar behaviour.
    assertEquals("timestamptz[] lands in a zoneless timestamp[] column",
        "_timestamp", columnUdtName(DST_TABLE, "tstz"));

    // Both ends apply the same zone, so the wall clock survives. 12:34:56+02 is 10:34:56Z,
    // re-rendered in America/New_York (EDT, UTC-4).
    assertDestArrayText("d::text, t::text, ts::text, tstz::text",
        "{2020-04-01,2020-04-02}", "{12:34:56}", "{\"2020-04-01 12:34:56\"}",
        "{\"2020-04-01 06:34:56\"}");
  }

  /** An {@code hstore[]} fixture: a populated element, one with a NULL value, and an empty one. */
  private void createHstoreArraySourceRows() throws SQLException {
    execute("CREATE EXTENSION IF NOT EXISTS hstore",
        "CREATE TABLE " + SRC_TABLE + "(id int PRIMARY KEY, hs hstore[])",
        "INSERT INTO " + SRC_TABLE + " VALUES (1, ARRAY["
            + "'\"env\" => \"prod\"'::hstore, '\"k\" => NULL'::hstore, ''::hstore])");
  }

  /**
   * Both handling modes converge on the same destination: a Connect map and a Json string each
   * provision {@code jsonb[]}, so the data survives but the hstore target type does not.
   */
  private void assertHstoreArrayRoundTripRows() throws SQLException {
    assertEquals("hstore[] must land in a native jsonb[] column",
        "_jsonb", columnUdtName(DST_TABLE, "hs"));
    // A NULL hstore value stays a JSON null with its key; an empty hstore stays {} rather than
    // NULL. jsonb_typeof is itself NULL for a NULL element, so "object" distinguishes the two.
    assertDestArrayText(
        "hs[1]->>'env', jsonb_typeof(hs[2]->'k'), hs[3]::text, jsonb_typeof(hs[3])",
        "prod", "null", "{}", "object");
  }

  @Test
  public void testHstoreArrayMapModeRoundTrip() throws Exception {
    createHstoreArraySourceRows();

    runRoundTrip(1,
        Collections.singletonMap(
            JdbcSourceConnectorConfig.SQL_COMPLEX_TYPES_ENABLE_CONFIG, "true"),
        Collections.singletonMap(JdbcSinkConfig.SQL_COMPLEX_TYPES_ENABLE, "true"));

    assertHstoreArrayRoundTripRows();
  }

  @Test
  public void testHstoreArrayJsonModeRoundTrip() throws Exception {
    createHstoreArraySourceRows();

    Map<String, String> sourceExtras = new HashMap<>();
    sourceExtras.put(JdbcSourceConnectorConfig.SQL_COMPLEX_TYPES_ENABLE_CONFIG, "true");
    sourceExtras.put(JdbcSourceConnectorConfig.HSTORE_HANDLING_MODE_CONFIG, "json");

    runRoundTrip(1, sourceExtras,
        Collections.singletonMap(JdbcSinkConfig.SQL_COMPLEX_TYPES_ENABLE, "true"));

    assertHstoreArrayRoundTripRows();
  }

  /**
   * {@code timestamp.granularity} propagates into the destination DDL. Under {@code micros_long} the
   * element is a bare INT64 with no logical name, so the sink cannot recognise it as a timestamp and
   * provisions {@code bigint[]} of epoch microseconds: the value survives, the column type does not.
   * Replicating Postgres to Postgres wants the default {@code connect_logical}.
   */
  @Test
  public void testTimestampArrayRoundTripUnderMicrosGranularity() throws Exception {
    execute("CREATE TABLE " + SRC_TABLE + "(id int PRIMARY KEY, ts timestamp[])",
        "INSERT INTO " + SRC_TABLE + " VALUES (1, ARRAY['2020-04-01 12:34:56'::timestamp])");

    Map<String, String> sourceExtras = new HashMap<>();
    sourceExtras.put(JdbcSourceConnectorConfig.SQL_COMPLEX_TYPES_ENABLE_CONFIG, "true");
    sourceExtras.put(JdbcSourceConnectorConfig.TIMESTAMP_GRANULARITY_CONFIG, "micros_long");

    runRoundTrip(1, sourceExtras,
        Collections.singletonMap(JdbcSinkConfig.SQL_COMPLEX_TYPES_ENABLE, "true"));

    assertEquals("an INT64 element carries no logical type, so it lands in bigint[]",
        "_int8", columnUdtName(DST_TABLE, "ts"));
    long micros = utcMillis("2020-04-01T12:34:56Z") * 1000L;
    assertDestArrayText("ts::text", "{" + micros + "}");
  }

  /**
   * A pre-1582 value must survive under either {@code date.calendar.system}, since the source and
   * sink halves of a setting are inverses. Only pre-1582 exercises this; the calendars agree after.
   */
  private void assertPre1582ArrayRoundTrip(String calendarSystem) throws Exception {
    execute("CREATE TABLE " + SRC_TABLE + "(id int PRIMARY KEY, d date[], ts timestamp[])",
        "INSERT INTO " + SRC_TABLE + " VALUES (1, ARRAY['1500-01-01'::date], "
            + "ARRAY['1500-01-01 00:00:00'::timestamp])");

    Map<String, String> sourceExtras = new HashMap<>();
    sourceExtras.put(JdbcSourceConnectorConfig.SQL_COMPLEX_TYPES_ENABLE_CONFIG, "true");
    sourceExtras.put(JdbcSourceConnectorConfig.DATE_CALENDAR_SYSTEM_CONFIG, calendarSystem);
    Map<String, String> sinkExtras = new HashMap<>();
    sinkExtras.put(JdbcSinkConfig.SQL_COMPLEX_TYPES_ENABLE, "true");
    sinkExtras.put(JdbcSinkConfig.DATE_CALENDAR_SYSTEM_CONFIG, calendarSystem);
    sinkExtras.put(JdbcSinkConfig.DATE_TIMEZONE_CONFIG, "UTC");

    runRoundTrip(1, sourceExtras, sinkExtras);

    assertEquals("_date", columnUdtName(DST_TABLE, "d"));
    assertEquals("_timestamp", columnUdtName(DST_TABLE, "ts"));
    assertDestArrayText("d::text, ts::text",
        "{1500-01-01}", "{\"1500-01-01 00:00:00\"}");
  }

  @Test
  public void testTemporalArrayRoundTripUnderProlepticGregorianCalendar() throws Exception {
    assertPre1582ArrayRoundTrip("PROLEPTIC_GREGORIAN");
  }

  @Test
  public void testTemporalArrayRoundTripUnderLegacyCalendar() throws Exception {
    assertPre1582ArrayRoundTrip(JdbcSinkConfig.DATE_CALENDAR_SYSTEM_DEFAULT);
  }

  /**
   * {@code numeric.mapping} narrows a scalar NUMERIC but is deliberately not applied to array
   * elements, which stay VariableScaleDecimal so each keeps its own scale. Both columns round-trip
   * together so the asymmetry is visible: the scalar changes type at the destination, the array not.
   */
  @Test
  public void testNumericArrayRoundTripIsUnaffectedByNumericMapping() throws Exception {
    execute("CREATE TABLE " + SRC_TABLE + "(id int PRIMARY KEY, n numeric[], sn numeric(5,0))",
        "INSERT INTO " + SRC_TABLE + " VALUES (1, '{1.1,2.22}', 42)");

    Map<String, String> sourceExtras = new HashMap<>();
    sourceExtras.put(JdbcSourceConnectorConfig.SQL_COMPLEX_TYPES_ENABLE_CONFIG, "true");
    sourceExtras.put(JdbcSourceConnectorConfig.NUMERIC_MAPPING_CONFIG, "best_fit");

    runRoundTrip(1, sourceExtras,
        Collections.singletonMap(JdbcSinkConfig.SQL_COMPLEX_TYPES_ENABLE, "true"));

    assertEquals("array elements ignore numeric.mapping and stay numeric[]",
        "_numeric", columnUdtName(DST_TABLE, "n"));
    // Per-element scale still survives under a non-default numeric.mapping.
    assertDestArrayText("n::text", "{1.1,2.22}");

    // The scalar column, by contrast, is narrowed to an integer by best_fit.
    try (Connection c = pg.getEmbeddedPostgres().getPostgresDatabase().getConnection();
         Statement s = c.createStatement();
         ResultSet rs = s.executeQuery(
             "SELECT data_type FROM information_schema.columns WHERE table_name = '"
                 + DST_TABLE + "' AND column_name = 'sn'")) {
      assertTrue(rs.next());
      assertEquals("scalar numeric(5,0) is narrowed by numeric.mapping=best_fit",
          "integer", rs.getString(1));
    }
  }

  /**
   * The upgrade asymmetry for arrays: the source is upgraded with the flag on, the sink still has
   * it off. A {@code Json} element is STRING-based, so it falls to the primitive bind path and
   * lands in {@code text[]} rather than {@code jsonb[]} — degraded, but every document intact.
   * This is the only route by which the gated {@code Json} mapping is reachable on this branch,
   * since a scalar json column is still emitted as an untagged STRING here.
   */
  @Test
  public void testJsonArrayRoundTripSourceEnabledSinkDisabled() throws Exception {
    execute("CREATE TABLE " + SRC_TABLE + "(id int PRIMARY KEY, j json[], jb jsonb[])",
        "INSERT INTO " + SRC_TABLE + " VALUES (1, "
            + "ARRAY['{\"a\": 1}'::json, '[1, 2]'::json], "
            + "ARRAY['{\"a\": 1}'::jsonb, '[1, 2]'::jsonb])");

    runRoundTrip(1,
        Collections.singletonMap(
            JdbcSourceConnectorConfig.SQL_COMPLEX_TYPES_ENABLE_CONFIG, "true"),
        Collections.emptyMap());

    assertEquals("a sink with the flag off must fall back to text[]",
        "_text", columnUdtName(DST_TABLE, "j"));
    assertEquals("a sink with the flag off must fall back to text[]",
        "_text", columnUdtName(DST_TABLE, "jb"));
    // The documents survive verbatim; only the column type is degraded.
    assertDestArrayText("j::text", "{\"{\\\"a\\\": 1}\",\"[1, 2]\"}");
  }

  /**
   * Backward compatibility end to end: with the flag off on both connectors a gated element type
   * never reaches the topic, so the destination table has no such column.
   */
  @Test
  public void testArrayRoundTripDroppedWhenComplexTypesDisabled() throws Exception {
    execute("CREATE TABLE " + SRC_TABLE + "(id int PRIMARY KEY, i int4[], n numeric[])",
        "INSERT INTO " + SRC_TABLE + " VALUES (1, '{1,2}', '{1.1}')");

    runRoundTrip(1);

    for (String column : new String[]{"i", "n"}) {
      try (Connection c = pg.getEmbeddedPostgres().getPostgresDatabase().getConnection();
           Statement s = c.createStatement();
           ResultSet rs = s.executeQuery(
               "SELECT column_name FROM information_schema.columns WHERE table_name = '"
                   + DST_TABLE + "' AND column_name = '" + column + "'")) {
        assertTrue("array column " + column + " must not reach the destination while complex "
            + "types are disabled", !rs.next());
      }
    }
  }

  // ---------- shared harness for complex-type source tests ----------

  /**
   * A field's expected schema and value, asserted together, mirroring Debezium's
   * {@code SchemaAndValueField}. The field must exist, its schema must match <em>in full</em> — type,
   * logical name, optionality and nested key/value schemas, via Connect's own structural
   * {@code Schema.equals} — and its value must match. Asserting the whole schema object is what
   * catches a dropped logical name or a wrongly non-optional element in a single assertion.
   */
  protected static class SchemaAndValueField {

    private final String fieldName;
    private final Schema schema;
    private final Object value;
    private final boolean valueIsJsonText;

    protected SchemaAndValueField(String fieldName, Schema schema, Object value) {
      this(fieldName, schema, value, false);
    }

    private SchemaAndValueField(
        String fieldName, Schema schema, Object value, boolean valueIsJsonText) {
      this.fieldName = fieldName;
      this.schema = schema;
      this.value = value;
      this.valueIsJsonText = valueIsJsonText;
    }

    /**
     * A field carrying JSON text whose expected value is compared <em>parsed</em>. Necessary wherever
     * the text is built from a driver-supplied {@code HashMap}, since key order is hash order and not
     * insertion order — Debezium's own expectations are byte-exact and therefore order-fragile.
     */
    protected static SchemaAndValueField jsonText(String fieldName, Schema schema, Object parsed) {
      return new SchemaAndValueField(fieldName, schema, parsed, true);
    }

    protected void assertFor(Struct content) {
      assertSchema(content);
      assertValue(content);
    }

    private void assertSchema(Struct content) {
      Field field = content.schema().field(fieldName);
      assertNotNull(fieldName + " not found in schema " + content.schema(), field);
      assertEquals("Schema for " + fieldName, schema, field.schema());
    }

    private void assertValue(Struct content) {
      Object actual = content.get(fieldName);
      if (value == null) {
        assertNull(fieldName + " should be null but was " + actual, actual);
        return;
      }
      assertNotNull(fieldName + " should not be null", actual);
      if (valueIsJsonText) {
        assertTrue(fieldName + " should be JSON text but was " + actual.getClass(),
            actual instanceof String);
        assertEquals("Parsed JSON for " + fieldName, value, parseJson((String) actual));
        return;
      }
      assertEquals("Value for " + fieldName, value, actual);
    }

    private static Object parseJson(String text) {
      try {
        return new ObjectMapper().readValue(text, Object.class);
      } catch (Exception e) {
        throw new AssertionError("Field value is not parseable JSON: " + text, e);
      }
    }
  }

  /**
   * Assert that a field is absent from the record's schema, i.e. the column was dropped rather than
   * emitted. Used for the complex-types-disabled and unsupported-type cases.
   */
  protected static void assertFieldAbsent(Struct content, String fieldName) {
    assertNull(fieldName + " must not be emitted, but the schema has " + content.schema().fields(),
        content.schema().field(fieldName));
  }

  /**
   * Source-connector properties for a bulk read of {@link #tableName} in the given database, with
   * complex types enabled. Extra key/value pairs override or add to the defaults.
   */
  protected Map<String, String> complexTypesSourceProps(String database, String... extras) {
    Map<String, String> sourceProps = new HashMap<>();
    sourceProps.put(JdbcSourceConnectorConfig.CONNECTION_URL_CONFIG, String.format(
        "jdbc:postgresql://localhost:%s/%s", pg.getEmbeddedPostgres().getPort(), database));
    sourceProps.put(JdbcSourceConnectorConfig.CONNECTION_USER_CONFIG, "postgres");
    sourceProps.put(JdbcSourceConnectorConfig.MODE_CONFIG, JdbcSourceConnectorConfig.MODE_BULK);
    sourceProps.put(JdbcSourceTaskConfig.TOPIC_PREFIX_CONFIG, "topic_");
    sourceProps.put(JdbcSourceTaskConfig.TABLES_CONFIG, tableName);
    sourceProps.put(JdbcSourceTaskConfig.TABLES_FETCHED, "true");
    sourceProps.put(JdbcSourceConnectorConfig.SQL_COMPLEX_TYPES_ENABLE_CONFIG, "true");
    for (int i = 0; i < extras.length; i += 2) {
      sourceProps.put(extras[i], extras[i + 1]);
    }
    return sourceProps;
  }

  /** Poll the configured table and return the single expected row's value Struct. */
  protected Struct pollOneRow(Map<String, String> sourceProps) throws InterruptedException {
    List<Struct> rows = pollRows(sourceProps);
    assertEquals("expected exactly one row", 1, rows.size());
    return rows.get(0);
  }

  /**
   * Poll the configured table and return its rows, distinct by {@code id} and in {@code id} order.
   *
   * <p>De-duplication is deliberate: in bulk mode a single {@code poll()} re-runs the query once the
   * querier is exhausted, so a small table can legitimately come back more than once in one batch.
   * These tests assert type mapping, not polling cadence, so the primary key is the right notion of
   * "the rows of the table" and keeps them deterministic.
   */
  protected List<Struct> pollRows(Map<String, String> sourceProps) throws InterruptedException {
    JdbcSourceTask task = new JdbcSourceTask();
    try {
      task.start(sourceProps);
      List<SourceRecord> records = task.poll();
      assertNotNull("source task returned no records", records);
      Map<Integer, Struct> byId = new TreeMap<>();
      List<Struct> unkeyed = new ArrayList<>();
      for (SourceRecord record : records) {
        Struct row = (Struct) record.value();
        if (row.schema().field("id") == null) {
          unkeyed.add(row);
        } else {
          byId.putIfAbsent(row.getInt32("id"), row);
        }
      }
      if (byId.isEmpty()) {
        return unkeyed;
      }
      return new ArrayList<>(byId.values());
    } finally {
      task.stop();
    }
  }

  // ---------- round-trip harness: source connector -> Kafka -> sink connector ----------

  protected static final String SRC_TABLE = "src_types";
  protected static final String DST_TABLE = "dst_types";
  private static final String ROUND_TRIP_TOPIC = "rt_" + SRC_TABLE;

  /**
   * Run a full round trip: a source connector reads {@link #SRC_TABLE}, publishes to Kafka, and a sink
   * connector writes to {@link #DST_TABLE}. Both connectors run in the embedded Connect cluster, so
   * this exercises the converters and the worker, which the task-level tests bypass.
   *
   * <p>Modelled on Debezium's {@code AbstractJdbcSinkPipelineIT}, which likewise asserts the
   * destination <em>column type</em> as well as the values.
   *
   * @param expectedRows how many rows the sink should commit before assertions run
   * @param sourceExtras extra source-connector properties, as key/value pairs
   * @param sinkExtras extra sink-connector properties, as key/value pairs
   */
  protected void runRoundTrip(int expectedRows, Map<String, String> sourceExtras,
      Map<String, String> sinkExtras) throws Exception {
    connect.kafka().createTopic(ROUND_TRIP_TOPIC, 1);

    Map<String, String> sourceProps = new HashMap<>();
    sourceProps.put(ConnectorConfig.CONNECTOR_CLASS_CONFIG, JdbcSourceConnector.class.getName());
    sourceProps.put(ConnectorConfig.TASKS_MAX_CONFIG, "1");
    sourceProps.put(JdbcSourceConnectorConfig.CONNECTION_URL_CONFIG, jdbcUrl());
    sourceProps.put(JdbcSourceConnectorConfig.CONNECTION_USER_CONFIG, "postgres");
    sourceProps.put(JdbcSourceConnectorConfig.MODE_CONFIG, JdbcSourceConnectorConfig.MODE_BULK);
    sourceProps.put(JdbcSourceConnectorConfig.POLL_INTERVAL_MS_CONFIG, "1000");
    sourceProps.put(JdbcSourceConnectorConfig.TABLE_WHITELIST_CONFIG, SRC_TABLE);
    sourceProps.put(JdbcSourceTaskConfig.TOPIC_PREFIX_CONFIG, "rt_");
    sourceProps.put("key.converter", StringConverter.class.getName());
    sourceProps.put("value.converter", JsonConverter.class.getName());
    sourceProps.putAll(sourceExtras);

    Map<String, String> sinkProps = new HashMap<>(props);
    sinkProps.put("topics", ROUND_TRIP_TOPIC);
    sinkProps.put(JdbcSinkConfig.AUTO_CREATE, "true");
    sinkProps.put(JdbcSinkConfig.TABLE_NAME_FORMAT, DST_TABLE);
    sinkProps.put(JdbcSinkConfig.PK_MODE, "record_value");
    sinkProps.put(JdbcSinkConfig.PK_FIELDS, "id");
    sinkProps.put(JdbcSinkConfig.INSERT_MODE, "upsert");
    sinkProps.putAll(sinkExtras);

    connect.configureConnector("rt-source", sourceProps);
    waitForConnectorToStart("rt-source", 1);
    connect.configureConnector("rt-sink", sinkProps);
    waitForConnectorToStart("rt-sink", 1);

    waitForCommittedRecords("rt-sink", Collections.singleton(ROUND_TRIP_TOPIC), expectedRows, 1,
        TimeUnit.MINUTES.toMillis(3));
  }

  protected void runRoundTrip(int expectedRows) throws Exception {
    runRoundTrip(expectedRows, Collections.emptyMap(), Collections.emptyMap());
  }

  protected String jdbcUrl() {
    return String.format("jdbc:postgresql://localhost:%s/postgres",
        pg.getEmbeddedPostgres().getPort());
  }

  /**
   * The declared SQL type of a column in the destination table, e.g. {@code jsonb} or {@code text}.
   * Asserting this — not merely the value — is what proves the DDL mapping rather than just the bind.
   */
  protected String destColumnType(String column) throws SQLException {
    try (Connection c = pg.getEmbeddedPostgres().getPostgresDatabase().getConnection();
         Statement s = c.createStatement();
         ResultSet rs = s.executeQuery(
             "SELECT data_type FROM information_schema.columns WHERE table_name = '"
                 + DST_TABLE + "' AND column_name = '" + column + "'")) {
      assertTrue("destination table has no column " + column, rs.next());
      return rs.getString(1);
    }
  }

  /** Run a query against the destination table and hand each row to the given check, in order. */
  protected void queryDest(String selectList, String orderBy, RowCheck... checks)
      throws SQLException {
    try (Connection c = pg.getEmbeddedPostgres().getPostgresDatabase().getConnection();
         Statement s = c.createStatement();
         ResultSet rs = s.executeQuery(
             "SELECT " + selectList + " FROM " + DST_TABLE + " ORDER BY " + orderBy)) {
      for (int i = 0; i < checks.length; i++) {
        assertTrue("destination table has fewer than " + checks.length + " rows", rs.next());
        checks[i].check(rs);
      }
      assertTrue("destination table has more than " + checks.length + " rows", !rs.next());
    }
  }

  @FunctionalInterface
  protected interface RowCheck {
    void check(ResultSet rs) throws SQLException;
  }

  /** Execute the given statements against {@link #tableName}'s database. */
  protected void execute(String... statements) throws SQLException {
    try (Connection c = pg.getEmbeddedPostgres().getPostgresDatabase().getConnection();
         Statement s = c.createStatement()) {
      for (String statement : statements) {
        s.execute(statement);
      }
    }
  }
}
