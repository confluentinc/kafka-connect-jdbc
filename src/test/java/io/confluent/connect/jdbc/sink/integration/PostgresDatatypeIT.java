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

import java.sql.Array;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Collections;
import java.util.Map;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import io.confluent.common.utils.IntegrationTest;
import io.confluent.connect.jdbc.data.Json;
import io.confluent.connect.jdbc.integration.BaseConnectorIT;
import io.confluent.connect.jdbc.sink.JdbcSinkConfig;
import io.confluent.connect.jdbc.source.JdbcSourceConnectorConfig;
import io.confluent.connect.jdbc.source.JdbcSourceTask;
import io.confluent.connect.jdbc.source.JdbcSourceTaskConfig;

import io.zonky.test.db.postgres.junit.EmbeddedPostgresRules;
import io.zonky.test.db.postgres.junit.SingleInstancePostgresRule;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.connect.data.Date;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.json.JsonConverter;
import org.apache.kafka.connect.runtime.errors.ToleranceType;
import org.apache.kafka.connect.source.SourceRecord;
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

  /**
   * The sink half: a Connect {@code MAP<STRING,STRING>} — the shape an hstore column takes on the
   * topic in map mode — must auto-create a native {@code JSONB} column and land as valid jsonb via
   * the {@code ::jsonb} cast.
   */
  @Test
  public void testWriteToTableWithHstoreMapColumn() throws Exception {
    props.put(JdbcSinkConfig.AUTO_CREATE, "true");
    props.put(JdbcSinkConfig.SQL_COMPLEX_TYPES_ENABLE, "true");
    connect.configureConnector("jdbc-sink-connector", props);
    waitForConnectorToStart("jdbc-sink-connector", 1);

    final Schema schema = SchemaBuilder.struct().name("com.example.Server")
        .field("name", Schema.STRING_SCHEMA)
        .field("tags", SchemaBuilder.map(Schema.STRING_SCHEMA, Schema.OPTIONAL_STRING_SCHEMA)
            .optional().build())
        .build();

    Map<String, String> tags = new LinkedHashMap<>();
    tags.put("env", "prod");
    tags.put("cities", "Pune, Mumbai");
    produceRecord(schema, new Struct(schema).put("name", "web-1").put("tags", tags));

    waitForCommittedRecords("jdbc-sink-connector", Collections.singleton(tableName), 1, 1,
        TimeUnit.MINUTES.toMillis(2));

    try (Connection c = pg.getEmbeddedPostgres().getPostgresDatabase().getConnection();
         Statement s = c.createStatement()) {
      try (ResultSet rs = s.executeQuery(
          "SELECT data_type FROM information_schema.columns "
              + "WHERE table_name = '" + tableName + "' AND column_name = 'tags'")) {
        assertTrue(rs.next());
        assertEquals("jsonb", rs.getString(1));
      }
      // Read it back through jsonb operators, which only work if the value really is jsonb.
      try (ResultSet rs = s.executeQuery(
          "SELECT tags->>'env', tags->>'cities' FROM " + tableName)) {
        assertTrue(rs.next());
        assertEquals("prod", rs.getString(1));
        assertEquals("Pune, Mumbai", rs.getString(2));
      }
    }
  }

  // ---------- hstore, read from a real Postgres rather than a mocked ResultSet ----------

  private Map<String, String> hstoreSourceProps(String db, String... extras) {
    Map<String, String> sourceProps = new HashMap<>();
    sourceProps.put(JdbcSourceConnectorConfig.CONNECTION_URL_CONFIG, String.format(
        "jdbc:postgresql://localhost:%s/%s", pg.getEmbeddedPostgres().getPort(), db));
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

  private Struct pollOneRow(Map<String, String> sourceProps) throws InterruptedException {
    JdbcSourceTask task = new JdbcSourceTask();
    try {
      task.start(sourceProps);
      List<SourceRecord> records = task.poll();
      assertEquals(1, records.size());
      return (Struct) records.get(0).value();
    } finally {
      task.stop();
    }
  }

  /**
   * Establishes what pgjdbc actually returns for an hstore column, for both handling modes. The
   * value deliberately contains a comma, an {@code =>} and a SQL NULL, so a mis-read cannot pass.
   */
  @Test
  public void testHstoreSourceEmitsMapAndJsonPerMode() throws Exception {
    createTable("CREATE EXTENSION IF NOT EXISTS hstore; CREATE TABLE %s(tags hstore)");
    try (Connection c = pg.getEmbeddedPostgres().getPostgresDatabase().getConnection();
         Statement s = c.createStatement()) {
      s.execute("INSERT INTO " + tableName + " VALUES (hstore("
          + "ARRAY['env','cities','absent'], ARRAY['prod','Pune, Mumbai',NULL]))");

      // Pin the driver contract directly, which a mocked ResultSet can only assume: pgjdbc
      // returns a Map for an hstore column while the type is visible on the search_path.
      try (ResultSet rs = s.executeQuery("SELECT tags FROM " + tableName)) {
        assertTrue(rs.next());
        Object driverValue = rs.getObject(1);
        assertTrue("pgjdbc must return a Map for hstore, got " + driverValue.getClass().getName(),
            driverValue instanceof Map);
      }
    }

    Map<String, String> expected = new LinkedHashMap<>();
    expected.put("env", "prod");
    expected.put("cities", "Pune, Mumbai");
    expected.put("absent", null);

    // map mode (default): the driver's Map, with the SQL NULL preserved as a null value.
    Struct mapMode = pollOneRow(hstoreSourceProps("postgres"));
    assertEquals(Schema.Type.MAP, mapMode.schema().field("tags").schema().type());
    assertEquals(expected, mapMode.get("tags"));

    // json mode: a JSON-object STRING. Compared parsed, since the driver returns a HashMap and
    // key order is therefore hash order rather than insertion order.
    Struct jsonMode = pollOneRow(hstoreSourceProps("postgres",
        JdbcSourceConnectorConfig.HSTORE_HANDLING_MODE_CONFIG,
        JdbcSourceConnectorConfig.HSTORE_HANDLING_MODE_JSON));
    Schema jsonSchema = jsonMode.schema().field("tags").schema();
    assertEquals(Schema.Type.STRING, jsonSchema.type());
    assertEquals("json mode must tag the STRING so the sink provisions JSONB, not TEXT",
        Json.LOGICAL_NAME, jsonSchema.name());
    assertEquals(expected, new ObjectMapper()
        .readValue((String) jsonMode.get("tags"), Map.class));
  }

  /**
   * The backward-compatibility guarantee for the whole feature flag: with the default {@code false}
   * an hstore column keeps today's drop-with-WARN behaviour and produces no field on the topic.
   */
  @Test
  public void testHstoreDroppedWhenComplexTypesDisabled() throws Exception {
    createTable("CREATE EXTENSION IF NOT EXISTS hstore; CREATE TABLE %s(id int, tags hstore)");
    try (Connection c = pg.getEmbeddedPostgres().getPostgresDatabase().getConnection();
         Statement s = c.createStatement()) {
      s.execute("INSERT INTO " + tableName + " VALUES (1, 'env=>prod'::hstore)");
    }

    Map<String, String> sourceProps = hstoreSourceProps("postgres");
    sourceProps.remove(JdbcSourceConnectorConfig.SQL_COMPLEX_TYPES_ENABLE_CONFIG);
    Struct value = pollOneRow(sourceProps);

    assertNull("hstore must not reach the topic while complex types are disabled",
        value.schema().field("tags"));
    assertEquals(1, value.get("id"));
  }

  /**
   * An hstore type outside the connection's {@code search_path} is reported by the driver as
   * {@code "ext"."hstore"} and read as raw text rather than a Map, so the column is skipped rather
   * than mis-read. This is the limitation that keeps the map-mode value guard unreachable.
   */
  @Test
  public void testHstoreOutsideSearchPathIsSkipped() throws Exception {
    try (Connection c = pg.getEmbeddedPostgres().getPostgresDatabase().getConnection();
         Statement s = c.createStatement()) {
      s.execute("CREATE DATABASE offpath");
    }
    // A fresh database, so hstore can be installed into "ext" only. The default search_path is
    // "$user", public — so "ext" is deliberately not visible.
    try (Connection c = pg.getEmbeddedPostgres().getDatabase("postgres", "offpath").getConnection();
         Statement s = c.createStatement()) {
      s.execute("CREATE SCHEMA ext");
      s.execute("CREATE EXTENSION hstore SCHEMA ext");
      s.execute("CREATE TABLE " + tableName + "(id int, tags ext.hstore)");
      s.execute("INSERT INTO " + tableName + " VALUES (1, 'env=>prod'::ext.hstore)");
    }

    Struct value = pollOneRow(hstoreSourceProps("offpath"));
    assertNull("an hstore type off the search_path must be skipped, not mis-read",
        value.schema().field("tags"));
  }
}
