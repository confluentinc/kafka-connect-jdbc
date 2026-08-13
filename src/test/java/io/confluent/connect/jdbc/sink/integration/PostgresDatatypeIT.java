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

import java.sql.Array;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

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
import java.util.concurrent.atomic.AtomicReference;

import io.confluent.common.utils.IntegrationTest;
import io.confluent.connect.jdbc.data.Json;
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
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.ConnectException;
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
import static org.apache.kafka.test.TestUtils.waitForCondition;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import java.math.BigDecimal;
import java.time.Instant;
import io.confluent.connect.jdbc.data.VariableScaleDecimal;
import org.apache.kafka.connect.data.Decimal;
import org.apache.kafka.connect.data.Time;
import org.apache.kafka.connect.data.Timestamp;
import static org.junit.Assert.assertNotEquals;


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

  // ---------- json / jsonb, read from a real Postgres ----------

  private static final String JSON_DOC = "{\"bar\": \"baz\"}";

  /**
   * Both variants emit the logical JSON STRING, and the text is passed through untouched. The space
   * after the colon is asserted deliberately: for {@code json} Postgres stores the document verbatim,
   * so its survival proves the connector performs no normalization. Mirrors Debezium's
   * {@code schemasAndValuesForTextTypes}, which asserts the same spaced document for both columns.
   */
  @Test
  public void testJsonAndJsonbEmitLogicalJsonStringVerbatim() throws Exception {
    execute("CREATE TABLE " + tableName + "(id int, j json, jb jsonb)",
        "INSERT INTO " + tableName + " VALUES (1, '" + JSON_DOC + "'::json, '"
            + JSON_DOC + "'::jsonb)");

    Struct row = pollOneRow(complexTypesSourceProps("postgres"));

    // json: byte-identical to what was written, including the space.
    new SchemaAndValueField("j", Json.optionalSchema(), JSON_DOC).assertFor(row);
    // jsonb: Postgres re-canonicalizes on write, so compare parsed rather than byte-for-byte.
    SchemaAndValueField.jsonText("jb", Json.optionalSchema(),
        Collections.singletonMap("bar", "baz")).assertFor(row);
  }

  /**
   * A JSON document is opaque text, so shapes that are not objects must survive unchanged. hstore
   * cannot produce any of these — only a real {@code json} column can — so this has no Debezium
   * counterpart and is the widest gap their suite leaves open.
   */
  @Test
  public void testJsonPreservesNonObjectDocuments() throws Exception {
    execute("CREATE TABLE " + tableName + "(id int, j json)",
        "INSERT INTO " + tableName + " VALUES (1, '[1, 2, 3]'::json)",
        "INSERT INTO " + tableName + " VALUES (2, '\"a string\"'::json)",
        "INSERT INTO " + tableName + " VALUES (3, '42'::json)",
        "INSERT INTO " + tableName + " VALUES (4, 'true'::json)",
        "INSERT INTO " + tableName + " VALUES (5, 'null'::json)",
        "INSERT INTO " + tableName + " VALUES (6, '{\"a\": {\"b\": [1, null]}}'::json)");

    List<Struct> rows = pollRows(complexTypesSourceProps("postgres"));
    assertEquals(6, rows.size());

    new SchemaAndValueField("j", Json.optionalSchema(), "[1, 2, 3]").assertFor(rows.get(0));
    new SchemaAndValueField("j", Json.optionalSchema(), "\"a string\"").assertFor(rows.get(1));
    new SchemaAndValueField("j", Json.optionalSchema(), "42").assertFor(rows.get(2));
    new SchemaAndValueField("j", Json.optionalSchema(), "true").assertFor(rows.get(3));
    // The JSON literal null is a 4-character document, NOT a SQL NULL and NOT a Connect null.
    new SchemaAndValueField("j", Json.optionalSchema(), "null").assertFor(rows.get(4));
    new SchemaAndValueField("j", Json.optionalSchema(), "{\"a\": {\"b\": [1, null]}}")
        .assertFor(rows.get(5));
  }

  /**
   * A SQL NULL is a Connect null, which must stay distinguishable from the JSON literal
   * {@code null} asserted above.
   */
  @Test
  public void testJsonSqlNullIsConnectNull() throws Exception {
    execute("CREATE TABLE " + tableName + "(id int, j json, jb jsonb)",
        "INSERT INTO " + tableName + " VALUES (1, NULL, NULL)");

    Struct row = pollOneRow(complexTypesSourceProps("postgres"));
    new SchemaAndValueField("j", Json.optionalSchema(), null).assertFor(row);
    new SchemaAndValueField("jb", Json.optionalSchema(), null).assertFor(row);
  }

  /**
   * A NOT NULL json column is emitted as the non-optional logical schema, so the optionality of the
   * column survives into the topic. Debezium asserts the same distinction via {@code Json.schema()}
   * versus {@code Json.builder().optional().build()}.
   */
  @Test
  public void testNotNullJsonColumnEmitsNonOptionalSchema() throws Exception {
    execute("CREATE TABLE " + tableName + "(id int, j json NOT NULL)",
        "INSERT INTO " + tableName + " VALUES (1, '" + JSON_DOC + "'::json)");

    Struct row = pollOneRow(complexTypesSourceProps("postgres"));
    new SchemaAndValueField("j", Json.schema(), JSON_DOC).assertFor(row);
  }

  /**
   * The backward-compatibility guarantee for the feature flag: with the default {@code false} a
   * json/jsonb column stays an untagged STRING carrying the same text, so existing pipelines are
   * unchanged. This is the branch of {@code jsonSchema} that had no coverage at all.
   */
  @Test
  public void testJsonEmitsPlainStringWhenComplexTypesDisabled() throws Exception {
    execute("CREATE TABLE " + tableName + "(id int, j json, jb jsonb)",
        "INSERT INTO " + tableName + " VALUES (1, '" + JSON_DOC + "'::json, '"
            + JSON_DOC + "'::jsonb)");

    Map<String, String> sourceProps = complexTypesSourceProps("postgres");
    sourceProps.remove(JdbcSourceConnectorConfig.SQL_COMPLEX_TYPES_ENABLE_CONFIG);
    Struct row = pollOneRow(sourceProps);

    // Same value, but no logical name — the pre-feature behaviour.
    new SchemaAndValueField("j", Schema.OPTIONAL_STRING_SCHEMA, JSON_DOC).assertFor(row);
    assertNull("json must not be tagged while complex types are disabled",
        row.schema().field("j").schema().name());
  }

  /**
   * The sink half: a logical JSON STRING must auto-create a native {@code jsonb} column and land as
   * real jsonb, not text. Verified through jsonb operators, which only work on a genuine jsonb value.
   */
  @Test
  public void testWriteToTableWithJsonColumn() throws Exception {
    props.put(JdbcSinkConfig.AUTO_CREATE, "true");
    props.put(JdbcSinkConfig.SQL_COMPLEX_TYPES_ENABLE, "true");
    connect.configureConnector("jdbc-sink-connector", props);
    waitForConnectorToStart("jdbc-sink-connector", 1);

    final Schema schema = SchemaBuilder.struct().name("com.example.Doc")
        .field("name", Schema.STRING_SCHEMA)
        .field("payload", Json.optionalSchema())
        .build();
    produceRecord(schema, new Struct(schema)
        .put("name", "doc-1")
        .put("payload", "{\"env\":\"prod\",\"nested\":{\"n\":1}}"));

    waitForCommittedRecords("jdbc-sink-connector", Collections.singleton(tableName), 1, 1,
        TimeUnit.MINUTES.toMillis(2));

    try (Connection c = pg.getEmbeddedPostgres().getPostgresDatabase().getConnection();
         Statement s = c.createStatement()) {
      try (ResultSet rs = s.executeQuery(
          "SELECT data_type FROM information_schema.columns "
              + "WHERE table_name = '" + tableName + "' AND column_name = 'payload'")) {
        assertTrue(rs.next());
        assertEquals("jsonb", rs.getString(1));
      }
      try (ResultSet rs = s.executeQuery(
          "SELECT payload->>'env', payload->'nested'->>'n' FROM " + tableName)) {
        assertTrue(rs.next());
        assertEquals("prod", rs.getString(1));
        assertEquals("1", rs.getString(2));
      }
    }
  }

  private void produceRecord(Schema schema, Struct struct) {
    String kafkaValue = new String(jsonConverter.fromConnectData(tableName, schema, struct));
    connect.kafka().produce(tableName, null, kafkaValue);
  }

  // ---------- hstore, read from a real Postgres ----------

  private static final Schema HSTORE_MAP_SCHEMA = SchemaBuilder
      .map(Schema.STRING_SCHEMA, Schema.OPTIONAL_STRING_SCHEMA).optional().build();

  /**
   * Create an hstore table holding one row, and read it back in the given mode. Mirrors Debezium's
   * per-scenario structure, where each hstore case is its own named test over a dedicated fixture.
   */
  private Struct readHstore(String hstoreLiteral, String mode) throws Exception {
    execute("CREATE EXTENSION IF NOT EXISTS hstore",
        "CREATE TABLE " + tableName + "(id int, hs hstore)",
        "INSERT INTO " + tableName + " VALUES (1, '" + hstoreLiteral + "'::hstore)");
    return pollOneRow(complexTypesSourceProps("postgres",
        JdbcSourceConnectorConfig.HSTORE_HANDLING_MODE_CONFIG, mode));
  }

  @Test
  public void testHstoreSingleValueAsMap() throws Exception {
    new SchemaAndValueField("hs", HSTORE_MAP_SCHEMA, Collections.singletonMap("key", "val"))
        .assertFor(readHstore("\"key\" => \"val\"", "map"));
  }

  @Test
  public void testHstoreMultipleValuesAsMap() throws Exception {
    Map<String, String> expected = new LinkedHashMap<>();
    expected.put("key1", "val1");
    expected.put("key2", "val2");
    expected.put("key3", "val3");
    new SchemaAndValueField("hs", HSTORE_MAP_SCHEMA, expected).assertFor(
        readHstore("\"key1\" => \"val1\",\"key2\" => \"val2\",\"key3\" => \"val3\"", "map"));
  }

  /**
   * A NULL hstore <em>value</em> — distinct from the whole column being NULL. This is what guards the
   * choice of an optional value schema: were it non-optional, Connect would reject the null entry.
   */
  @Test
  public void testHstoreNullValueAsMap() throws Exception {
    Map<String, String> expected = new LinkedHashMap<>();
    expected.put("key1", "val1");
    expected.put("key2", null);
    new SchemaAndValueField("hs", HSTORE_MAP_SCHEMA, expected)
        .assertFor(readHstore("\"key1\" => \"val1\",\"key2\" => NULL", "map"));
  }

  /**
   * Spaces, {@code #} and a leading space inside a value must survive the driver's hstore parsing.
   * Same literal Debezium uses.
   */
  @Test
  public void testHstoreSpecialCharactersAsMap() throws Exception {
    Map<String, String> expected = new LinkedHashMap<>();
    expected.put("key_#1", "val 1");
    expected.put("key 2", " ##123 78");
    new SchemaAndValueField("hs", HSTORE_MAP_SCHEMA, expected)
        .assertFor(readHstore("\"key_#1\" => \"val 1\",\"key 2\" =>\" ##123 78\"", "map"));
  }

  @Test
  public void testHstoreSingleValueAsJsonString() throws Exception {
    SchemaAndValueField.jsonText("hs", Json.optionalSchema(),
        Collections.singletonMap("key", "val"))
        .assertFor(readHstore("\"key\" => \"val\"", "json"));
  }

  @Test
  public void testHstoreMultipleValuesAsJsonString() throws Exception {
    Map<String, String> expected = new LinkedHashMap<>();
    expected.put("key1", "val1");
    expected.put("key2", "val2");
    expected.put("key3", "val3");
    SchemaAndValueField.jsonText("hs", Json.optionalSchema(), expected).assertFor(
        readHstore("\"key1\" => \"val1\",\"key2\" => \"val2\",\"key3\" => \"val3\"", "json"));
  }

  /**
   * A NULL hstore value becomes an unquoted JSON {@code null} — not the string {@code "null"} and not
   * an omitted key.
   */
  @Test
  public void testHstoreNullValueAsJsonString() throws Exception {
    Map<String, String> expected = new LinkedHashMap<>();
    expected.put("key1", "val1");
    expected.put("key2", null);
    Struct row = readHstore("\"key1\" => \"val1\",\"key2\" => NULL", "json");
    SchemaAndValueField.jsonText("hs", Json.optionalSchema(), expected).assertFor(row);
    // Pin the literal form too: an unquoted null, so a consumer can tell it from the text "null".
    assertTrue("expected an unquoted JSON null, got " + row.get("hs"),
        ((String) row.get("hs")).contains("\"key2\":null"));
  }

  @Test
  public void testHstoreSpecialCharactersAsJsonString() throws Exception {
    Map<String, String> expected = new LinkedHashMap<>();
    expected.put("key_#1", "val 1");
    expected.put("key 2", " ##123 78");
    SchemaAndValueField.jsonText("hs", Json.optionalSchema(), expected)
        .assertFor(readHstore("\"key_#1\" => \"val 1\",\"key 2\" =>\" ##123 78\"", "json"));
  }

  /** An empty hstore is an empty map, not a null and not a dropped field. */
  @Test
  public void testEmptyHstore() throws Exception {
    new SchemaAndValueField("hs", HSTORE_MAP_SCHEMA, Collections.emptyMap())
        .assertFor(readHstore("", "map"));
    execute("DROP TABLE " + tableName);
    SchemaAndValueField.jsonText("hs", Json.optionalSchema(), Collections.emptyMap())
        .assertFor(readHstore("", "json"));
  }

  /** A SQL NULL hstore column is a Connect null in both modes. */
  @Test
  public void testHstoreSqlNullIsConnectNull() throws Exception {
    execute("CREATE EXTENSION IF NOT EXISTS hstore",
        "CREATE TABLE " + tableName + "(id int, hs hstore)",
        "INSERT INTO " + tableName + " VALUES (1, NULL)");

    new SchemaAndValueField("hs", HSTORE_MAP_SCHEMA, null)
        .assertFor(pollOneRow(complexTypesSourceProps("postgres",
            JdbcSourceConnectorConfig.HSTORE_HANDLING_MODE_CONFIG, "map")));
    new SchemaAndValueField("hs", Json.optionalSchema(), null)
        .assertFor(pollOneRow(complexTypesSourceProps("postgres",
            JdbcSourceConnectorConfig.HSTORE_HANDLING_MODE_CONFIG, "json")));
  }

  /**
   * Pin the driver contract a mocked ResultSet can only assume: pgjdbc returns a Map for an hstore
   * column while the type is visible on the search_path.
   */
  @Test
  public void testHstoreDriverReturnsMap() throws Exception {
    execute("CREATE EXTENSION IF NOT EXISTS hstore",
        "CREATE TABLE " + tableName + "(id int, hs hstore)",
        "INSERT INTO " + tableName + " VALUES (1, '\"k\" => \"v\"'::hstore)");

    try (Connection c = pg.getEmbeddedPostgres().getPostgresDatabase().getConnection();
         Statement s = c.createStatement();
         ResultSet rs = s.executeQuery("SELECT hs FROM " + tableName)) {
      assertTrue(rs.next());
      Object driverValue = rs.getObject(1);
      assertTrue("pgjdbc must return a Map for hstore, got " + driverValue.getClass().getName(),
          driverValue instanceof Map);
      assertEquals(Collections.singletonMap("k", "v"), driverValue);
    }
  }

  /** Backward compatibility: with the default flag, hstore keeps today's drop-with-WARN behaviour. */
  @Test
  public void testHstoreDroppedWhenComplexTypesDisabled() throws Exception {
    execute("CREATE EXTENSION IF NOT EXISTS hstore",
        "CREATE TABLE " + tableName + "(id int, hs hstore)",
        "INSERT INTO " + tableName + " VALUES (1, '\"k\" => \"v\"'::hstore)");

    Map<String, String> sourceProps = complexTypesSourceProps("postgres");
    sourceProps.remove(JdbcSourceConnectorConfig.SQL_COMPLEX_TYPES_ENABLE_CONFIG);
    Struct row = pollOneRow(sourceProps);

    assertFieldAbsent(row, "hs");
    assertEquals(1, row.get("id"));
  }

  /**
   * An hstore installed outside the connection's search_path is reported as {@code "ext"."hstore"}
   * and its values arrive as raw text rather than a decoded map. Both are normalised, so the column
   * maps exactly as an on-search_path one does — the extension's location is not the operator's
   * problem, matching Debezium, which strips the schema before its own catalog lookup.
   */
  @Test
  public void testHstoreOutsideSearchPathIsMappedInBothModes() throws Exception {
    execute("CREATE DATABASE offpath");
    executeIn("offpath",
        "CREATE SCHEMA ext",
        "CREATE EXTENSION hstore SCHEMA ext",
        "CREATE TABLE " + tableName + "(id int, hs ext.hstore, hsa ext.hstore[])",
        "INSERT INTO " + tableName + " VALUES (1, '\"k\" => \"v\",\"n\" => NULL'::ext.hstore, "
            + "ARRAY['\"a\" => \"1\"'::ext.hstore, ''::ext.hstore])");

    Map<String, String> expected = new LinkedHashMap<>();
    expected.put("k", "v");
    expected.put("n", null);

    // map mode: the raw text is parsed into the same map a resolved column would yield.
    Struct mapRow = pollOneRow(complexTypesSourceProps("offpath",
        JdbcSourceConnectorConfig.HSTORE_HANDLING_MODE_CONFIG, "map"));
    new SchemaAndValueField("hs", HSTORE_MAP_SCHEMA, expected).assertFor(mapRow);
    assertEquals("hstore[] elements must decode too",
        Arrays.asList(Collections.singletonMap("a", "1"), Collections.emptyMap()),
        mapRow.get("hsa"));

    // json mode: the same value, serialized.
    Struct jsonRow = pollOneRow(complexTypesSourceProps("offpath",
        JdbcSourceConnectorConfig.HSTORE_HANDLING_MODE_CONFIG, "json"));
    SchemaAndValueField.jsonText("hs", Json.optionalSchema(),
        parsedMapWithNull("k", "v", "n")).assertFor(jsonRow);

    // none still means skip, wherever the extension lives.
    assertFieldAbsent(pollOneRow(complexTypesSourceProps("offpath",
        JdbcSourceConnectorConfig.HSTORE_HANDLING_MODE_CONFIG, "none")), "hs");
  }

  /**
   * The whole feature end to end against an extension in its own schema: source reads
   * {@code ext.hstore} and {@code ext.hstore[]}, and the sink provisions and writes native hstore
   * columns in the same database. This is the case that previously failed the source task outright.
   */
  @Test
  public void testHstoreRoundTripWithExtensionOutsideSearchPath() throws Exception {
    final String database = "extroundtrip";
    execute("CREATE DATABASE " + database);
    executeIn(database,
        "CREATE SCHEMA ext",
        "CREATE EXTENSION hstore SCHEMA ext",
        "CREATE TABLE " + SRC_TABLE + "(id int PRIMARY KEY, hs ext.hstore, hsa ext.hstore[])",
        "INSERT INTO " + SRC_TABLE + " VALUES (1, "
            + "'\"key\" => \"val\",\"absent\" => NULL'::ext.hstore, "
            + "ARRAY['\"a\" => \"1\"'::ext.hstore, ''::ext.hstore])",
        "INSERT INTO " + SRC_TABLE + " VALUES (2, ''::ext.hstore, NULL)");

    Map<String, String> sourceExtras = new HashMap<>();
    sourceExtras.put(JdbcSourceConnectorConfig.CONNECTION_URL_CONFIG, jdbcUrl(database));
    sourceExtras.put(JdbcSourceConnectorConfig.SQL_COMPLEX_TYPES_ENABLE_CONFIG, "true");
    sourceExtras.put(JdbcSourceConnectorConfig.HSTORE_HANDLING_MODE_CONFIG, "map");
    Map<String, String> sinkExtras = new HashMap<>();
    sinkExtras.put(JdbcSinkConfig.CONNECTION_URL, jdbcUrl(database));
    sinkExtras.put(JdbcSinkConfig.SQL_COMPLEX_TYPES_ENABLE, "true");

    runRoundTrip(2, sourceExtras, sinkExtras);

    // Both destination columns are the real extension type, not jsonb or text.
    assertEquals("hstore", queryOne(database,
        "SELECT udt_name FROM information_schema.columns WHERE table_name = '" + DST_TABLE
            + "' AND column_name = 'hs'"));
    assertEquals("_hstore", queryOne(database,
        "SELECT udt_name FROM information_schema.columns WHERE table_name = '" + DST_TABLE
            + "' AND column_name = 'hsa'"));

    // Values survive, read back through the extension's own operators.
    assertEquals("val", queryOne(database, "SELECT hs OPERATOR(ext.->) 'key' FROM " + DST_TABLE
        + " WHERE id = 1"));
    assertNull("a NULL hstore value stays NULL", queryOne(database,
        "SELECT hs OPERATOR(ext.->) 'absent' FROM " + DST_TABLE + " WHERE id = 1"));
    assertEquals("the key is still present", "2", queryOne(database,
        "SELECT array_length(ext.akeys(hs), 1)::text FROM " + DST_TABLE + " WHERE id = 1"));
    assertEquals("1", queryOne(database,
        "SELECT hsa[1] OPERATOR(ext.->) 'a' FROM " + DST_TABLE + " WHERE id = 1"));
    // An empty hstore stays empty rather than becoming NULL, and a NULL array stays NULL.
    assertEquals("0", queryOne(database,
        "SELECT coalesce(array_length(ext.akeys(hs), 1), 0)::text FROM " + DST_TABLE
            + " WHERE id = 2"));
    assertNull(queryOne(database, "SELECT hsa::text FROM " + DST_TABLE + " WHERE id = 2"));
  }

  /**
   * Writing into a hand-created hstore column, rather than one this connector provisioned, and with
   * the extension off the search_path so the cast has to carry the qualified type name.
   */
  @Test
  public void testWriteToPreExistingHstoreColumnOutsideSearchPath() throws Exception {
    final String database = "extexisting";
    execute("CREATE DATABASE " + database);
    executeIn(database,
        "CREATE SCHEMA ext",
        "CREATE EXTENSION hstore SCHEMA ext",
        "CREATE TABLE " + tableName + "(name text, tags ext.hstore)");

    props.put(JdbcSinkConfig.CONNECTION_URL, jdbcUrl(database));
    props.put(JdbcSinkConfig.SQL_COMPLEX_TYPES_ENABLE, "true");
    connect.configureConnector("jdbc-sink-connector", props);
    waitForConnectorToStart("jdbc-sink-connector", 1);

    produceRecord(HSTORE_SINK_SCHEMA, new Struct(HSTORE_SINK_SCHEMA)
        .put("name", "web-1")
        .put("tags", Collections.singletonMap("env", "prod")));

    waitForCommittedRecords("jdbc-sink-connector", Collections.singleton(tableName), 1, 1,
        TimeUnit.MINUTES.toMillis(2));

    assertEquals("prod", queryOne(database,
        "SELECT tags OPERATOR(ext.->) 'env' FROM " + tableName));
  }

  /**
   * A map is written as hstore text, so an existing column of another type has to be refused with a
   * message naming it, rather than letting PostgreSQL report a JSON syntax error.
   */
  @Test
  public void testMapIntoNonHstoreColumnIsRefused() throws Exception {
    execute("CREATE EXTENSION IF NOT EXISTS hstore",
        "CREATE TABLE " + tableName + "(name text, tags jsonb)");
    props.put(JdbcSinkConfig.SQL_COMPLEX_TYPES_ENABLE, "true");
    props.put(MAX_RETRIES, "0");
    connect.configureConnector("jdbc-sink-connector", props);
    waitForConnectorToStart("jdbc-sink-connector", 1);

    produceRecord(HSTORE_SINK_SCHEMA, new Struct(HSTORE_SINK_SCHEMA)
        .put("name", "web-1")
        .put("tags", Collections.singletonMap("env", "prod")));

    // The remediation has to name a type the operator can create, not just report the mismatch.
    assertTasksFailedWithTrace("jdbc-sink-connector", 1, "Recreate the column as hstore");
  }

  /**
   * The array form of the same refusal. This is the upgrade path: an earlier build wrote maps to
   * jsonb, so a pipeline that predates native hstore already has a {@code jsonb[]} column here. The
   * message must name {@code hstore[]} — pgjdbc reports these types as {@code _jsonb} and
   * {@code _hstore}, and telling an operator to recreate the column "as hstore" would give them a
   * scalar that fails the very next batch.
   */
  @Test
  public void testMapArrayIntoNonHstoreArrayColumnIsRefused() throws Exception {
    execute("CREATE EXTENSION IF NOT EXISTS hstore",
        "CREATE TABLE " + tableName + "(name text, tags jsonb[])");
    props.put(JdbcSinkConfig.SQL_COMPLEX_TYPES_ENABLE, "true");
    props.put(MAX_RETRIES, "0");
    connect.configureConnector("jdbc-sink-connector", props);
    waitForConnectorToStart("jdbc-sink-connector", 1);

    produceRecord(HSTORE_ARRAY_SINK_SCHEMA, new Struct(HSTORE_ARRAY_SINK_SCHEMA)
        .put("name", "web-1")
        .put("tags", Collections.singletonList(Collections.singletonMap("env", "prod"))));

    assertTasksFailedWithTrace("jdbc-sink-connector", 1, "Recreate the column as hstore[]");
  }

  /**
   * A missing extension is a property of the database, so no record shape can succeed and the task
   * fails rather than reporting records one at a time. Reached through the real catalog lookup: the
   * database is created without the extension rather than the resolved state being forced.
   */
  @Test
  public void testMapArrayFailsWhenExtensionIsNotInstalled() throws Exception {
    execute("CREATE DATABASE nohstorearray");
    props.put(JdbcSinkConfig.CONNECTION_URL, jdbcUrl("nohstorearray"));
    props.put(JdbcSinkConfig.AUTO_CREATE, "true");
    props.put(JdbcSinkConfig.SQL_COMPLEX_TYPES_ENABLE, "true");
    props.put(MAX_RETRIES, "0");
    connect.configureConnector("jdbc-sink-connector", props);
    waitForConnectorToStart("jdbc-sink-connector", 1);

    produceRecord(HSTORE_ARRAY_SINK_SCHEMA, new Struct(HSTORE_ARRAY_SINK_SCHEMA)
        .put("name", "web-1")
        .put("tags", Collections.singletonList(Collections.singletonMap("env", "prod"))));

    assertTasksFailedWithTrace("jdbc-sink-connector", 1, "CREATE EXTENSION hstore");
  }

  /** Execute statements against a named database rather than the default one. */
  private void executeIn(String database, String... statements) throws SQLException {
    try (Connection c = pg.getEmbeddedPostgres().getDatabase("postgres", database).getConnection();
         Statement s = c.createStatement()) {
      for (String statement : statements) {
        s.execute(statement);
      }
    }
  }

  /** The first column of the single row the query returns, from a named database. */
  private String queryOne(String database, String sql) throws SQLException {
    try (Connection c = pg.getEmbeddedPostgres().getDatabase("postgres", database).getConnection();
         Statement s = c.createStatement();
         ResultSet rs = s.executeQuery(sql)) {
      assertTrue("query returned no rows: " + sql, rs.next());
      return rs.getString(1);
    }
  }

  /** An expected parsed JSON object whose last named key carries a JSON null. */
  private static Map<String, Object> parsedMapWithNull(String key, String value, String nullKey) {
    Map<String, Object> map = new LinkedHashMap<>();
    map.put(key, value);
    map.put(nullKey, null);
    return map;
  }

  /**
   * Run a source task until polling throws, returning what it threw. The querier runs on a
   * background thread, so the failure is recorded by {@code RecordQueue.failWith} and rethrown,
   * wrapped, from a later poll rather than the first one.
   */
  private Throwable pollUntilTaskFails(Map<String, String> sourceProps) throws Exception {
    JdbcSourceTask task = new JdbcSourceTask();
    AtomicReference<Throwable> thrown = new AtomicReference<>();
    try {
      task.start(sourceProps);
      waitForCondition(() -> {
        try {
          task.poll();
          return false;
        } catch (InterruptedException e) {
          throw new RuntimeException(e);
        } catch (Throwable e) {
          thrown.set(e);
          return true;
        }
      }, 60_000, "the source task did not fail in time");
    } finally {
      task.stop();
    }
    return thrown.get();
  }

  /** The messages of a whole cause chain, since the original failure is wrapped on the way out. */
  private static String causeChain(Throwable thrown) {
    StringBuilder messages = new StringBuilder();
    for (Throwable cause = thrown; cause != null; cause = cause.getCause()) {
      messages.append(cause.getMessage()).append(' ');
    }
    return messages.toString();
  }

  /**
   * The sink half: a Connect {@code MAP<STRING,STRING>} auto-creates a native hstore column and
   * lands as real hstore, verified through hstore operators.
   */
  @Test
  public void testWriteToTableWithHstoreMapColumn() throws Exception {
    execute("CREATE EXTENSION IF NOT EXISTS hstore");
    props.put(JdbcSinkConfig.AUTO_CREATE, "true");
    props.put(JdbcSinkConfig.SQL_COMPLEX_TYPES_ENABLE, "true");
    connect.configureConnector("jdbc-sink-connector", props);
    waitForConnectorToStart("jdbc-sink-connector", 1);

    final Schema schema = SchemaBuilder.struct().name("com.example.Server")
        .field("name", Schema.STRING_SCHEMA)
        .field("tags", HSTORE_MAP_SCHEMA)
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
          "SELECT udt_name FROM information_schema.columns "
              + "WHERE table_name = '" + tableName + "' AND column_name = 'tags'")) {
        assertTrue(rs.next());
        assertEquals("hstore", rs.getString(1));
      }
      try (ResultSet rs = s.executeQuery(
          "SELECT tags->'env', tags->'cities' FROM " + tableName)) {
        assertTrue(rs.next());
        assertEquals("prod", rs.getString(1));
        assertEquals("Pune, Mumbai", rs.getString(2));
      }
    }
  }

  /**
   * The extension may be installed in its own schema and kept off the search_path, which is common
   * where extensions are segregated. The sink resolves where it lives and writes the qualified type
   * name, so auto-create still produces a real hstore column.
   */
  @Test
  public void testWriteToTableWithHstoreInstalledInAnotherSchema() throws Exception {
    execute("CREATE DATABASE extsink");
    try (Connection c = pg.getEmbeddedPostgres().getDatabase("postgres", "extsink").getConnection();
         Statement s = c.createStatement()) {
      s.execute("CREATE SCHEMA ext");
      s.execute("CREATE EXTENSION hstore SCHEMA ext");
    }
    props.put(JdbcSinkConfig.CONNECTION_URL, jdbcUrl("extsink"));
    props.put(JdbcSinkConfig.AUTO_CREATE, "true");
    props.put(JdbcSinkConfig.SQL_COMPLEX_TYPES_ENABLE, "true");
    connect.configureConnector("jdbc-sink-connector", props);
    waitForConnectorToStart("jdbc-sink-connector", 1);

    produceRecord(HSTORE_SINK_SCHEMA, new Struct(HSTORE_SINK_SCHEMA)
        .put("name", "web-1")
        .put("tags", Collections.singletonMap("env", "prod")));

    waitForCommittedRecords("jdbc-sink-connector", Collections.singleton(tableName), 1, 1,
        TimeUnit.MINUTES.toMillis(2));

    try (Connection c = pg.getEmbeddedPostgres().getDatabase("postgres", "extsink").getConnection();
         Statement s = c.createStatement()) {
      try (ResultSet rs = s.executeQuery("SELECT udt_name FROM information_schema.columns "
          + "WHERE table_name = '" + tableName + "' AND column_name = 'tags'")) {
        assertTrue(rs.next());
        assertEquals("hstore", rs.getString(1));
      }
      try (ResultSet rs = s.executeQuery(
          "SELECT tags OPERATOR(ext.->) 'env' FROM " + tableName)) {
        assertTrue(rs.next());
        assertEquals("prod", rs.getString(1));
      }
    }
  }

  /**
   * Selected but unavailable must fail loudly: without the extension there is no column type that
   * could hold the map, so the task fails with an actionable message rather than inventing one.
   */
  @Test
  public void testHstoreSinkFailsWhenExtensionIsNotInstalled() throws Exception {
    execute("CREATE DATABASE nohstore");
    props.put(JdbcSinkConfig.CONNECTION_URL, jdbcUrl("nohstore"));
    props.put(JdbcSinkConfig.AUTO_CREATE, "true");
    props.put(JdbcSinkConfig.SQL_COMPLEX_TYPES_ENABLE, "true");
    props.put(MAX_RETRIES, "0");
    connect.configureConnector("jdbc-sink-connector", props);
    waitForConnectorToStart("jdbc-sink-connector", 1);

    produceRecord(HSTORE_SINK_SCHEMA, new Struct(HSTORE_SINK_SCHEMA)
        .put("name", "web-1")
        .put("tags", Collections.singletonMap("env", "prod")));

    assertTasksFailedWithTrace("jdbc-sink-connector", 1, "CREATE EXTENSION hstore");
  }

  private static final Schema HSTORE_SINK_SCHEMA = SchemaBuilder.struct().name("com.example.Server")
      .field("name", Schema.STRING_SCHEMA)
      .field("tags", HSTORE_MAP_SCHEMA)
      .build();

  private static final Schema HSTORE_ARRAY_SINK_SCHEMA = SchemaBuilder.struct()
      .name("com.example.ServerTags")
      .field("name", Schema.STRING_SCHEMA)
      .field("tags", SchemaBuilder.array(HSTORE_MAP_SCHEMA).optional().build())
      .build();

  // ---------- hstore round trips: source -> Kafka -> sink ----------

  /**
   * Populate {@link #SRC_TABLE} with every hstore value scenario in one table, so a single round trip
   * covers them all: a single pair, several pairs, a NULL hstore <em>value</em>, special characters,
   * an empty hstore, a SQL NULL column, and a numeric-looking value.
   */
  private void createHstoreSourceRows() throws SQLException {
    execute("CREATE EXTENSION IF NOT EXISTS hstore",
        "CREATE TABLE " + SRC_TABLE + "(id int PRIMARY KEY, hs hstore)",
        "INSERT INTO " + SRC_TABLE + " VALUES (1, '\"key\" => \"val\"'::hstore)",
        "INSERT INTO " + SRC_TABLE + " VALUES "
            + "(2, '\"key1\" => \"val1\",\"key2\" => \"val2\",\"key3\" => \"val3\"'::hstore)",
        "INSERT INTO " + SRC_TABLE + " VALUES (3, '\"key1\" => \"val1\",\"key2\" => NULL'::hstore)",
        "INSERT INTO " + SRC_TABLE
            + " VALUES (4, '\"key_#1\" => \"val 1\",\"key 2\" =>\" ##123 78\"'::hstore)",
        "INSERT INTO " + SRC_TABLE + " VALUES (5, ''::hstore)",
        "INSERT INTO " + SRC_TABLE + " VALUES (6, NULL)",
        "INSERT INTO " + SRC_TABLE + " VALUES (7, '\"count\" => \"5\"'::hstore)");
  }

  /**
   * Assert the destination rows written by an hstore round trip in json mode, where the {@code Json}
   * string is a JSON document that happens to have come from hstore and lands in {@code jsonb}.
   */
  private void assertHstoreJsonRoundTripRows() throws SQLException {
    assertEquals("json mode must land in a native jsonb column", "jsonb", destColumnType("hs"));

    queryDest("id, hs, hs IS NULL AS is_null, jsonb_typeof(hs) AS kind", "id",
        rs -> {
          assertEquals(1, rs.getInt("id"));
          assertEquals("{\"key\": \"val\"}", rs.getString("hs"));
        },
        rs -> assertEquals(parsedMap("key1", "val1", "key2", "val2", "key3", "val3"),
            parseJson(rs.getString("hs"))),
        rs -> {
          // A NULL hstore value survives as a JSON null, with the key still present.
          Map<String, Object> expected = new LinkedHashMap<>();
          expected.put("key1", "val1");
          expected.put("key2", null);
          assertEquals(expected, parseJson(rs.getString("hs")));
        },
        rs -> assertEquals(parsedMap("key_#1", "val 1", "key 2", " ##123 78"),
            parseJson(rs.getString("hs"))),
        // Empty hstore is an empty JSON object, not NULL.
        rs -> {
          assertEquals("{}", rs.getString("hs"));
          assertEquals(false, rs.getBoolean("is_null"));
        },
        // SQL NULL column stays SQL NULL, distinct from both {} and the JSON literal null.
        rs -> {
          assertEquals(true, rs.getBoolean("is_null"));
          assertNull(rs.getString("kind"));
        },
        // hstore has no numeric type: "5" must remain a JSON string, never the number 5.
        rs -> {
          assertEquals("{\"count\": \"5\"}", rs.getString("hs"));
          assertEquals("string", jsonbTypeOfField("hs", "count", 7));
        });
  }

  /**
   * Map mode round trips into a native hstore column, matching Debezium, whose PostgreSQL sink
   * dialect maps the Connect MAP type to hstore unconditionally.
   */
  @Test
  public void testHstoreMapModeRoundTripsToHstore() throws Exception {
    createHstoreSourceRows();
    Map<String, String> sourceExtras = new HashMap<>();
    sourceExtras.put(JdbcSourceConnectorConfig.SQL_COMPLEX_TYPES_ENABLE_CONFIG, "true");
    sourceExtras.put(JdbcSourceConnectorConfig.HSTORE_HANDLING_MODE_CONFIG, "map");
    runRoundTrip(7, sourceExtras,
        Collections.singletonMap(JdbcSinkConfig.SQL_COMPLEX_TYPES_ENABLE, "true"));
    assertHstoreMapRoundTripRows();
  }

  /**
   * Assert the destination rows written by a map-mode hstore round trip: a real hstore column, so
   * the hstore operators apply, and every scenario from {@link #createHstoreSourceRows()} survives.
   */
  private void assertHstoreMapRoundTripRows() throws SQLException {
    assertEquals("map mode must land in a native hstore column", "USER-DEFINED",
        destColumnType("hs"));
    assertEquals("hstore", destColumnUdtName("hs"));

    // Read through the hstore operators rather than its text form, whose pair order is hash order.
    queryDest("id, hs::text AS text, hs IS NULL AS is_null, hs -> 'key2' AS key2, "
            + "array_length(akeys(hs), 1) AS pairs, hs -> 'count' AS count, "
            + "hs -> 'key_#1' AS hashed, hs -> 'key 2' AS spaced", "id",
        rs -> assertEquals("\"key\"=>\"val\"", rs.getString("text")),
        rs -> assertEquals(3, rs.getInt("pairs")),
        // A NULL hstore value survives as a NULL value, with the key still present.
        rs -> {
          assertNull(rs.getString("key2"));
          assertEquals(2, rs.getInt("pairs"));
        },
        // Spaces and # inside keys and values survive the hstore text round trip.
        rs -> {
          assertEquals("val 1", rs.getString("hashed"));
          assertEquals(" ##123 78", rs.getString("spaced"));
        },
        // Empty hstore stays an empty hstore, not NULL.
        rs -> {
          assertEquals("", rs.getString("text"));
          assertEquals(false, rs.getBoolean("is_null"));
        },
        // SQL NULL column stays SQL NULL, distinct from the empty hstore.
        rs -> assertEquals(true, rs.getBoolean("is_null")),
        // hstore is text to text: "5" must remain the string 5.
        rs -> assertEquals("5", rs.getString("count")));
  }

  /** The underlying type name of a destination column, e.g. {@code hstore} for a USER-DEFINED. */
  private String destColumnUdtName(String column) throws SQLException {
    try (Connection c = pg.getEmbeddedPostgres().getPostgresDatabase().getConnection();
         Statement s = c.createStatement();
         ResultSet rs = s.executeQuery(
             "SELECT udt_name FROM information_schema.columns WHERE table_name = '"
                 + DST_TABLE + "' AND column_name = '" + column + "'")) {
      assertTrue("destination table has no column " + column, rs.next());
      return rs.getString(1);
    }
  }

  @Test
  public void testHstoreSkippedWhenHandlingModeIsNone() throws Exception {
    // The complex types flag alone leaves hstore unmapped, since none is the default.
    execute("CREATE EXTENSION IF NOT EXISTS hstore",
        "CREATE TABLE " + tableName + "(id int, hs hstore)",
        "INSERT INTO " + tableName + " VALUES (1, '\"k\" => \"v\"'::hstore)");

    assertFieldAbsent(pollOneRow(complexTypesSourceProps("postgres")), "hs");
    assertFieldAbsent(pollOneRow(complexTypesSourceProps("postgres",
        JdbcSourceConnectorConfig.HSTORE_HANDLING_MODE_CONFIG, "none")), "hs");
  }

  @Test
  public void testHstoreJsonModeRoundTripsToJsonb() throws Exception {
    createHstoreSourceRows();
    Map<String, String> sourceExtras = new HashMap<>();
    sourceExtras.put(JdbcSourceConnectorConfig.SQL_COMPLEX_TYPES_ENABLE_CONFIG, "true");
    sourceExtras.put(JdbcSourceConnectorConfig.HSTORE_HANDLING_MODE_CONFIG, "json");

    runRoundTrip(7, sourceExtras,
        Collections.singletonMap(JdbcSinkConfig.SQL_COMPLEX_TYPES_ENABLE, "true"));
    assertHstoreJsonRoundTripRows();
  }

  /**
   * Backward compatibility, end to end: with the flag off on both connectors, an hstore column never
   * reaches the topic, so the destination table has no such column at all.
   */
  @Test
  public void testHstoreRoundTripDroppedWhenComplexTypesDisabled() throws Exception {
    createHstoreSourceRows();
    runRoundTrip(7);

    try (Connection c = pg.getEmbeddedPostgres().getPostgresDatabase().getConnection();
         Statement s = c.createStatement();
         ResultSet rs = s.executeQuery(
             "SELECT column_name FROM information_schema.columns WHERE table_name = '"
                 + DST_TABLE + "' AND column_name = 'hs'")) {
      assertTrue("hstore must not reach the destination while complex types are disabled",
          !rs.next());
    }
  }

  // ---------- json round trips: source -> Kafka -> sink ----------

  /**
   * Populate {@link #SRC_TABLE} with every document shape in one table: an object, an array, a string
   * scalar, a number, a boolean, the JSON literal null, a nested document and a SQL NULL.
   */
  private void createJsonSourceRows() throws SQLException {
    execute("CREATE TABLE " + SRC_TABLE + "(id int PRIMARY KEY, j json, jb jsonb)",
        "INSERT INTO " + SRC_TABLE + " VALUES (1, '{\"bar\": \"baz\"}', '{\"bar\": \"baz\"}')",
        "INSERT INTO " + SRC_TABLE + " VALUES (2, '[1, 2, 3]', '[1, 2, 3]')",
        "INSERT INTO " + SRC_TABLE + " VALUES (3, '\"a string\"', '\"a string\"')",
        "INSERT INTO " + SRC_TABLE + " VALUES (4, '42', '42')",
        "INSERT INTO " + SRC_TABLE + " VALUES (5, 'true', 'true')",
        "INSERT INTO " + SRC_TABLE + " VALUES (6, 'null', 'null')",
        "INSERT INTO " + SRC_TABLE
            + " VALUES (7, '{\"a\": {\"b\": [1, null]}}', '{\"a\": {\"b\": [1, null]}}')",
        "INSERT INTO " + SRC_TABLE + " VALUES (8, NULL, NULL)");
  }

  /**
   * A full round trip for both json variants, covering every document shape. The literal {@code null}
   * document (row 6) and the SQL NULL column (row 8) are the pair most easily conflated: one is a
   * jsonb value of type {@code null}, the other is the absence of a value.
   */
  @Test
  public void testJsonRoundTripsAcrossDocumentShapes() throws Exception {
    createJsonSourceRows();
    runRoundTrip(8,
        Collections.singletonMap(
            JdbcSourceConnectorConfig.SQL_COMPLEX_TYPES_ENABLE_CONFIG, "true"),
        Collections.singletonMap(JdbcSinkConfig.SQL_COMPLEX_TYPES_ENABLE, "true"));

    assertEquals("json must land in a native jsonb column", "jsonb", destColumnType("j"));
    assertEquals("jsonb must land in a native jsonb column", "jsonb", destColumnType("jb"));

    queryDest("id, j, jb, j IS NULL AS j_null, jsonb_typeof(j) AS kind", "id",
        rs -> assertEquals(parsedMap("bar", "baz"), parseJson(rs.getString("j"))),
        rs -> {
          assertEquals("array", rs.getString("kind"));
          assertEquals(Arrays.asList(1, 2, 3), parseJson(rs.getString("j")));
        },
        rs -> {
          assertEquals("string", rs.getString("kind"));
          assertEquals("a string", parseJson(rs.getString("j")));
        },
        rs -> {
          assertEquals("number", rs.getString("kind"));
          assertEquals(42, parseJson(rs.getString("j")));
        },
        rs -> {
          assertEquals("boolean", rs.getString("kind"));
          assertEquals(true, parseJson(rs.getString("j")));
        },
        // The JSON literal null is a jsonb value of type "null" — NOT a SQL NULL.
        rs -> {
          assertEquals("null", rs.getString("kind"));
          assertEquals(false, rs.getBoolean("j_null"));
        },
        rs -> assertEquals(parseJson("{\"a\":{\"b\":[1,null]}}"), parseJson(rs.getString("j"))),
        // A SQL NULL column stays SQL NULL, with no jsonb type at all.
        rs -> {
          assertEquals(true, rs.getBoolean("j_null"));
          assertNull(rs.getString("kind"));
        });
  }

  /**
   * Backward compatibility end to end: with the flag off on both connectors, json/jsonb reach the
   * destination as plain {@code text}, exactly as before the feature existed.
   */
  @Test
  public void testJsonRoundTripLandsInTextWhenComplexTypesDisabled() throws Exception {
    createJsonSourceRows();
    runRoundTrip(8);

    assertEquals("json must stay text while complex types are disabled", "text",
        destColumnType("j"));
    assertEquals("jsonb must stay text while complex types are disabled", "text",
        destColumnType("jb"));
    queryDest("id, j", "id",
        rs -> assertEquals("{\"bar\": \"baz\"}", rs.getString("j")),
        rs -> assertEquals("[1, 2, 3]", rs.getString("j")),
        rs -> assertEquals("\"a string\"", rs.getString("j")),
        rs -> assertEquals("42", rs.getString("j")),
        rs -> assertEquals("true", rs.getString("j")),
        rs -> assertEquals("null", rs.getString("j")),
        rs -> assertEquals("{\"a\": {\"b\": [1, null]}}", rs.getString("j")),
        rs -> assertNull(rs.getString("j")));
  }

  /**
   * The upgrade asymmetry: the source is upgraded and has the flag on, the sink still has it off. The
   * topic carries the {@code Json} logical type but the sink ignores it, so the value lands in
   * {@code text} rather than {@code jsonb} — degraded, but the document itself is not lost.
   */
  @Test
  public void testSourceEnabledSinkDisabledLandsInTextWithoutDataLoss() throws Exception {
    createJsonSourceRows();
    runRoundTrip(8,
        Collections.singletonMap(
            JdbcSourceConnectorConfig.SQL_COMPLEX_TYPES_ENABLE_CONFIG, "true"),
        Collections.emptyMap());

    assertEquals("a sink with the flag off must fall back to text", "text", destColumnType("j"));
    queryDest("id, j", "id",
        rs -> assertEquals("{\"bar\": \"baz\"}", rs.getString("j")),
        rs -> assertEquals("[1, 2, 3]", rs.getString("j")),
        rs -> assertEquals("\"a string\"", rs.getString("j")),
        rs -> assertEquals("42", rs.getString("j")),
        rs -> assertEquals("true", rs.getString("j")),
        rs -> assertEquals("null", rs.getString("j")),
        rs -> assertEquals("{\"a\": {\"b\": [1, null]}}", rs.getString("j")),
        rs -> assertNull(rs.getString("j")));
  }

  /**
   * The reverse asymmetry: the source has the flag off so json arrives as an untagged STRING, and an
   * enabled sink has nothing to recognise — it must still land in {@code text}, not guess.
   */
  @Test
  public void testSourceDisabledSinkEnabledLandsInText() throws Exception {
    createJsonSourceRows();
    runRoundTrip(8, Collections.emptyMap(),
        Collections.singletonMap(JdbcSinkConfig.SQL_COMPLEX_TYPES_ENABLE, "true"));

    assertEquals("an untagged STRING must not be promoted to jsonb", "text", destColumnType("j"));
  }

  // ---------- sink rejection of unexpected shapes (DLQ) ----------

  private void configureDlqSink(String... extras) throws Exception {
    props.put(ERRORS_TOLERANCE_CONFIG, ToleranceType.ALL.value());
    props.put(DLQ_TOPIC_NAME_CONFIG, DLQ_TOPIC_NAME);
    props.put(DLQ_TOPIC_REPLICATION_FACTOR_CONFIG, "1");
    props.put(MAX_RETRIES, "0");
    props.put(JdbcSinkConfig.AUTO_CREATE, "true");
    props.put(JdbcSinkConfig.SQL_COMPLEX_TYPES_ENABLE, "true");
    for (int i = 0; i < extras.length; i += 2) {
      props.put(extras[i], extras[i + 1]);
    }
    connect.configureConnector("jdbc-sink-connector", props);
    waitForConnectorToStart("jdbc-sink-connector", 1);
  }

  /**
   * A malformed JSON document under the {@code Json} logical type cannot be cast to jsonb, so the
   * record must be reported rather than silently corrupting the column.
   */
  @Test
  public void testSinkReportsMalformedJsonText() throws Exception {
    configureDlqSink();

    final Schema schema = SchemaBuilder.struct().name("com.example.Doc")
        .field("name", Schema.STRING_SCHEMA)
        .field("payload", Json.optionalSchema())
        .build();
    produceRecord(schema, new Struct(schema).put("name", "bad").put("payload", "{not json"));

    ConsumerRecords<byte[], byte[]> dlq =
        connect.kafka().consume(1, CONSUME_MAX_DURATION_MS, DLQ_TOPIC_NAME);
    assertEquals("malformed JSON must reach the DLQ", 1, dlq.count());
  }

  /**
   * An untagged STRING written into a pre-existing jsonb column: the sink binds it as text and the
   * {@code ::jsonb} cast applies, so a valid document still lands correctly.
   */
  @Test
  public void testSinkWritesPlainStringIntoExistingJsonbColumn() throws Exception {
    execute("CREATE TABLE " + tableName + "(name text, payload jsonb)");
    props.put(JdbcSinkConfig.SQL_COMPLEX_TYPES_ENABLE, "true");
    connect.configureConnector("jdbc-sink-connector", props);
    waitForConnectorToStart("jdbc-sink-connector", 1);

    final Schema schema = SchemaBuilder.struct().name("com.example.Doc")
        .field("name", Schema.STRING_SCHEMA)
        .field("payload", Schema.OPTIONAL_STRING_SCHEMA)
        .build();
    produceRecord(schema, new Struct(schema)
        .put("name", "plain").put("payload", "{\"env\":\"prod\"}"));

    waitForCommittedRecords("jdbc-sink-connector", Collections.singleton(tableName), 1, 1,
        TimeUnit.MINUTES.toMillis(2));

    try (Connection c = pg.getEmbeddedPostgres().getPostgresDatabase().getConnection();
         Statement s = c.createStatement();
         ResultSet rs = s.executeQuery("SELECT payload->>'env' FROM " + tableName)) {
      assertTrue(rs.next());
      assertEquals("prod", rs.getString(1));
    }
  }

  private static Map<String, Object> parsedMap(String... kv) {
    Map<String, Object> map = new LinkedHashMap<>();
    for (int i = 0; i < kv.length; i += 2) {
      map.put(kv[i], kv[i + 1]);
    }
    return map;
  }

  private static Object parseJson(String text) {
    try {
      return new ObjectMapper().readValue(text, Object.class);
    } catch (Exception e) {
      throw new AssertionError("not parseable JSON: " + text, e);
    }
  }

  /** {@code jsonb_typeof} of one field inside the destination document, e.g. "string". */
  private String jsonbTypeOfField(String column, String field, int id) throws SQLException {
    try (Connection c = pg.getEmbeddedPostgres().getPostgresDatabase().getConnection();
         Statement s = c.createStatement();
         ResultSet rs = s.executeQuery("SELECT jsonb_typeof(" + column + "->'" + field + "') FROM "
             + DST_TABLE + " WHERE id = " + id)) {
      assertTrue(rs.next());
      return rs.getString(1);
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
    return jdbcUrl("postgres");
  }

  protected String jdbcUrl(String database) {
    return String.format("jdbc:postgresql://localhost:%s/%s",
        pg.getEmbeddedPostgres().getPort(), database);
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

  private void createTableWithComplexArrayColumns() throws SQLException {
    try (Connection c = pg.getEmbeddedPostgres().getPostgresDatabase().getConnection()) {
      try (Statement s = c.createStatement()) {
        s.execute(String.format(
            "CREATE TABLE %s(nums numeric[], docs jsonb[])", tableName));
      }
    }
  }

  /** An optional Connect ARRAY of the given element schema, the shape the source always emits. */
  private static Schema arrayOf(Schema elementSchema) {
    return SchemaBuilder.array(elementSchema).optional().build();
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
    execute("CREATE EXTENSION IF NOT EXISTS hstore");
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
    expectedUdtNames.put("a_hstore", "_hstore");
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

  /** Map mode provisions a native {@code hstore[]}, so the elements stay real hstores. */
  private void assertHstoreArrayMapRoundTripRows() throws SQLException {
    assertEquals("hstore[] must land in a native hstore[] column",
        "_hstore", columnUdtName(DST_TABLE, "hs"));
    // A NULL hstore value keeps its key with a NULL value; an empty hstore stays empty, not NULL.
    assertDestArrayText(
        "hs[1]->'env', hs[2]->'k', array_length(akeys(hs[2]), 1)::text, hs[3]::text",
        "prod", null, "1", "");
  }

  /** Json mode carries JSON documents, so the elements provision {@code jsonb[]} as before. */
  private void assertHstoreArrayJsonRoundTripRows() throws SQLException {
    assertEquals("a Json string array must land in a native jsonb[] column",
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

    Map<String, String> sourceExtras = new HashMap<>();
    sourceExtras.put(JdbcSourceConnectorConfig.SQL_COMPLEX_TYPES_ENABLE_CONFIG, "true");
    sourceExtras.put(JdbcSourceConnectorConfig.HSTORE_HANDLING_MODE_CONFIG, "map");

    runRoundTrip(1, sourceExtras,
        Collections.singletonMap(JdbcSinkConfig.SQL_COMPLEX_TYPES_ENABLE, "true"));

    assertHstoreArrayMapRoundTripRows();
  }

  @Test
  public void testHstoreArraySkippedWhenHandlingModeIsNone() throws Exception {
    // The hstore mode governs hstore[] too, so the array column is skipped while a plain int[]
    // column alongside it is unaffected.
    execute("CREATE EXTENSION IF NOT EXISTS hstore",
        "CREATE TABLE " + tableName + "(id int, hs hstore[], nums int[])",
        "INSERT INTO " + tableName + " VALUES (1, ARRAY['\"k\" => \"v\"'::hstore], ARRAY[1, 2])");

    Struct row = pollOneRow(complexTypesSourceProps("postgres"));
    assertFieldAbsent(row, "hs");
    assertEquals(Arrays.asList(1, 2), row.get("nums"));
  }

  @Test
  public void testHstoreArrayJsonModeRoundTrip() throws Exception {
    createHstoreArraySourceRows();

    Map<String, String> sourceExtras = new HashMap<>();
    sourceExtras.put(JdbcSourceConnectorConfig.SQL_COMPLEX_TYPES_ENABLE_CONFIG, "true");
    sourceExtras.put(JdbcSourceConnectorConfig.HSTORE_HANDLING_MODE_CONFIG, "json");

    runRoundTrip(1, sourceExtras,
        Collections.singletonMap(JdbcSinkConfig.SQL_COMPLEX_TYPES_ENABLE, "true"));

    assertHstoreArrayJsonRoundTripRows();
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
}
