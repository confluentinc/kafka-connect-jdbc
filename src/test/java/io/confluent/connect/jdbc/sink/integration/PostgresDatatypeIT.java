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
import java.util.Map;
import java.util.List;
import java.util.TreeMap;
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
import org.apache.kafka.connect.data.Field;
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
