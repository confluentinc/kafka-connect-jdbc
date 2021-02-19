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

import static org.apache.kafka.connect.runtime.ConnectorConfig.ERRORS_TOLERANCE_CONFIG;
import static org.apache.kafka.connect.runtime.SinkConnectorConfig.DLQ_TOPIC_NAME_CONFIG;
import static org.apache.kafka.connect.runtime.SinkConnectorConfig.DLQ_TOPIC_REPLICATION_FACTOR_CONFIG;
import static org.apache.kafka.test.TestUtils.waitForCondition;
import static org.junit.Assert.assertEquals;

import io.confluent.common.utils.IntegrationTest;
import io.confluent.connect.jdbc.integration.BaseConnectorIT;
import io.confluent.connect.jdbc.sink.JdbcSinkConfig;
import io.zonky.test.db.postgres.junit.EmbeddedPostgresRules;
import io.zonky.test.db.postgres.junit.SingleInstancePostgresRule;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Optional;

import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.runtime.errors.ToleranceType;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Integration tests for writing to Postgres views.
 */
@Category(IntegrationTest.class)
public class PostgresViewIT extends BaseConnectorIT  {

  private static Logger log = LoggerFactory.getLogger(PostgresViewIT.class);

  @Rule
  public SingleInstancePostgresRule pg = EmbeddedPostgresRules.singleInstance();

  private String tableName;
  private String topic;

  @Before
  public void before() {
    startConnect();
    setUpForSinkIt();

    tableName = "test";
    topic = tableName + "_view";
    String jdbcURL = String
        .format("jdbc:postgresql://localhost:%s/postgres", pg.getEmbeddedPostgres().getPort());
    props.put(JdbcSinkConfig.CONNECTION_URL, jdbcURL);
    props.put(JdbcSinkConfig.CONNECTION_USER, "postgres");
    props.put(JdbcSinkConfig.TABLE_TYPES_CONFIG, "VIEW");
    props.put("pk.mode", "none");
    props.put("topics", topic);

    // create topic in Kafka
    connect.kafka().createTopic(topic, 1);
  }

  @After
  public void after() throws SQLException {
   stopConnect();
    try (Connection c = pg.getEmbeddedPostgres().getPostgresDatabase().getConnection()) {
      try (Statement s = c.createStatement()) {
        s.execute("DROP VIEW " + topic);
        s.execute("DROP TABLE " + tableName);
      }
    }
    log.info("Dropped table");
  }

  /**
   * Verifies that when sending records with more fields than the view has, these errant records
   * are sent to the error reporter. The test also intersperses correct schema records to verify
   * that only the errant records are being sent to the error reporter.
   *
   * @throws Exception
   */
  @Test
  public void testRecordSchemaMoreFieldsThanViewSendsToErrorReporter() throws Exception {
    props.put(ERRORS_TOLERANCE_CONFIG, ToleranceType.ALL.value());
    props.put(DLQ_TOPIC_NAME_CONFIG, DLQ_TOPIC_NAME);
    props.put(DLQ_TOPIC_REPLICATION_FACTOR_CONFIG, "1");

    createTestTableAndView("firstName, lastName");
    connect.configureConnector("jdbc-sink-connector", props);
    waitForConnectorToStart("jdbc-sink-connector", 1);

    final Schema correctSchema = SchemaBuilder.struct().name("com.example.Person")
        .field("firstname", Schema.STRING_SCHEMA)
        .field("lastname", Schema.STRING_SCHEMA)
        .build();
    final Struct correctStruct = new Struct(correctSchema)
        .put("firstname", "Christina")
        .put("lastname", "Brams");

    final Schema errorSchema = SchemaBuilder.struct().name("com.example.Person")
        .field("firstname", Schema.STRING_SCHEMA)
        .field("lastname", Schema.STRING_SCHEMA)
        .field("age", Schema.INT32_SCHEMA)
        .build();
    final Struct errorStruct = new Struct(errorSchema)
        .put("firstname", "Christina")
        .put("lastname", "Brams")
        .put("age", 20);

    String kafkaValue;

    for (int i = 0; i < 6; i++) {
      if (i % 2 == 0) {
        kafkaValue = new String(jsonConverter.fromConnectData(topic, correctSchema, correctStruct));
        connect.kafka().produce(topic, null, kafkaValue);
      } else {
        kafkaValue = new String(jsonConverter.fromConnectData(topic, errorSchema, errorStruct));
        connect.kafka().produce(topic, null, kafkaValue);
      }
    }

    try {
      // Consume the number of records produced since this is the upper bound of records that
      // could be sent to the error reporter.
      ConsumerRecords<byte[], byte[]> records = connect.kafka().consume(6,
          CONSUME_MAX_DURATION_MS, DLQ_TOPIC_NAME);
    } catch (RuntimeException e) {
      // Check that the amount of records that were actually found by the consumer matches the
      // number of records expected to be sent to the error reporter.
      assertEquals("3", e.toString().substring(65, 66));
    }
  }

  @Test
  public void testRecordSchemaLessFieldsThanView() throws Exception {
    createTestTableAndView("firstName, lastName");
    connect.configureConnector("jdbc-sink-connector", props);
    waitForConnectorToStart("jdbc-sink-connector", 1);

    final Schema schema = SchemaBuilder.struct().name("com.example.Person")
        .field("firstname", Schema.STRING_SCHEMA)
        .build();
    final Struct struct = new Struct(schema)
        .put("firstname", "Christina");

    String kafkaValue = new String(jsonConverter.fromConnectData(topic, schema, struct));
    connect.kafka().produce(topic, null, kafkaValue);

    waitForCondition(
        () -> {
          try (Connection c = pg.getEmbeddedPostgres().getPostgresDatabase().getConnection()) {
            try (Statement s = c.createStatement()) {
              try (ResultSet rs = s.executeQuery("SELECT * FROM " + topic)) {
                boolean result = rs.next()
                    && struct.getString("firstname").equals(rs.getString("firstname"))
                    && rs.getString("lastname") == null;
                return Optional.of(result).orElse(false);
              }
            }
          }
        },
        VERIFY_MAX_DURATION_MS,
        "The database content did not match the record's content."
    );
  }

  @Test
  public void testWriteToView() throws Exception {
    createTestTableAndView("firstName, lastName");
    connect.configureConnector("jdbc-sink-connector", props);
    waitForConnectorToStart("jdbc-sink-connector", 1);

    final Schema schema = SchemaBuilder.struct().name("com.example.Person")
        .field("firstname", Schema.STRING_SCHEMA)
        .field("lastname", Schema.STRING_SCHEMA)
        .build();
    final Struct struct = new Struct(schema)
        .put("firstname", "Christina")
        .put("lastname", "Brams");

    String kafkaValue = new String(jsonConverter.fromConnectData(topic, schema, struct));
    connect.kafka().produce(topic, null, kafkaValue);

    waitForCondition(
        () -> {
          try (Connection c = pg.getEmbeddedPostgres().getPostgresDatabase().getConnection()) {
            try (Statement s = c.createStatement()) {
              try (ResultSet rs = s.executeQuery("SELECT * FROM " + topic)) {
                boolean result = rs.next()
                    && struct.getString("firstname").equals(rs.getString("firstname"))
                    && struct.getString("lastname").equals(rs.getString("lastname"));
                return Optional.of(result).orElse(false);
              }
            }
          }
        },
        VERIFY_MAX_DURATION_MS,
        "The database content did not match the record's content."
    );
  }

  private void createTestTableAndView(String viewFields) throws SQLException {
    log.info("Creating test table and view");
    try (Connection c = pg.getEmbeddedPostgres().getPostgresDatabase().getConnection()) {
      c.setAutoCommit(false);
      try (Statement s = c.createStatement()) {
        s.execute("CREATE TABLE " + tableName + "(firstName TEXT, lastName TEXT, age INTEGER)");
        s.execute("CREATE VIEW " + topic + " AS SELECT " + viewFields + " FROM " + tableName);
        c.commit();
      }
    }
    log.info("Created table and view");
  }
}
