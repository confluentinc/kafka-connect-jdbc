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

import io.confluent.common.utils.IntegrationTest;
import io.confluent.connect.jdbc.integration.BaseConnectorIT;
import io.confluent.connect.jdbc.sink.JdbcSinkConfig;
import io.zonky.test.db.postgres.junit.EmbeddedPostgresRules;
import io.zonky.test.db.postgres.junit.SingleInstancePostgresRule;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.json.JsonConverter;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

/**
 * Integration tests for writing to Postgres views.
 */
@Category(IntegrationTest.class)
public class PostgresQueryIT extends BaseConnectorIT  {

  private static final Logger LOG = LoggerFactory.getLogger(PostgresQueryIT.class);

  @Rule
  public SingleInstancePostgresRule pg = EmbeddedPostgresRules.singleInstance();

  private String tableName;
  private String topic;
  private JsonConverter jsonConverter;
  private Map<String, String> props;

  @Before
  public void before() {
    startConnect();
    jsonConverter = jsonConverter();
    props = baseSinkProps();

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
    try (Connection c = pg.getEmbeddedPostgres().getPostgresDatabase().getConnection()) {
      try (Statement s = c.createStatement()) {
        s.execute("DROP TABLE " + tableName);
      }
      LOG.info("Dropped table");
    } finally {
      pg = null;
      stopConnect();
    }
  }

  @Test
  public void test() throws Exception {
    createTestTable();
    props.put(JdbcSinkConfig.QUERY, "INSERT INTO " + tableName + "(firstname, lastname, age) VALUES " +
            "($.value.firstname, $.value.lastname, COALESCE($.value.age, -1))");
    connect.configureConnector("jdbc-sink-connector", props);
    waitForConnectorToStart("jdbc-sink-connector", 1);

    final Schema schema = SchemaBuilder.struct().name("com.example.Person")
        .field("firstname", Schema.STRING_SCHEMA)
        .field("lastname", Schema.STRING_SCHEMA)
        .build();
    final Struct struct = new Struct(schema)
        .put("firstname", "Christina")
        .put("lastname", "Brams");

    produceRecord(schema, struct);

    Thread.sleep(TimeUnit.SECONDS.toMillis(30));

    try (Connection c = pg.getEmbeddedPostgres().getPostgresDatabase().getConnection()) {
      try (Statement s = c.createStatement()) {
        try (ResultSet rs = s.executeQuery("SELECT * FROM " + tableName)) {
          assertTrue(rs.next());
          assertEquals(struct.getString("firstname"), rs.getString("firstname"));
          assertEquals(struct.getString("lastname"), rs.getString("lastname"));
          assertEquals(-1, rs.getInt("age"));
        }
      }
    }
  }

  private void createTestTable() throws SQLException {
    LOG.info("Creating test table");
    try (Connection c = pg.getEmbeddedPostgres().getPostgresDatabase().getConnection()) {
      c.setAutoCommit(false);
      try (Statement s = c.createStatement()) {
        s.execute("CREATE TABLE " + tableName + "(firstName TEXT, lastName TEXT, age INTEGER)");
        c.commit();
      }
    }
    LOG.info("Created table and view");
  }

  private void produceRecord(Schema schema, Struct struct) {
    String kafkaValue = new String(jsonConverter.fromConnectData(tableName, schema, struct));
    connect.kafka().produce(topic, null, kafkaValue);
  }
}
