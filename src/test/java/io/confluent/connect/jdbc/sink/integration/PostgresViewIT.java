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

import static junit.framework.TestCase.assertTrue;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.fail;

import io.confluent.common.utils.IntegrationTest;
import io.confluent.connect.jdbc.sink.JdbcSinkConfig;
import io.confluent.connect.jdbc.sink.JdbcSinkTask;
import io.zonky.test.db.postgres.junit.EmbeddedPostgresRules;
import io.zonky.test.db.postgres.junit.SingleInstancePostgresRule;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.SinkRecord;
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
public class PostgresViewIT {

  private static Logger log = LoggerFactory.getLogger(PostgresViewIT.class);

  @Rule
  public SingleInstancePostgresRule pg = EmbeddedPostgresRules.singleInstance();

  private Map<String, String> props;
  private String tableName;
  private String topic;
  private JdbcSinkTask task;

  @Before
  public void before() {
    tableName = "test";
    topic = tableName + "_view";
    props = new HashMap<>();
    String jdbcURL = String
        .format("jdbc:postgresql://localhost:%s/postgres", pg.getEmbeddedPostgres().getPort());
    props.put(JdbcSinkConfig.CONNECTION_URL, jdbcURL);
    props.put(JdbcSinkConfig.CONNECTION_USER, "postgres");
    props.put(JdbcSinkConfig.TABLE_TYPES_CONFIG, "VIEW");
    props.put("pk.mode", "none");
    props.put("topics", topic);
  }

  @After
  public void after() throws SQLException {
    stopTask();
    try (Connection c = pg.getEmbeddedPostgres().getPostgresDatabase().getConnection()) {
      try (Statement s = c.createStatement()) {
        s.execute("DROP VIEW " + topic);
        s.execute("DROP TABLE " + tableName);
      }
    }
    log.info("Dropped table");
  }

  @Test
  public void testRecordSchemaMoreFieldsThanViewFails() throws SQLException {
    createTestTableAndView("firstName");
    startTask();
    final Schema schema = SchemaBuilder.struct().name("com.example.Person")
        .field("firstname", Schema.STRING_SCHEMA)
        .field("lastname", Schema.STRING_SCHEMA)
        .field("age", Schema.INT32_SCHEMA)
        .build();
    final Struct struct = new Struct(schema)
        .put("firstname", "Christina")
        .put("lastname", "Brams")
        .put("age", 20);
    try {
      task.put(Collections.singleton(new SinkRecord(topic, 1, null, null, schema, struct, 1)));
      fail();
    } catch (ConnectException e) {
      assertTrue(e.getMessage().contains("View \"" + topic + "\" is missing fields"));
    }
  }

  @Test
  public void testRecordSchemaLessFieldsThanView() throws SQLException {
    createTestTableAndView("firstName, lastName");
    startTask();
    final Schema schema = SchemaBuilder.struct().name("com.example.Person")
        .field("firstname", Schema.STRING_SCHEMA)
        .build();
    final Struct struct = new Struct(schema)
        .put("firstname", "Christina");
    task.put(Collections.singleton(new SinkRecord(topic, 1, null, null, schema, struct, 1)));
    try (Connection c = pg.getEmbeddedPostgres().getPostgresDatabase().getConnection()) {
      try (Statement s = c.createStatement()) {
        try (ResultSet rs = s.executeQuery("SELECT * FROM " + topic)) {
          assertTrue(rs.next());
          assertEquals(struct.getString("firstname"), rs.getString("firstname"));
          assertNull(rs.getString("lastname"));
        }
      }
    }
  }

  @Test
  public void testWriteToView() throws SQLException {
    createTestTableAndView("firstName, lastName");
    startTask();
    final Schema schema = SchemaBuilder.struct().name("com.example.Person")
        .field("firstname", Schema.STRING_SCHEMA)
        .field("lastname", Schema.STRING_SCHEMA)
        .build();
    final Struct struct = new Struct(schema)
        .put("firstname", "Christina")
        .put("lastname", "Brams");
    task.put(Collections.singleton(new SinkRecord(topic, 1, null, null, schema, struct, 1)));
    try (Connection c = pg.getEmbeddedPostgres().getPostgresDatabase().getConnection()) {
      try (Statement s = c.createStatement()) {
        try (ResultSet rs = s.executeQuery("SELECT * FROM " + topic)) {
          assertTrue(rs.next());
          assertEquals(struct.getString("firstname"), rs.getString("firstname"));
          assertEquals(struct.getString("lastname"), rs.getString("lastname"));
        }
      }
    }
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

  private void startTask() {
    task = new JdbcSinkTask();
    task.start(props);
  }

  public void stopTask() {
    if (task != null) {
      task.stop();
    }
  }
}
