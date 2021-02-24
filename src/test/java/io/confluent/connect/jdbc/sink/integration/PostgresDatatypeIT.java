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

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.Arrays;

import io.confluent.common.utils.IntegrationTest;
import io.confluent.connect.jdbc.sink.JdbcSinkConfig;
import io.confluent.connect.jdbc.sink.JdbcSinkTask;

import io.zonky.test.db.postgres.junit.EmbeddedPostgresRules;
import io.zonky.test.db.postgres.junit.SingleInstancePostgresRule;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static junit.framework.TestCase.assertTrue;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

/**
 * Integration tests for writing to Postgres with UUID columns.
 */
@Category(IntegrationTest.class)
public class PostgresDatatypeIT {

  private static Logger log = LoggerFactory.getLogger(PostgresDatatypeIT.class);

  @Rule
  public SingleInstancePostgresRule pg = EmbeddedPostgresRules.singleInstance();

  private Map<String, String> props;
  private String tableName;
  private JdbcSinkTask task;

  @Before
  public void before() {
    tableName = "test";
    props = new HashMap<>();
    String jdbcURL = String
        .format("jdbc:postgresql://localhost:%s/postgres", pg.getEmbeddedPostgres().getPort());
    props.put(JdbcSinkConfig.CONNECTION_URL, jdbcURL);
    props.put(JdbcSinkConfig.CONNECTION_USER, "postgres");
    props.put("pk.mode", "record_key");
    props.put("pk.fields", "userid");
    props.put("delete.enabled", "true");
    props.put("errors.log.enable", "true");
    props.put("errors.log.include.messages", "true");
    props.put("topics", tableName);
  }

  @After
  public void after() throws SQLException {
    stopTask();
    try (Connection c = pg.getEmbeddedPostgres().getPostgresDatabase().getConnection()) {
      try (Statement s = c.createStatement()) {
        s.execute("DROP TABLE IF EXISTS " + tableName);
      }
    }
    log.info("Dropped table");
  }

  @Test
  public void testWriteToTableWithUuidColumn() throws SQLException {
    createTableWithUuidColumns();
    startTask();
    final Schema keySchema = SchemaBuilder.STRING_SCHEMA;
    final Schema schema = SchemaBuilder.struct().name("com.example.Person")
                                       .field("firstname", Schema.STRING_SCHEMA)
                                       .field("lastname", Schema.STRING_SCHEMA)
                                       .field("jsonid", Schema.STRING_SCHEMA)
                                       .field("userid", Schema.STRING_SCHEMA)
                                       .build();
    UUID uuid = UUID.randomUUID();
    String jsonid = "5";
    final Struct struct = new Struct(schema)
        .put("firstname", "Christina")
        .put("lastname", "Brams")
        .put("jsonid", jsonid)
        .put("userid", uuid.toString());
    task.put(Collections.singleton(new SinkRecord(tableName, 1, keySchema, uuid.toString(), schema, struct, 1)));
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
  public void testDeleteFromTableWithUuidColumn() throws SQLException {
    createTableWithUuidColumns();
    startTask();
    final Schema keySchema = SchemaBuilder.STRING_SCHEMA;
    final Schema schema = SchemaBuilder.struct().name("com.example.Person")
            .field("firstname", Schema.STRING_SCHEMA)
            .field("lastname", Schema.STRING_SCHEMA)
            .field("jsonid", Schema.STRING_SCHEMA)
            .field("userid", Schema.STRING_SCHEMA)
            .build();
    UUID uuid = UUID.randomUUID();
    String jsonid = "5";
    final Struct insertStruct = new Struct(schema)
            .put("firstname", "Steve")
            .put("lastname", "Stevenson")
            .put("jsonid", jsonid)
            .put("userid", uuid.toString());

    task.put(Arrays.asList(
            new SinkRecord(tableName, 1, keySchema, uuid.toString(), schema, insertStruct, 1),
            new SinkRecord(tableName, 1, keySchema, uuid.toString(), schema, null, 2)
    ));
    try (Connection c = pg.getEmbeddedPostgres().getPostgresDatabase().getConnection()) {
      try (Statement s = c.createStatement()) {
        try (ResultSet rs = s.executeQuery("SELECT * FROM " + tableName)) {
          assertFalse(rs.next());
        }
      }
    }
  }


  private void createTableWithUuidColumns() throws SQLException {
    log.info("Creating table {} with UUID column", tableName);
    try (Connection c = pg.getEmbeddedPostgres().getPostgresDatabase().getConnection()) {
      c.setAutoCommit(false);
      try (Statement s = c.createStatement()) {
        String sql = String.format(
            "CREATE TABLE %s(userid UUID PRIMARY KEY, firstName TEXT, lastName TEXT, jsonid json)",
            tableName
        );
        log.info("Executing statement: {}", sql);
        s.execute(sql);
        c.commit();
      }
    }
    log.info("Created table {} with UUID column", tableName);
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
