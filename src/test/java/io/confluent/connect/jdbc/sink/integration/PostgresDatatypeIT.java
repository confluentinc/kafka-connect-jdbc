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
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import io.confluent.common.utils.IntegrationTest;
import io.confluent.connect.jdbc.sink.JdbcSinkConfig;
import io.confluent.connect.jdbc.sink.JdbcSinkTask;
import io.confluent.connect.jdbc.util.BytesUtil;

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
    props.put("pk.mode", "none");
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
    task.put(Collections.singleton(new SinkRecord(tableName, 1, null, null, schema, struct, 1)));
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
  public void testWriteTableWithPrimitiveArray() throws SQLException {
    createTableWithPrimitiveArray();
    startTask();

    final Schema schema = SchemaBuilder.struct().name("com.example.Arrays")
            .field("int8",    SchemaBuilder.array(Schema.INT8_SCHEMA))
            .field("int16",   SchemaBuilder.array(Schema.INT16_SCHEMA))
            .field("int32",   SchemaBuilder.array(Schema.INT32_SCHEMA))
            .field("int64",   SchemaBuilder.array(Schema.INT64_SCHEMA))
            .field("float32", SchemaBuilder.array(Schema.FLOAT32_SCHEMA))
            .field("float64", SchemaBuilder.array(Schema.FLOAT64_SCHEMA))
            .field("string",  SchemaBuilder.array(Schema.STRING_SCHEMA))
            .field("bool",    SchemaBuilder.array(Schema.BOOLEAN_SCHEMA))
            .build();

    final Struct struct = new Struct(schema)
            .put("int8", Arrays.asList( (byte) 42, (byte) 12))
            .put("int16", Arrays.asList( (short) 42, (short) 12))
            .put("int32", Arrays.asList(42, 16))
            .put("int64", Arrays.asList(42L, 16L))
            .put("float32", Arrays.asList(42.5F, 16.2F))
            .put("float64", Arrays.asList(42.5D, 16.2D))
            .put("string", Arrays.asList("42", "16"))
            .put("bool", Arrays.asList(true, false, true));
    task.put(Collections.singleton(new SinkRecord(tableName, 1, null, null, schema, struct, 1)));

    try (Connection c = pg.getEmbeddedPostgres().getPostgresDatabase().getConnection()) {
      try (Statement s = c.createStatement()) {
        try (ResultSet rs = s.executeQuery("SELECT * FROM " + tableName)) {
          assertTrue(rs.next());

          assertEquals(struct.getArray("int8"), BytesUtil.truncateShortArrayToBytes((Short[]) rs.getArray("int8").getArray()));
          assertEquals(struct.getArray("int16"), Arrays.asList((Short[]) rs.getArray("int16").getArray()));
          assertEquals(struct.getArray("int32"), Arrays.asList((Integer[]) rs.getArray("int32").getArray()));
          assertEquals(struct.getArray("int64"), Arrays.asList((Long[]) rs.getArray("int64").getArray()));
          assertEquals(struct.getArray("float32"), Arrays.asList((Float[]) rs.getArray("float32").getArray()));
          assertEquals(struct.getArray("float64"), Arrays.asList((Double[]) rs.getArray("float64").getArray()));
          assertEquals(struct.getArray("string"), Arrays.asList((String[]) rs.getArray("string").getArray()));
          assertEquals(struct.getArray("bool"), Arrays.asList((Boolean[]) rs.getArray("bool").getArray()));
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
            "CREATE TABLE %s(firstName TEXT, lastName TEXT, jsonid json, userid UUID)",
            tableName
        );
        log.info("Executing statement: {}", sql);
        s.execute(sql);
        c.commit();
      }
    }
    log.info("Created table {} with UUID column", tableName);
  }

  private void createTableWithPrimitiveArray() throws SQLException {
    log.info("Creating table {} with primitives array columns", tableName);
    try (Connection c = pg.getEmbeddedPostgres().getPostgresDatabase().getConnection()) {
      c.setAutoCommit(false);
      try (Statement s = c.createStatement()) {
        String sql = String.format(
                "CREATE TABLE %s(" +
                        "int8    SMALLINT         ARRAY," +
                        "int16   SMALLINT         ARRAY," +
                        "int32   INTEGER          ARRAY," +
                        "int64   BIGINT           ARRAY," +
                        "float32 REAL             ARRAY," +
                        "float64 DOUBLE PRECISION ARRAY," +
                        "string  TEXT             ARRAY," +
                        "bool    BOOLEAN          ARRAY" +
                ")",
                tableName
        );
        log.info("Executing statement: {}", sql);
        s.execute(sql);
        c.commit();
      }
    }
    log.info("Created table {} with primitives array columns", tableName);
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
