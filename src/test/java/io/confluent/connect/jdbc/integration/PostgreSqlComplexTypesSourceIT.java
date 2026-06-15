/*
 * Copyright 2026 Confluent Inc.
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

package io.confluent.connect.jdbc.integration;

import io.confluent.common.utils.IntegrationTest;
import io.confluent.connect.jdbc.data.Json;
import io.confluent.connect.jdbc.data.VariableScaleDecimal;
import io.confluent.connect.jdbc.data.ZonedTimestamp;
import io.confluent.connect.jdbc.source.JdbcSourceConnectorConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.json.JsonConverter;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.testcontainers.containers.PostgreSQLContainer;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 * Integration tests for the PostgreSQL complex-type support gated behind
 * {@code sql.complex.types.enable}: native arrays (scalar/numeric/json/temporal element types) and
 * the {@code json.handling.mode} / {@code hstore.handling.mode} representations, end-to-end through
 * the JDBC source connector. Uses a throwaway PostgreSQL container (no external credentials).
 */
@Category(IntegrationTest.class)
public class PostgreSqlComplexTypesSourceIT extends BaseConnectorIT {

  @ClassRule
  public static PostgreSQLContainer<?> postgres = new PostgreSQLContainer<>("postgres:13")
      .withDatabaseName("testdb")
      .withUsername("test")
      .withPassword("test123");

  private static Connection connection;

  private static final String TABLE_NAME = "pg_complex_types";
  private static final String CONNECTOR_NAME = "PostgreSqlComplexTypesSourceConnector";
  private static final String TOPIC_PREFIX = "pg-complex-";
  private static final String TOPIC = TOPIC_PREFIX + TABLE_NAME;

  // 2024-01-15 10:00:00 UTC and 2025-06-10 18:30:00 UTC, as epoch millis (timezone-independent).
  private static final long TS0_UTC_MILLIS = 1705312800000L;
  private static final long TS1_UTC_MILLIS = 1749580200000L;

  private JsonConverter converter;
  private Map<String, String> props;

  @BeforeClass
  public static void setupClass() throws SQLException {
    postgres.start();
    connection = DriverManager.getConnection(
        postgres.getJdbcUrl(), postgres.getUsername(), postgres.getPassword());
  }

  @AfterClass
  public static void teardownClass() throws SQLException {
    if (connection != null && !connection.isClosed()) {
      connection.close();
    }
    postgres.stop();
  }

  @Before
  public void setup() throws Exception {
    converter = new JsonConverter();
    Map<String, String> converterConfig = new HashMap<>();
    converterConfig.put("schemas.enable", "true");
    converter.configure(converterConfig, false);

    startConnect();
    createTableAndInsertData();

    props = new HashMap<>();
    props.put("connector.class", "io.confluent.connect.jdbc.JdbcSourceConnector");
    props.put("tasks.max", "1");
    props.put(JdbcSourceConnectorConfig.CONNECTION_URL_CONFIG, postgres.getJdbcUrl());
    props.put(JdbcSourceConnectorConfig.CONNECTION_USER_CONFIG, postgres.getUsername());
    props.put(JdbcSourceConnectorConfig.CONNECTION_PASSWORD_CONFIG, postgres.getPassword());
    props.put(JdbcSourceConnectorConfig.MODE_CONFIG, "bulk");
    props.put(JdbcSourceConnectorConfig.TOPIC_PREFIX_CONFIG, TOPIC_PREFIX);
    props.put(JdbcSourceConnectorConfig.TABLE_WHITELIST_CONFIG, TABLE_NAME);
    props.put(JdbcSourceConnectorConfig.SQL_COMPLEX_TYPES_ENABLE_CONFIG, "true");
    props.put("value.converter", "org.apache.kafka.connect.json.JsonConverter");
    props.put("value.converter.schemas.enable", "true");
    props.put("key.converter", "org.apache.kafka.connect.json.JsonConverter");
    props.put("key.converter.schemas.enable", "true");
  }

  @After
  public void tearDown() {
    if (connect != null) {
      try {
        connect.deleteConnector(CONNECTOR_NAME);
      } catch (Exception e) {
        // ignore — connector may not have been created
      }
      connect.stop();
    }
    dropTable();
  }

  private void createTableAndInsertData() throws SQLException {
    try (Statement stmt = connection.createStatement()) {
      stmt.execute("CREATE EXTENSION IF NOT EXISTS hstore");
      stmt.execute("DROP TABLE IF EXISTS " + TABLE_NAME);
      stmt.execute("CREATE TABLE " + TABLE_NAME + " (\n"
          + "  id SERIAL PRIMARY KEY,\n"
          + "  text_arr TEXT[],\n"
          + "  int_arr INTEGER[],\n"
          + "  bool_arr BOOLEAN[],\n"
          + "  numeric_arr NUMERIC[],\n"
          + "  jsonb_arr JSONB[],\n"
          + "  ts_arr TIMESTAMP[],\n"
          + "  tstz_arr TIMESTAMPTZ[],\n"
          + "  jsonb_col JSONB,\n"
          + "  hstore_col HSTORE\n"
          + ")");
      stmt.execute("INSERT INTO " + TABLE_NAME + " ("
          + "text_arr, int_arr, bool_arr, numeric_arr, jsonb_arr, ts_arr, tstz_arr, "
          + "jsonb_col, hstore_col"
          + ") VALUES ("
          + "  '{a,b,c}',\n"
          + "  '{1,2,3}',\n"
          + "  '{true,false}',\n"
          + "  '{1.5,3.14159}',\n"
          + "  ARRAY['{\"k\": \"v\"}', '{\"a\": 1}']::jsonb[],\n"
          + "  ARRAY['2024-01-15 10:00:00', '2025-06-10 18:30:00']::timestamp[],\n"
          // second value is +05:30, which PostgreSQL stores as 13:00:00 UTC
          + "  ARRAY['2024-01-15 10:00:00+00', '2025-06-10 18:30:00+05:30']::timestamptz[],\n"
          + "  '{\"key\": \"value\"}',\n"
          + "  'env=>prod, region=>us-west-2'\n"
          + ")");
    }
  }

  private void dropTable() {
    try (Statement stmt = connection.createStatement()) {
      stmt.execute("DROP TABLE IF EXISTS " + TABLE_NAME);
    } catch (SQLException e) {
      // best effort
    }
  }

  /**
   * Default modes: {@code json.handling.mode=string} (logical JSON STRING) and
   * {@code hstore.handling.mode=map}. Verifies every supported array element family round-trips
   * with the correct Connect schema type and values.
   */
  @Test
  public void shouldEmitArraysAndDefaultJsonAndHstoreRepresentations() throws Exception {
    Struct value = runAndConsume(props);

    assertEquals(java.util.Arrays.asList("a", "b", "c"), value.get("text_arr"));
    assertEquals(java.util.Arrays.asList(1, 2, 3), value.get("int_arr"));
    assertEquals(java.util.Arrays.asList(true, false), value.get("bool_arr"));

    // numeric[] -> ARRAY<VariableScaleDecimal>, decoded per element with its own scale.
    List<?> numericArr = (List<?>) value.get("numeric_arr");
    assertEquals(0, new BigDecimal("1.5").compareTo(
        VariableScaleDecimal.toLogical((Struct) numericArr.get(0))));
    assertEquals(0, new BigDecimal("3.14159").compareTo(
        VariableScaleDecimal.toLogical((Struct) numericArr.get(1))));

    // jsonb[] -> ARRAY<Json>, raw JSON text preserved.
    List<?> jsonbArr = (List<?>) value.get("jsonb_arr");
    assertEquals(Json.LOGICAL_NAME,
        value.schema().field("jsonb_arr").schema().valueSchema().name());
    assertTrue(jsonbArr.get(0).toString().contains("\"k\""));

    // timestamp[] -> ARRAY<Timestamp>, decoded in UTC (instants are timezone-independent).
    List<?> tsArr = (List<?>) value.get("ts_arr");
    assertEquals(TS0_UTC_MILLIS, ((Date) tsArr.get(0)).getTime());
    assertEquals(TS1_UTC_MILLIS, ((Date) tsArr.get(1)).getTime());

    // timestamptz[] -> ARRAY<ZonedTimestamp>: tz-aware ISO-8601 strings normalized to UTC.
    assertEquals(ZonedTimestamp.LOGICAL_NAME,
        value.schema().field("tstz_arr").schema().valueSchema().name());
    List<?> tstzArr = (List<?>) value.get("tstz_arr");
    assertEquals("2024-01-15T10:00:00Z", tstzArr.get(0));
    assertEquals("2025-06-10T13:00:00Z", tstzArr.get(1)); // 18:30+05:30 == 13:00Z

    // json.handling.mode=string (default) -> logical JSON STRING.
    assertEquals(Schema.Type.STRING, value.schema().field("jsonb_col").schema().type());
    assertEquals(Json.LOGICAL_NAME, value.schema().field("jsonb_col").schema().name());
    assertTrue(value.get("jsonb_col").toString().contains("\"value\""));

    // hstore.handling.mode=map (default) -> Connect MAP.
    assertEquals(Schema.Type.MAP, value.schema().field("hstore_col").schema().type());
    Map<?, ?> hstore = (Map<?, ?>) value.get("hstore_col");
    assertEquals("prod", hstore.get("env"));
    assertEquals("us-west-2", hstore.get("region"));
  }

  /**
   * Alternate modes: {@code json.handling.mode=map} (shallow MAP) and
   * {@code hstore.handling.mode=json} (JSON-object STRING).
   */
  @Test
  public void shouldHonorMapJsonAndJsonHstoreModes() throws Exception {
    props.put(JdbcSourceConnectorConfig.JSON_HANDLING_MODE_CONFIG,
        JdbcSourceConnectorConfig.JSON_HANDLING_MODE_MAP);
    props.put(JdbcSourceConnectorConfig.HSTORE_HANDLING_MODE_CONFIG,
        JdbcSourceConnectorConfig.HSTORE_HANDLING_MODE_JSON);

    Struct value = runAndConsume(props);

    // json map mode -> shallow Connect MAP with top-level fields.
    assertEquals(Schema.Type.MAP, value.schema().field("jsonb_col").schema().type());
    Map<?, ?> jsonMap = (Map<?, ?>) value.get("jsonb_col");
    assertEquals("value", jsonMap.get("key"));

    // hstore json mode -> plain JSON-object STRING (untagged).
    Schema hstoreSchema = value.schema().field("hstore_col").schema();
    assertEquals(Schema.Type.STRING, hstoreSchema.type());
    String hstoreJson = (String) value.get("hstore_col");
    assertTrue(hstoreJson.contains("\"env\"") && hstoreJson.contains("prod"));
  }

  private Struct runAndConsume(Map<String, String> connectorProps) throws Exception {
    connect.kafka().createTopic(TOPIC, 1);
    connect.configureConnector(CONNECTOR_NAME, connectorProps);
    waitForConnectorToStart(CONNECTOR_NAME, 1);

    ConsumerRecords<byte[], byte[]> records = connect.kafka().consume(1, 30000, TOPIC);
    assertNotNull("No records consumed", records);
    assertTrue("Expected at least one record", records.count() >= 1);

    byte[] payload = records.records(TOPIC).iterator().next().value();
    SchemaAndValue schemaAndValue = converter.toConnectData(TOPIC, payload);
    return (Struct) schemaAndValue.value();
  }
}
