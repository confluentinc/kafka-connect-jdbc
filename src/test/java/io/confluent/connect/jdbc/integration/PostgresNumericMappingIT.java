/*
 * Copyright 2018 Confluent Inc.
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

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;

/**
 * Verifies how {@code numeric.mapping} turns a NUMERIC column's driver-reported precision and scale
 * into the emitted Connect schema type. This is the value that decides whether a topic carries an
 * integer, a double, or a Decimal (bytes). A driver change to the reported precision or scale can
 * silently flip that type on a live topic and break Schema Registry compatibility downstream, so
 * the mapping is pinned per mode here rather than only in the mock-driver unit tests.
 *
 * <p>Expected mappings follow {@code GenericDatabaseDialect} (MAX_INTEGER_TYPE_PRECISION = 18;
 * integerSchema picks INT16/INT32/INT64 by precision):
 * <pre>
 *                        NONE        PRECISION_ONLY   BEST_FIT     BEST_FIT_EAGER_DOUBLE
 *   NUMERIC(8,0)         Decimal     INT32            INT32        INT32
 *   NUMERIC(10,2)        Decimal     Decimal          FLOAT64      FLOAT64
 *   NUMERIC(20,5)        Decimal     Decimal          Decimal      FLOAT64
 * </pre>
 * Unconstrained {@code NUMERIC} is deliberately not asserted: its mapping depends on the precision
 * the driver reports for an unconstrained column, which is exactly the driver-dependent behaviour
 * this test is guarding against, so pinning a fixed type for it would be brittle.
 */
@Category(IntegrationTest.class)
public class PostgresNumericMappingIT extends BaseConnectorIT {

  @ClassRule
  public static final PostgreSQLContainer<?> postgres = new PostgreSQLContainer<>("postgres:13")
      .withDatabaseName("testdb")
      .withUsername("test")
      .withPassword("test123");

  private static final String TABLE_NAME = "pg_numeric_types";
  private static final String TOPIC_PREFIX = "numeric-";
  private static final String TOPIC = TOPIC_PREFIX + TABLE_NAME;
  private static final long POLL_INTERVAL_MS = TimeUnit.SECONDS.toMillis(1);
  private static final long CONSUME_TIMEOUT_MS = TimeUnit.SECONDS.toMillis(30);

  private static Connection connection;

  private JsonConverter converter;
  private Map<String, String> props;

  @BeforeClass
  public static void setupClass() throws SQLException {
    connection = DriverManager.getConnection(
        postgres.getJdbcUrl(), postgres.getUsername(), postgres.getPassword());
  }

  @AfterClass
  public static void teardownClass() throws SQLException {
    if (connection != null && !connection.isClosed()) {
      connection.close();
    }
  }

  @Before
  public void setup() throws SQLException {
    converter = new JsonConverter();
    Map<String, String> converterConfig = new HashMap<>();
    converterConfig.put("schemas.enable", "true");
    converter.configure(converterConfig, false);

    startConnect();

    try (Statement stmt = connection.createStatement()) {
      stmt.execute("CREATE TABLE " + TABLE_NAME + " ("
          + "id SERIAL PRIMARY KEY, "
          + "num_int NUMERIC(8,0), "
          + "num_dec NUMERIC(10,2), "
          + "num_big NUMERIC(20,5)"
          + ")");
      stmt.execute("INSERT INTO " + TABLE_NAME + " (num_int, num_dec, num_big) "
          + "VALUES (42, 123.45, 12345678901234.56789)");
    }

    props = new HashMap<>();
    props.put("connector.class", "io.confluent.connect.jdbc.JdbcSourceConnector");
    props.put("tasks.max", "1");
    props.put(JdbcSourceConnectorConfig.CONNECTION_URL_CONFIG, postgres.getJdbcUrl());
    props.put(JdbcSourceConnectorConfig.CONNECTION_USER_CONFIG, postgres.getUsername());
    props.put(JdbcSourceConnectorConfig.CONNECTION_PASSWORD_CONFIG, postgres.getPassword());
    props.put(JdbcSourceConnectorConfig.MODE_CONFIG, JdbcSourceConnectorConfig.MODE_BULK);
    props.put(JdbcSourceConnectorConfig.TABLE_WHITELIST_CONFIG, TABLE_NAME);
    props.put(JdbcSourceConnectorConfig.TOPIC_PREFIX_CONFIG, TOPIC_PREFIX);
    props.put(JdbcSourceConnectorConfig.POLL_INTERVAL_MS_CONFIG, String.valueOf(POLL_INTERVAL_MS));
    props.put(JdbcSourceConnectorConfig.POLL_LINGER_MS_CONFIG, "0");
    props.put("value.converter", "org.apache.kafka.connect.json.JsonConverter");
    props.put("value.converter.schemas.enable", "true");
    props.put("key.converter", "org.apache.kafka.connect.json.JsonConverter");
    props.put("key.converter.schemas.enable", "true");
  }

  @After
  public void tearDown() throws SQLException {
    stopConnect();
    try (Statement stmt = connection.createStatement()) {
      stmt.execute("DROP TABLE IF EXISTS " + TABLE_NAME);
    }
  }

  @Test
  public void noneMapsEveryNumericToDecimal() throws Exception {
    Schema schema = runAndGetValueSchema("none");
    assertEquals(Schema.Type.BYTES, schema.field("num_int").schema().type());
    assertEquals(Schema.Type.BYTES, schema.field("num_dec").schema().type());
    assertEquals(Schema.Type.BYTES, schema.field("num_big").schema().type());
  }

  @Test
  public void precisionOnlyMapsOnlyZeroScaleToInteger() throws Exception {
    Schema schema = runAndGetValueSchema("precision_only");
    assertEquals(Schema.Type.INT32, schema.field("num_int").schema().type());
    assertEquals(Schema.Type.BYTES, schema.field("num_dec").schema().type());
    assertEquals(Schema.Type.BYTES, schema.field("num_big").schema().type());
  }

  @Test
  public void bestFitMapsFittingTypesAndFallsBackToDecimal() throws Exception {
    Schema schema = runAndGetValueSchema("best_fit");
    assertEquals(Schema.Type.INT32, schema.field("num_int").schema().type());
    assertEquals(Schema.Type.FLOAT64, schema.field("num_dec").schema().type());
    // precision 20 exceeds the integer/double fit threshold, so it stays a Decimal.
    assertEquals(Schema.Type.BYTES, schema.field("num_big").schema().type());
  }

  @Test
  public void bestFitEagerDoubleMapsAnyScaledNumericToDouble() throws Exception {
    Schema schema = runAndGetValueSchema("best_fit_eager_double");
    assertEquals(Schema.Type.INT32, schema.field("num_int").schema().type());
    assertEquals(Schema.Type.FLOAT64, schema.field("num_dec").schema().type());
    assertEquals(Schema.Type.FLOAT64, schema.field("num_big").schema().type());
  }

  private Schema runAndGetValueSchema(String numericMapping) throws Exception {
    props.put(JdbcSourceConnectorConfig.NUMERIC_MAPPING_CONFIG, numericMapping);
    connect.kafka().createTopic(TOPIC, 1);

    String connectorName = "numeric-" + numericMapping;
    connect.configureConnector(connectorName, props);
    waitForConnectorToStart(connectorName, 1);

    ConsumerRecords<byte[], byte[]> records = connect.kafka().consume(1, CONSUME_TIMEOUT_MS, TOPIC);
    byte[] value = records.iterator().next().value();
    SchemaAndValue schemaAndValue = converter.toConnectData(TOPIC, value);
    return ((Struct) schemaAndValue.value()).schema();
  }
}
