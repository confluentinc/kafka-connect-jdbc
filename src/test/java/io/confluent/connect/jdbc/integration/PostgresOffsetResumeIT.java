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
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.testcontainers.containers.PostgreSQLContainer;

import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Offset continuity across a connector restart - the exact symptom class behind INC-11312, where a
 * change in the source-partition offset key made a restarted task re-read from the beginning and
 * re-produce every row.
 *
 * <p>The connector streams three rows in incrementing mode, is deleted (its committed source
 * offset persists in the worker's offsets topic), gets two more rows inserted, and is recreated
 * under the same name. If offsets resume correctly the topic ends with five distinct records; if
 * the offset is lost the pre-restart rows are re-read and duplicated. The assertion reads the first
 * records off the topic and fails on any duplicate, which is what over-production looks like. This
 * is the E2E complement to the {@code *RestoreOffset*} unit tests in {@code JdbcSourceTaskUpdateTest}.
 */
@Category(IntegrationTest.class)
public class PostgresOffsetResumeIT extends BaseConnectorIT {

  @ClassRule
  public static final PostgreSQLContainer<?> postgres = new PostgreSQLContainer<>("postgres:13")
      .withDatabaseName("testdb")
      .withUsername("test")
      .withPassword("test123");

  private static final String CONNECTOR_NAME = "offset-resume-source";
  private static final String TABLE_NAME = "resume_tbl";
  private static final String TOPIC_PREFIX = "resume-";
  private static final String TOPIC = TOPIC_PREFIX + TABLE_NAME;
  private static final long POLL_INTERVAL_MS = TimeUnit.SECONDS.toMillis(1);
  private static final long CONSUME_TIMEOUT_MS = TimeUnit.SECONDS.toMillis(60);

  private static Connection connection;

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
    startConnect();

    try (Statement stmt = connection.createStatement()) {
      stmt.execute("CREATE TABLE " + TABLE_NAME + " ("
          + "id SERIAL PRIMARY KEY, name VARCHAR(100))");
    }

    props = new HashMap<>();
    props.put("connector.class", "io.confluent.connect.jdbc.JdbcSourceConnector");
    props.put("tasks.max", "1");
    props.put(JdbcSourceConnectorConfig.CONNECTION_URL_CONFIG, postgres.getJdbcUrl());
    props.put(JdbcSourceConnectorConfig.CONNECTION_USER_CONFIG, postgres.getUsername());
    props.put(JdbcSourceConnectorConfig.CONNECTION_PASSWORD_CONFIG, postgres.getPassword());
    props.put(JdbcSourceConnectorConfig.MODE_CONFIG, JdbcSourceConnectorConfig.MODE_INCREMENTING);
    props.put(JdbcSourceConnectorConfig.INCREMENTING_COLUMN_MAPPING_CONFIG, ".*" + TABLE_NAME + ":id");
    props.put(JdbcSourceConnectorConfig.TABLE_INCLUDE_LIST_CONFIG, ".*" + TABLE_NAME);
    props.put(JdbcSourceConnectorConfig.TOPIC_PREFIX_CONFIG, TOPIC_PREFIX);
    props.put(JdbcSourceConnectorConfig.VALIDATE_NON_NULL_CONFIG, "false");
    props.put(JdbcSourceConnectorConfig.POLL_INTERVAL_MS_CONFIG, String.valueOf(POLL_INTERVAL_MS));
    props.put(JdbcSourceConnectorConfig.POLL_LINGER_MS_CONFIG, "0");
    props.put("value.converter", "org.apache.kafka.connect.json.JsonConverter");
    props.put("value.converter.schemas.enable", "false");
    props.put("key.converter", "org.apache.kafka.connect.storage.StringConverter");
  }

  @After
  public void tearDown() throws SQLException {
    stopConnect();
    try (Statement stmt = connection.createStatement()) {
      stmt.execute("DROP TABLE IF EXISTS " + TABLE_NAME);
    }
  }

  @Test
  public void offsetsResumeAfterRestartWithoutRereadingRows() throws Exception {
    connect.kafka().createTopic(TOPIC, 1);

    insertRow("pre_1");
    insertRow("pre_2");
    insertRow("pre_3");

    connect.configureConnector(CONNECTOR_NAME, props);
    waitForConnectorToStart(CONNECTOR_NAME, 1);

    // The three pre-restart rows stream exactly once.
    ConsumerRecords<byte[], byte[]> before = connect.kafka().consume(3, CONSUME_TIMEOUT_MS, TOPIC);
    assertEquals("Pre-restart rows should stream once", 3, before.count());

    // Let the source offset flush before the restart (startConnect uses offset.flush.interval.ms=1000).
    Thread.sleep(TimeUnit.SECONDS.toMillis(3));

    // Restart: delete and recreate under the same name so the committed offset is reused.
    connect.deleteConnector(CONNECTOR_NAME);

    insertRow("post_1");
    insertRow("post_2");

    connect.configureConnector(CONNECTOR_NAME, props);
    waitForConnectorToStart(CONNECTOR_NAME, 1);

    // If offsets resumed, the first five records are the five distinct rows; a lost offset re-reads
    // the pre rows, so a duplicate shows up within the first five (single-partition topic keeps order).
    ConsumerRecords<byte[], byte[]> after = connect.kafka().consume(5, CONSUME_TIMEOUT_MS, TOPIC);
    List<String> values = new ArrayList<>();
    for (ConsumerRecord<byte[], byte[]> record : after.records(TOPIC)) {
      values.add(new String(record.value(), StandardCharsets.UTF_8));
    }
    Set<String> distinct = new HashSet<>(values);
    assertEquals("A restarted connector must not re-read committed rows (offset should resume); "
        + "got " + values, values.size(), distinct.size());
    assertTrue("Post-restart rows should stream after resume; got " + values,
        values.stream().anyMatch(v -> v.contains("post_2")));
  }

  private void insertRow(String name) throws SQLException {
    try (Statement stmt = connection.createStatement()) {
      stmt.execute("INSERT INTO " + TABLE_NAME + " (name) VALUES ('" + name + "')");
    }
  }
}
