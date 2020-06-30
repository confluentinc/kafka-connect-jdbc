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

package io.confluent.connect.jdbc.sink.integration;

import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.json.JsonConverter;
import org.apache.kafka.connect.json.JsonConverterConfig;
import org.apache.kafka.connect.storage.ConverterConfig;
import org.apache.kafka.connect.storage.ConverterType;
import org.apache.kafka.test.IntegrationTest;
import org.apache.kafka.test.TestUtils;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.DockerComposeContainer;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

@Category(IntegrationTest.class)
public class JdbcSinkConnectorIT extends BaseConnectorIT {

  private static final Logger log = LoggerFactory.getLogger(JdbcSinkConnectorIT.class);
  private Map<String, String> props;
  private static Connection connection;

  @ClassRule
  public static DockerComposeContainer mySqlContainer =
      new DockerComposeContainer(new File("src/test/docker/configA/mysql-docker-compose.yml"));

  public static DockerComposeContainer pumbaContainer;
  private void startPumbaContainer() {
    pumbaContainer =
        new DockerComposeContainer(new File("src/test/docker/configB/pumba-docker-compose.yml"));
    pumbaContainer.start();
  }

  @Before
  public void setup() throws Exception {
    startConnect();
    connect.kafka().createTopic(KAFKA_TOPIC, 1);
    props = getCommonProps();
    if (connection == null) {
      TestUtils.waitForCondition(() -> assertConnection().orElse(false),
          TimeUnit.SECONDS.toMillis(50),
          "Failed to start the container.");
    }
  }

  private Optional<Boolean> assertConnection() {
    try {
      connection = getConnection();
      if (connection != null) {
        return Optional.of(true);
      }
      //Delay to avoid frequent connection requests.
      TimeUnit.MILLISECONDS.sleep(100);
      return Optional.empty();
    } catch (SQLException | InterruptedException e) {
      return Optional.empty();
    }
  }

  @After
  public void close() {
    // delete connector
    connect.deleteConnector(CONNECTOR_NAME);
    connect.stop();
  }

  @AfterClass
  public static void closeConnection() {
    mySqlContainer.close();
  }

  @Test
  public void testWithJDBCMysqlDialectSuccess() throws Exception {
    dropTableIfExists(KAFKA_TOPIC);
    sendTestDataToKafka(SCHEMA);
    ConsumerRecords<byte[], byte[]> totalRecords = connect.kafka().consume(
        NUM_RECORDS,
        CONSUME_MAX_DURATION_MS,
        KAFKA_TOPIC);
    log.info("Number of records added in kafka {}", totalRecords.count());

    // Add mysql dialect related configurations.
    getConnectorConfigurations();

    // Configure Connector and wait some specific time to start the connector.
    connect.configureConnector(CONNECTOR_NAME, props);
    waitForConnectorToStart(CONNECTOR_NAME, Integer.valueOf(MAX_TASKS));

    // Wait Connector to write data into Mysql
    waitConnectorToWriteDataIntoMysql(
        connection,
        CONNECTOR_NAME,
        Integer.valueOf(MAX_TASKS),
        KAFKA_TOPIC,
        NUM_RECORDS);

    int count = loadFromSQL(KAFKA_TOPIC);
    Assert.assertEquals(NUM_RECORDS, count);

    sendTestDataToKafka(SCHEMA);
    totalRecords = connect.kafka().consume(
        NUM_RECORDS * 2,
        CONSUME_MAX_DURATION_MS,
        KAFKA_TOPIC);
    log.info("Number of records added in kafka {}", totalRecords.count());
    // Wait Connector to write data into Mysql
    waitConnectorToWriteDataIntoMysql(
        connection,
        CONNECTOR_NAME,
        Integer.valueOf(MAX_TASKS),
        KAFKA_TOPIC,
        NUM_RECORDS * 2);
    count = loadFromSQL(KAFKA_TOPIC);
    Assert.assertEquals(NUM_RECORDS * 2, count);
  }

  @Test
  public void testWithJDBCMysqlDialectUnavailable() throws Exception {
    // Starting 'pumba' container to periodically pause services in sql container.
    startPumbaContainer();
    dropTableIfExists(KAFKA_TOPIC);
    sendTestDataToKafka(SCHEMA);
    ConsumerRecords<byte[], byte[]> totalRecords = connect.kafka().consume(
        NUM_RECORDS,
        CONSUME_MAX_DURATION_MS,
        KAFKA_TOPIC);
    log.info("Number of records added in kafka {}", totalRecords.count());

    // Add mysql dialect related configurations.
    getConnectorConfigurations();

    // Configure Connector and wait some specific time to start the connector.
    connect.configureConnector(CONNECTOR_NAME, props);
    waitForConnectorToStart(CONNECTOR_NAME, Integer.valueOf(MAX_TASKS));

    // Wait Connector to write data into Mysql
    waitConnectorToWriteDataIntoMysql(
        connection,
        CONNECTOR_NAME,
        Integer.valueOf(MAX_TASKS),
        KAFKA_TOPIC,
        NUM_RECORDS);

    int count = loadFromSQL(KAFKA_TOPIC);
    Assert.assertEquals(NUM_RECORDS, count);

    sendTestDataToKafka(SCHEMA);
    totalRecords = connect.kafka().consume(
        NUM_RECORDS * 2,
        CONSUME_MAX_DURATION_MS,
        KAFKA_TOPIC);
    log.info("Number of records added in kafka {}", totalRecords.count());
    // Wait Connector to write data into Mysql
    waitConnectorToWriteDataIntoMysql(
        connection,
        CONNECTOR_NAME,
        Integer.valueOf(MAX_TASKS),
        KAFKA_TOPIC,
        NUM_RECORDS * 2);
    count = loadFromSQL(KAFKA_TOPIC);
    Assert.assertEquals(NUM_RECORDS * 2, count);
    pumbaContainer.close();
  }

  private void dropTableIfExists(String kafkaTopic) throws SQLException {
    Statement st = connection.createStatement();
    st.executeQuery("drop table if exists " + kafkaTopic);
  }

  private int loadFromSQL(String mySqlTable) throws SQLException {
    Statement st = connection.createStatement();
    ResultSet rs = st.executeQuery("SELECT COUNT(*) AS rowcount FROM " + mySqlTable);
    rs.next();
    return rs.getInt("rowcount");
  }

  private void sendTestDataToKafka(Schema SCHEMA) throws InterruptedException {
    for (int i = 0; i < NUM_RECORDS; i++) {
      String value = asJson(KAFKA_TOPIC, SCHEMA, i);
      connect.kafka().produce(KAFKA_TOPIC, null, value);
      //A minor delay is added so that record produced will have different time stamp.
      Thread.sleep(10);
    }
  }

  private void getConnectorConfigurations() {
    props.put("connection.url", "jdbc:mysql://localhost:3306/db");
    props.put("connection.user", "user");
    props.put("connection.password", "password");
    props.put("dialect.name", "MySqlDatabaseDialect");
    props.put("auto.create", "true");
    props.put("value.converter", JsonConverter.class.getName());
  }

  private String asJson(String topic, Schema schema, int i) {
      final Struct struct = new Struct(schema)
          .put("firstName", "Alex")
          .put("lastName", "Smith")
          .put("bool", true)
          .put("short", (short) 1234)
          .put("byte", (byte) -32)
          .put("long", 12425436L)
          .put("float", (float) 2356.3)
          .put("double", -2436546.56457)
          .put("age", i)
          .put("modified", new Date(1474661402123L));
      JsonConverter jsonConverter = new JsonConverter();
      Map<String, String> config = new HashMap<>();
      config.put(JsonConverterConfig.SCHEMAS_CACHE_SIZE_CONFIG, "100");
      config.put(ConverterConfig.TYPE_CONFIG, ConverterType.VALUE.getName());
      config.put(JsonConverterConfig.SCHEMAS_ENABLE_CONFIG, "true");
      jsonConverter.configure(config);
      byte[] raw = jsonConverter.fromConnectData(topic, schema, struct);
      return new String(raw, StandardCharsets.UTF_8);
    }
}
