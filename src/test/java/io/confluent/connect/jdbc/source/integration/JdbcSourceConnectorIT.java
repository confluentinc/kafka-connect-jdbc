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

package io.confluent.connect.jdbc.source.integration;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.confluent.connect.jdbc.JdbcSourceConnector;
import io.confluent.connect.jdbc.BaseConnectorIT;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.connect.json.JsonConverter;
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
import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

@Category(IntegrationTest.class)
public class JdbcSourceConnectorIT extends BaseConnectorIT {
  private static final Logger log = LoggerFactory.getLogger(JdbcSourceConnectorIT.class);

  private static final ObjectMapper objectMapper = new ObjectMapper();
  private Map<String, String> props;
  private static final int NUM_RECORDS = 50000;

  @ClassRule
  public static DockerComposeContainer mySqlContainer =
      new DockerComposeContainer(new File("src/test/docker/configA/mysql-docker-compose.yml"));

  @Before
  public void setup() throws Exception {
    startConnect();
    connect.kafka().createTopic(KAFKA_TOPIC, 1);
    props = getSourceConnectorProps();
    if (connection == null) {
      TestUtils.waitForCondition(() -> assertDbConnection().orElse(false),
          TimeUnit.SECONDS.toMillis(30),
          "Failed to start the container.");
    }
    dropTableIfExists(KAFKA_TOPIC);
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
  public void testSuccess() throws Exception {
    String topicName = props.get("topic.prefix") + KAFKA_TOPIC;
    connect.kafka().createTopic(topicName);
    sendTestDataToMysql(0, NUM_RECORDS);

    // Configure Connector and wait some specific time to start the connector.
    connect.configureConnector(CONNECTOR_NAME, props);
    waitForConnectorToStart(CONNECTOR_NAME, Integer.valueOf(MAX_TASKS));

    log.info("Waiting for records in destination topic ...");
    ConsumerRecords<byte[], byte[]> records = connect.kafka().consume(
        NUM_RECORDS,
        CONSUME_MAX_DURATION_MS,
        topicName);
    assertRecordsCountAndContent(NUM_RECORDS, records);
  }

  @Test
  public void testForDbServerUnavailability() throws Exception {
    // Starting 'pumba' container to periodically pause services in sql container.
    // Will pause the sql container for 10s in a period of 30s.
    startPumbaPauseContainer();
    String topicName = props.get("topic.prefix") + KAFKA_TOPIC;
    connect.kafka().createTopic(topicName);
    sendTestDataToMysql(0, NUM_RECORDS);

    // Configure Connector and wait some specific time to start the connector.
    connect.configureConnector(CONNECTOR_NAME, props);
    log.info("Waiting for records in destination topic ...");
    waitForConnectorToStart(CONNECTOR_NAME, Integer.valueOf(MAX_TASKS));
    ConsumerRecords<byte[], byte[]> records = connect.kafka().consume(
        NUM_RECORDS,
        CONSUME_MAX_DURATION_MS,
        topicName);
    assertRecordsCountAndContent(NUM_RECORDS, records);
    pumbaPauseContainer.close();
  }

  @Test
  public void testForDbServerDelay() throws Exception {
    // Starting 'pumba' container to periodically delay services in sql container.
    // Will delay the sql container's services for 1s in a period of 5s.
    startPumbaDelayContainer();
    String topicName = props.get("topic.prefix") + KAFKA_TOPIC;
    connect.kafka().createTopic(topicName);
    sendTestDataToMysql(0, NUM_RECORDS);

    // Configure Connector and wait some specific time to start the connector.
    connect.configureConnector(CONNECTOR_NAME, props);
    log.info("Waiting for records in destination topic ...");
    waitForConnectorToStart(CONNECTOR_NAME, Integer.valueOf(MAX_TASKS));
    ConsumerRecords<byte[], byte[]> records = connect.kafka().consume(
        NUM_RECORDS,
        CONSUME_MAX_DURATION_MS,
        topicName);
    assertRecordsCountAndContent(NUM_RECORDS, records);
    pumbaDelayContainer.close();
  }

  /**
   * Create a map of Common connector properties.
   *
   * @return : Map of props.
   */
  private Map<String, String> getSourceConnectorProps() {
    Map<String, String> props = new HashMap<>();
    props.put("connector.class", JdbcSourceConnector.class.getName());
    props.put("tasks.max", MAX_TASKS);
    // license properties
    props.put("confluent.topic.replication.factor", "1");
    props.put("confluent.topic.bootstrap.servers", connect.kafka().bootstrapServers());
    // connector-specific properties
    props.put("connection.password", "password");
    props.put("connection.user", "user");
    props.put("connection.url", "jdbc:mysql://localhost:3306/db");
    props.put("dialect.name", "MySqlDatabaseDialect");
    props.put("value.converter", JsonConverter.class.getName());
    props.put("mode", "incrementing");
    props.put("incrementing.column.name", "userId");
    props.put("topic.prefix", "sql-");
    return props;
  }

  private void sendTestDataToMysql(int startIndex, int numRecords) throws SQLException {
    Statement st = connection.createStatement();
    String sql;
    if (!tableExist(connection, KAFKA_TOPIC)) {
      sql = "CREATE TABLE " + KAFKA_TOPIC +
          "(userId INTEGER not NULL, " +
          " firstName VARCHAR(255), " +
          " lastName VARCHAR(255), " +
          " age INTEGER, " +
          " PRIMARY KEY ( userId ))";
      st.executeQuery(sql);
    }
    sql = "INSERT INTO mysqlTable(userId,firstName,lastName,age) "
        + "VALUES(?,?,?,?)";
    PreparedStatement pstmt = connection.prepareStatement(sql);
    for (int i = startIndex; i < startIndex + numRecords; i++) {
      pstmt.setLong(1, i);
      pstmt.setString(2, "FirstName");
      pstmt.setString(3, "LastName");
      pstmt.setLong(4, 20);
      pstmt.executeUpdate();
    }
    pstmt.close();
  }

  private void assertRecordsCountAndContent(int numRecords, ConsumerRecords<byte[], byte[]> totalRecords) throws IOException {
    int id = 0;
    Assert.assertEquals(numRecords, totalRecords.count());
    for (ConsumerRecord<byte[], byte[]> record : totalRecords) {
      String value = new String(record.value());
      JsonNode jsonNode = objectMapper.readTree(value);
      jsonNode = jsonNode.get("payload");
      Assert.assertEquals(id++, jsonNode.get("userId").asInt());
      Assert.assertEquals("FirstName", jsonNode.get("firstName").asText());
      Assert.assertEquals("LastName", jsonNode.get("lastName").asText());
      Assert.assertEquals(20, jsonNode.get("age").asInt());
    }
  }
}