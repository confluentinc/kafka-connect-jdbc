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

import org.apache.http.HttpStatus;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.test.IntegrationTest;
import org.apache.kafka.test.TestUtils;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.DockerComposeContainer;

import java.io.File;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

@Category(IntegrationTest.class)
public class JdbcSourceConnectorIT extends BaseConnectorIT {
  private static final Logger log = LoggerFactory.getLogger(JdbcSourceConnectorIT.class);

  private Map<String, String> props;
  private static Connection connection;

  @ClassRule
  public static DockerComposeContainer compose =
          new DockerComposeContainer(new File("src/test/docker/docker-compose.yml"));

  @Before
  public void setup() throws Exception {
    startConnect();
    connect.kafka().createTopic(KAFKA_TOPIC, 1);
    props = getCommonProps();
    if (connection == null) {
      TestUtils.waitForCondition(() -> assertConnection().orElse(false),
              TimeUnit.SECONDS.toMillis(30),
              "Failed to start the container.");
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
    compose.close();
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

  @Test
  public void testTaskBounce() throws Exception {
    int startIndex = 0;

    dropTableIfExists(KAFKA_TOPIC, connection);
    createTable(connection);
    sendTestDataToMysql(startIndex, connection);

    connect.kafka().createTopic("sql-" + KAFKA_TOPIC);

    // Add mysql dialect related configurations.
    addConnectionProperties(props);

    props.put("mode", "incrementing");
    props.put("incrementing.column.name",  "id");

    // start a source connector
    connect.configureConnector(CONNECTOR_NAME, props);

    // wait for tasks to spin up
    int expectedNumTasks = Integer.valueOf(MAX_TASKS); // or set to actual number
    waitForConnectorToStart(CONNECTOR_NAME, expectedNumTasks);

    log.info("Waiting for records in destination topic ...");
    ConsumerRecords<byte[], byte[]> records = connect.kafka().consume(
        NUM_RECORDS,
        CONSUME_MAX_DURATION_MS,
        "sql-mysqlTable");

    assertRecordOffset(startIndex, startIndex+NUM_RECORDS, records);

    // restart Connector Task
    if(restartConnectorTask() != HttpStatus.SC_NO_CONTENT){
      throw new Exception("Unable to restart the task");
    };

    connect.kafka().consume(
        NUM_RECORDS,
        CONSUME_MAX_DURATION_MS,
        "sql-mysqlTable");

    sendTestDataToMysql(startIndex+NUM_RECORDS, connection);

    waitForConnectorToStart(CONNECTOR_NAME, expectedNumTasks);

    log.info("Waiting for records in destination topic ...");
    records = connect.kafka().consume(
        NUM_RECORDS*2,
        CONSUME_MAX_DURATION_MS,
        "sql-mysqlTable");

    assertRecordOffset(startIndex, (startIndex+NUM_RECORDS*2), records);

  }

  @Test
  public void testResumingOffsetsAfterConnectorRestart() throws Exception {
    int startIndex = 0;

    dropTableIfExists(KAFKA_TOPIC, connection);
    createTable(connection);
    sendTestDataToMysql(startIndex, connection);

    connect.kafka().createTopic("sql-" + KAFKA_TOPIC);

    // Add mysql dialect related configurations.
    addConnectionProperties(props);

    props.put("mode", "incrementing");
    props.put("incrementing.column.name",  "id");

    // start a source connector
    connect.configureConnector(CONNECTOR_NAME, props);

    // wait for tasks to spin up
    int expectedNumTasks = Integer.valueOf(MAX_TASKS); // or set to actual number
    waitForConnectorToStart(CONNECTOR_NAME, expectedNumTasks);

    log.info("Waiting for records in destination topic ...");
    ConsumerRecords<byte[], byte[]> records = connect.kafka().consume(
        NUM_RECORDS,
        CONSUME_MAX_DURATION_MS,
        "sql-mysqlTable");

    assertRecordOffset(startIndex, startIndex+NUM_RECORDS, records);

    // delete the connector
    connect.deleteConnector(CONNECTOR_NAME);

    sendTestDataToMysql(startIndex+NUM_RECORDS, connection);

    // start a source connector
    connect.configureConnector(CONNECTOR_NAME, props);

    // wait for tasks to spin up
    int expected = Integer.valueOf(MAX_TASKS); // or set to actual number
    waitForConnectorToStart(CONNECTOR_NAME, expected);

    records = connect.kafka().consume(
    NUM_RECORDS*2,
    CONSUME_MAX_DURATION_MS,
    "sql-mysqlTable");

    assertRecordOffset(startIndex, (startIndex+NUM_RECORDS*2), records);

  }

  @Test
  public void testConnectorModeChange() throws InterruptedException, SQLException {
    int startIndex = 0;
    dropTableIfExists(KAFKA_TOPIC, connection);
    createTimestampTable(connection);
    sendTestTimestampDataToMysql(startIndex, connection);

    connect.kafka().createTopic("sql-" + KAFKA_TOPIC);

    // Add mysql dialect related configurations.
    addConnectionProperties(props);
    props.put("mode", "timestamp");
    props.put("timestamp.column.name",  "time1");

    // start a source connector
    connect.configureConnector(CONNECTOR_NAME, props);

    // wait for tasks to spin up
    int expectedNumTasks = Integer.valueOf(MAX_TASKS); // or set to actual number
    waitForConnectorToStart(CONNECTOR_NAME, expectedNumTasks);

    log.info("Waiting for records in destination topic ...");
    ConsumerRecords<byte[], byte[]> records = connect.kafka().consume(
        NUM_RECORDS,
        5000,
        "sql-mysqlTable");

    assertRecordOffset(startIndex, NUM_RECORDS, records);

    // delete the connector
    connect.deleteConnector(CONNECTOR_NAME);

    // change mode
    props.replace("mode", "incrementing");
    props.put("incrementing.column.name",  "id");
    props.remove("timestamp.column.name");

    sendTestTimestampDataToMysql(startIndex+NUM_RECORDS, connection);

    // start a source connector
    connect.configureConnector(CONNECTOR_NAME, props);

    // wait for tasks to spin up
    int expected = Integer.valueOf(MAX_TASKS); // or set to actual number
    waitForConnectorToStart(CONNECTOR_NAME, expected);

    records = connect.kafka().consume(
        NUM_RECORDS * 3,
        CONSUME_MAX_DURATION_MS,
        "sql-mysqlTable");

    assertRecordOffset(startIndex, startIndex+NUM_RECORDS*3, records);
  }

}
