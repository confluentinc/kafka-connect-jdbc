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

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.json.JsonConverterConfig;
import org.apache.kafka.connect.runtime.rest.entities.ConnectorStateInfo;
import org.apache.kafka.connect.storage.ConverterConfig;
import org.apache.kafka.connect.storage.ConverterType;
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
import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;

@Category(IntegrationTest.class)
public class JdbcSourceConnectorIT extends BaseConnectorIT {
  private static final Logger log = LoggerFactory.getLogger(JdbcSourceConnectorIT.class);
  public static final String SQL_TESTTABLE = "sql-testtable";

  private Map<String, String> props;

  @ClassRule
  public static DockerComposeContainer compose =
          new DockerComposeContainer(new File("src/test/docker/docker-compose.yml"));

  @Before
  public void setup() throws Exception {
    startConnect();
    connect.kafka().createTopic(KAFKA_TOPIC, 1);
    props = getConnectorProperties();
    if (connection == null) {
      TestUtils.waitForCondition(() -> assertDbConnection().orElse(false),
          TimeUnit.SECONDS.toMillis(30),
          "Failed to start the container.");
    }
    dropTableIfExists(KAFKA_TOPIC);
    Map<String, String> converterConfig = new HashMap<>();
    converterConfig.put(JsonConverterConfig.SCHEMAS_CACHE_SIZE_CONFIG, "100");
    converterConfig.put(ConverterConfig.TYPE_CONFIG, ConverterType.VALUE.getName());
    converterConfig.put(JsonConverterConfig.SCHEMAS_ENABLE_CONFIG, "true");
    jsonConverter.configure(converterConfig);
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

  @Test
  public void testTaskBounce() throws Exception {
    int startIndex = 0;

    createTable();
    sendTestData(startIndex, NUM_RECORDS);

    connect.kafka().createTopic("sql-" + KAFKA_TOPIC);

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
        SQL_TESTTABLE);

    assertRecords(startIndex+NUM_RECORDS, records);

    // restart Connector Task
    List<ConnectorStateInfo.TaskState> tasks = connect.connectorStatus(CONNECTOR_NAME).tasks();
    // Bounce source tasks while connector is processing records.
    for(int i = 0 ; i < tasks.size() ; i++ ) {
      // Fetch ID of one task to restart it.
      int taskID = tasks.get(i).id();
      // Restart task with specific 'taskID'.
      restartConnectorTask(taskID);
      // Adding a slight delay so that source task can initiate.
      TimeUnit.SECONDS.sleep(1);
    };

    connect.kafka().consume(
        NUM_RECORDS,
        CONSUME_MAX_DURATION_MS,
        SQL_TESTTABLE);

    startIndex += NUM_RECORDS;
    sendTestData(startIndex, NUM_RECORDS);

    log.info("Waiting for records in destination topic ...");
    records = connect.kafka().consume(
        NUM_RECORDS*2,
        CONSUME_MAX_DURATION_MS,
        SQL_TESTTABLE);

    assertRecords(NUM_RECORDS*2, records);

  }

  @Test
  public void testResumingOffsetsAfterConnectorRestart() throws Exception {
    int startIndex = 0;

    createTable();
    sendTestData(startIndex, NUM_RECORDS);

    connect.kafka().createTopic("sql-" + KAFKA_TOPIC);

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
        SQL_TESTTABLE);

    assertRecords(startIndex+NUM_RECORDS, records);

    // delete the connector
    connect.deleteConnector(CONNECTOR_NAME);

    startIndex += NUM_RECORDS;
    sendTestData(startIndex, NUM_RECORDS);

    // start a source connector
    connect.configureConnector(CONNECTOR_NAME, props);

    // wait for tasks to spin up
    int expected = Integer.valueOf(MAX_TASKS); // or set to actual number
    waitForConnectorToStart(CONNECTOR_NAME, expected);

    records = connect.kafka().consume(
        NUM_RECORDS*2,
        CONSUME_MAX_DURATION_MS,
        SQL_TESTTABLE);

    assertRecords(NUM_RECORDS*2, records);

  }

  @Test
  public void testConnectorModeChange() throws InterruptedException, SQLException {
    int startIndex = 0;

    createTable();
    sendTestData(startIndex, NUM_RECORDS);

    connect.kafka().createTopic("sql-" + KAFKA_TOPIC);

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
        CONSUME_MAX_DURATION_MS,
        SQL_TESTTABLE);

    assertRecords(NUM_RECORDS, records);

    // delete the connector
    connect.deleteConnector(CONNECTOR_NAME);

    // change mode
    props.replace("mode", "incrementing");
    props.put("incrementing.column.name",  "id");
    props.remove("timestamp.column.name");

    startIndex += NUM_RECORDS;
    sendTestData(startIndex, NUM_RECORDS);

    // start a source connector
    connect.configureConnector(CONNECTOR_NAME, props);

    // wait for tasks to spin up
    int expected = Integer.valueOf(MAX_TASKS); // or set to actual number
    waitForConnectorToStart(CONNECTOR_NAME, expected);

    // consumes records from the beginning
    records = connect.kafka().consume(
        NUM_RECORDS * 3,
        CONSUME_MAX_DURATION_MS,
        SQL_TESTTABLE);

    // Assert first thousand records with id 0-999 and next 2000 records with id 0-1999
    int offset = 0;
    int start = 0;
    int flag = 0;
    for (ConsumerRecord<byte[], byte[]> record : records) {
      assertEquals(offset++, record.offset());
      Struct valueStruct = ((Struct) (jsonConverter.toConnectData(KAFKA_TOPIC, record.value()).value()));
      int id = valueStruct.getInt32("id");
      assertEquals(start++, id);
      if(start == 1000 && flag == 0) {
        start = 0;
        flag = 1;
      }
    }
  }

}
