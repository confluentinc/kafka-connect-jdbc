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

import io.confluent.connect.jdbc.sink.integration.JdbcSinkConnectorIT;
import org.apache.http.HttpHeaders;
import org.apache.http.HttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.connect.json.JsonConverter;
import org.apache.kafka.connect.runtime.rest.entities.ConnectorStateInfo;
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

import javax.ws.rs.core.MediaType;
import java.io.File;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

@Category(IntegrationTest.class)
public class JdbcSourceConnectorIT extends BaseConnectorIT {
  private static final Logger log = LoggerFactory.getLogger(JdbcSinkConnectorIT.class);

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
    int start = 0;
    String url = connect.endpointForResource(String.format("connectors/%s/tasks", CONNECTOR_NAME));
    connect.kafka().createTopic("sql-" + KAFKA_TOPIC);
    dropTableIfExists(KAFKA_TOPIC);
    createTable();
    sendTestDataToMysql(start);
    start += NUM_RECORDS;
    // Add mysql dialect related configurations.
    props.put("connection.password", "password");
    props.put("connection.user", "user");
    props.put("connection.url", "jdbc:mysql://localhost:3307/db");
    props.put("dialect.name", "MySqlDatabaseDialect");
    props.put("value.converter", JsonConverter.class.getName());
    props.put("mode", "incrementing");
    props.put("incrementing.column.name",  "id");
    props.put("topic.prefix",  "sql-");

    // start a source connector
    connect.configureConnector(CONNECTOR_NAME, props);

    // wait for tasks to spin up
    int expectedNumTasks = Integer.valueOf(MAX_TASKS); // or set to actual number
    waitForConnectorToStart(CONNECTOR_NAME, expectedNumTasks);

    Thread.sleep(3000);

    log.info("Waiting for records in destination topic ...");
    ConsumerRecords<byte[], byte[]> records = connect.kafka().consume(
            NUM_RECORDS,
            CONSUME_MAX_DURATION_MS,
            "sql-mysqlTable");
    Assert.assertEquals(NUM_RECORDS, records.count());
    Client client = new Client();
    List<ConnectorStateInfo.TaskState> list = connect.connectorStatus(CONNECTOR_NAME).tasks();
    int id = list.get(0).id();
    url = url + String.format("/%s/restart", String.valueOf(id));
    HttpPost postRequest = new HttpPost(url);
    postRequest.addHeader(HttpHeaders.CONTENT_TYPE, MediaType.APPLICATION_JSON);
    HttpResponse httpResponse = client.getHttpClient().execute(postRequest);
    ConsumerRecords<byte[], byte[]> newRecords = connect.kafka().consume(
            NUM_RECORDS,
            CONSUME_MAX_DURATION_MS,
            "sql-mysqlTable");
    Assert.assertEquals(NUM_RECORDS, newRecords.count());

    sendTestDataToMysql(start);

    waitForConnectorToStart(CONNECTOR_NAME, expectedNumTasks);

    Thread.sleep(1000);

    log.info("Waiting for records in destination topic ...");
    ConsumerRecords<byte[], byte[]> newfRecords = connect.kafka().consume(
            NUM_RECORDS*2,
            CONSUME_MAX_DURATION_MS,
            "sql-mysqlTable");
    Assert.assertEquals(NUM_RECORDS*2, newfRecords.count());

//    client.getHttpClient().execute()
  }

  @Test
  public void testOffsetPickup() throws Exception {
    int start = 0;
    String url = connect.endpointForResource(String.format("connectors/%s/tasks", CONNECTOR_NAME));
    connect.kafka().createTopic("sql-" + KAFKA_TOPIC);
    dropTableIfExists(KAFKA_TOPIC);
    createTable();
    sendTestDataToMysql(start);
    start += NUM_RECORDS;
    // Add mysql dialect related configurations.
    props.put("connection.password", "password");
    props.put("connection.user", "user");
    props.put("connection.url", "jdbc:mysql://localhost:3307/db");
    props.put("dialect.name", "MySqlDatabaseDialect");
    props.put("value.converter", JsonConverter.class.getName());
    props.put("mode", "incrementing");
    props.put("incrementing.column.name",  "id");
    props.put("topic.prefix",  "sql-");

    // start a source connector
    connect.configureConnector(CONNECTOR_NAME, props);

    // wait for tasks to spin up
    int expectedNumTasks = Integer.valueOf(MAX_TASKS); // or set to actual number
    waitForConnectorToStart(CONNECTOR_NAME, expectedNumTasks);

    Thread.sleep(3000);

    log.info("Waiting for records in destination topic ...");
    ConsumerRecords<byte[], byte[]> records = connect.kafka().consume(
            NUM_RECORDS,
            CONSUME_MAX_DURATION_MS,
            "sql-mysqlTable");
    Assert.assertEquals(NUM_RECORDS, records.count());
    connect.deleteConnector(CONNECTOR_NAME);

// start a source connector
    connect.configureConnector(CONNECTOR_NAME, props);

    // wait for tasks to spin up
    int expected = Integer.valueOf(MAX_TASKS); // or set to actual number
    waitForConnectorToStart(CONNECTOR_NAME, expected);

    Thread.sleep(3000);

    ConsumerRecords<byte[], byte[]> newRecords = connect.kafka().consume(
            NUM_RECORDS,
            CONSUME_MAX_DURATION_MS,
            "sql-mysqlTable");
    Assert.assertEquals(NUM_RECORDS, newRecords.count());
    sendTestDataToMysql(start);
    Thread.sleep(TimeUnit.SECONDS.toMillis(3));
    ConsumerRecords<byte[], byte[]> newestRecords = connect.kafka().consume(
            NUM_RECORDS*2,
            CONSUME_MAX_DURATION_MS,
            "sql-mysqlTable");
  }

  @Test
  public void testConfigUpdate() throws InterruptedException, SQLException {
    int start = 0;
    connect.kafka().createTopic("sql-" + KAFKA_TOPIC);
    dropTableIfExists(KAFKA_TOPIC);
    createTimestampTable();
    sendTestTimestampDataToMysql(start);
    // Add mysql dialect related configurations.
    props.put("connection.password", "password");
    props.put("connection.user", "user");
    props.put("connection.url", "jdbc:mysql://localhost:3307/db");
    props.put("dialect.name", "MySqlDatabaseDialect");
    props.put("value.converter", JsonConverter.class.getName());
    props.put("mode", "incrementing");
    props.put("incrementing.column.name",  "id");
    props.put("topic.prefix",  "sql-");

    // start a source connector
    connect.configureConnector(CONNECTOR_NAME, props);

    // wait for tasks to spin up
    int expectedNumTasks = Integer.valueOf(MAX_TASKS); // or set to actual number
    waitForConnectorToStart(CONNECTOR_NAME, expectedNumTasks);

    Thread.sleep(3000);

    log.info("Waiting for records in destination topic ...");
    ConsumerRecords<byte[], byte[]> records = connect.kafka().consume(
            NUM_RECORDS,
            CONSUME_MAX_DURATION_MS,
            "sql-mysqlTable");
    Assert.assertEquals(NUM_RECORDS, records.count());
    connect.deleteConnector(CONNECTOR_NAME);

    props.put("mode", "timestamp");
    props.put("timestamp.column.name",  "timeof");
    props.put("topic.prefix",  "sql-");

    sendTestTimestampDataToMysql(start+NUM_RECORDS);


    // start a source connector
    connect.configureConnector(CONNECTOR_NAME, props);

    // wait for tasks to spin up
    int expected = Integer.valueOf(MAX_TASKS); // or set to actual number
    waitForConnectorToStart(CONNECTOR_NAME, expected);

    Thread.sleep(3000);

    ConsumerRecords<byte[], byte[]> newRecords = connect.kafka().consume(
            NUM_RECORDS,
            CONSUME_MAX_DURATION_MS,
            "sql-mysqlTable");

    Assert.assertEquals(NUM_RECORDS*2, newRecords.count());

  }


  private void dropTableIfExists(String kafkaTopic) throws SQLException {
    Statement st = connection.createStatement();
    st.executeQuery("drop table if exists " + kafkaTopic);
  }

  private void createTable() throws SQLException {
    Statement st = connection.createStatement();
    String sql = "CREATE TABLE " + KAFKA_TOPIC +
            "(id INTEGER not NULL, " +
            " first_name VARCHAR(255), " +
            " last_name VARCHAR(255), " +
            " age INTEGER, " +
            " PRIMARY KEY ( id ))";
    st.executeQuery(sql);
  }

  private void createTimestampTable() throws SQLException {
    Statement st = connection.createStatement();
    String sql = "CREATE TABLE " + KAFKA_TOPIC +
            "(id INTEGER not NULL, " +
            " first_name VARCHAR(255), " +
            " last_name VARCHAR(255), " +
            " timeof TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP, " +
            " age INTEGER, " +
            " PRIMARY KEY ( id ))";
    st.executeQuery(sql);
  }

  private void sendTestDataToMysql(int start) throws SQLException {
    String sql = "INSERT INTO mysqlTable(id,first_name,last_name,age) "
            + "VALUES(?,?,?,?)";
    PreparedStatement pstmt = connection.prepareStatement(sql);
    for (int i=start; i<start+NUM_RECORDS; i++) {
      pstmt.setLong(1, i);
      pstmt.setString(2, "FirstName");
      pstmt.setString(3, "LastName");
      pstmt.setLong(4, 20);
      pstmt.executeUpdate();
    }
    pstmt.close();
  }

  private void sendTestTimestampDataToMysql(int start) throws SQLException {
    java.util.Date date=new java.util.Date();

    java.sql.Date sqlDate=new java.sql.Date(date.getTime());
    java.sql.Timestamp sqlTime=new java.sql.Timestamp(date.getTime());

    String sql = "INSERT INTO mysqlTable(id,first_name,last_name,timeof,age) "
            + "VALUES(?,?,?,?,?)";
    PreparedStatement pstmt = connection.prepareStatement(sql);
    for (int i=start; i<start+NUM_RECORDS; i++) {
      pstmt.setLong(1, i);
      pstmt.setString(2, "FirstName");
      pstmt.setString(3, "LastName");
      pstmt.setTimestamp(4,sqlTime);
      pstmt.setLong(5, 20);
      pstmt.executeUpdate();
    }
    pstmt.close();
  }

}
