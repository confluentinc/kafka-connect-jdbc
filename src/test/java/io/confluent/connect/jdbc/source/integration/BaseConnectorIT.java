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

import io.confluent.connect.jdbc.JdbcSourceConnector;
import org.apache.http.HttpHeaders;
import org.apache.http.HttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.connect.json.JsonConverter;
import org.apache.kafka.connect.runtime.AbstractStatus;
import org.apache.kafka.connect.runtime.rest.entities.ConnectorStateInfo;
import org.apache.kafka.connect.util.clusters.EmbeddedConnectCluster;
import org.apache.kafka.test.IntegrationTest;
import org.apache.kafka.test.TestUtils;
import org.junit.Assert;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.core.MediaType;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

@Category(IntegrationTest.class)
public class BaseConnectorIT {

  private static final Logger log = LoggerFactory.getLogger(BaseConnectorIT.class);

  protected static final long CONSUME_MAX_DURATION_MS = TimeUnit.SECONDS.toMillis(100);
  protected static final long CONNECTOR_STARTUP_DURATION_MS = TimeUnit.SECONDS.toMillis(100);
  protected static final String CONNECTOR_NAME = "mysql-jdbc-source";
  protected static final String KAFKA_TOPIC = "mysqlTable";
  protected static final int NUM_RECORDS = 25;

  protected static final String MAX_TASKS = "1";

  protected EmbeddedConnectCluster connect;

  protected void startConnect() {
      Properties properties = new Properties();
      properties.put("auto.create.topics.enable", "true");
      connect = new EmbeddedConnectCluster.Builder()
              .name(CONNECTOR_NAME)
              .brokerProps(properties)
              .build();
      connect.start();
  }

  /**
   * Wait up to {@link #CONNECTOR_STARTUP_DURATION_MS maximum time limit} for the connector with the given
   * name to start the specified number of tasks.
   *
   * @param name     the name of the connector
   * @param numTasks the minimum number of tasks that are expected
   * @return the time this method discovered the connector has started, in milliseconds past epoch
   * @throws InterruptedException if this was interrupted
   */
  protected long waitForConnectorToStart(String name, int numTasks) throws InterruptedException {
    TestUtils.waitForCondition(
        () -> assertConnectorAndTasksRunning(name, numTasks).orElse(false),
        CONNECTOR_STARTUP_DURATION_MS,
        "Connector tasks did not start in time."
    );
    return System.currentTimeMillis();
  }

  /**
   * Confirm that a connector with an exact number of tasks is running.
   *
   * @param connectorName the connector
   * @param numTasks      the expected number of tasks
   * @return true if the connector and tasks are in RUNNING state; false otherwise
   */
  protected Optional<Boolean> assertConnectorAndTasksRunning(String connectorName, int numTasks) {
    try {
      ConnectorStateInfo info = connect.connectorStatus(connectorName);
      boolean result = info != null
          && info.tasks().size() == numTasks
          && info.connector().state().equals(AbstractStatus.State.RUNNING.toString())
          && info.tasks().stream().allMatch(s -> s.state().equals(AbstractStatus.State.RUNNING.toString()));
      return Optional.of(result);
    } catch (Exception e) {
      log.warn("Could not check connector state info. Connector might not have started.");
      return Optional.empty();
    }
  }

  /**
   * Create a map of Common connector properties.
   *
   * @return : Map of props.
   */
  public Map<String, String> getCommonProps() {

    Map<String, String> props = new HashMap<>();
    props.put("connector.class", JdbcSourceConnector.class.getName());
    props.put("tasks.max", MAX_TASKS);
    // license properties
    props.put("confluent.topic.replication.factor", "1");
    props.put("confluent.topic.bootstrap.servers", connect.kafka().bootstrapServers());

    // connector-specific properties
    return props;
  }

  protected Connection getConnection() throws SQLException {
    //Class.forName("com.mysql.jdbc.Driver");
    return DriverManager.getConnection("jdbc:mysql://localhost:3307/db" , "user", "password");
  }

  protected void assertRecordOffset(int start, int end, ConsumerRecords<byte[], byte[]> records) {
    for (ConsumerRecord<byte[], byte[]> record : records) {
      Assert.assertEquals(start++, record.offset());
    }
    if(start!=end){
      throw new AssertionError("Invalid number of records found");
    }
  }

  protected void addConnectionProperties(Map<String, String> props) {
    props.put("connection.password", "password");
    props.put("connection.user", "user");
    props.put("connection.url", "jdbc:mysql://localhost:3307/db");
    props.put("dialect.name", "MySqlDatabaseDialect");
    props.put("value.converter", JsonConverter.class.getName());
    props.put("topic.prefix",  "sql-");
  }

  protected void dropTableIfExists(String kafkaTopic, Connection connection) throws SQLException {
    Statement st = connection.createStatement();
    st.executeQuery("drop table if exists " + kafkaTopic);
  }

  protected void createTable(Connection connection) throws SQLException {
    Statement st = connection.createStatement();
    String sql = "CREATE TABLE " + KAFKA_TOPIC +
            "(id INTEGER not NULL, " +
            " first_name VARCHAR(255), " +
            " last_name VARCHAR(255), " +
            " age INTEGER, " +
            " PRIMARY KEY ( id ))";
    st.executeQuery(sql);
  }

  protected void createTimestampTable(Connection connection) throws SQLException {
    Statement st = connection.createStatement();
    String sql = "CREATE TABLE " + KAFKA_TOPIC +
            "(id int not NULL, " +
            " first_name VARCHAR(255), " +
            " last_name VARCHAR(255), " +
            " time1 TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP );";
    st.executeQuery(sql);
  }

  protected void sendTestTimestampDataToMysql(int start, Connection connection) throws SQLException {
    String sql = "INSERT INTO mysqlTable(id,first_name,last_name) "
            + "VALUES(?,?,?)";
    PreparedStatement pstmt = connection.prepareStatement(sql);
    for (int i=start; i<start+NUM_RECORDS; i++) {
      pstmt.setInt(1, i);
      pstmt.setString(2, "FirstName");
      pstmt.setString(3, "LastName");
      pstmt.executeUpdate();
    }
    pstmt.close();
  }

  protected void sendTestDataToMysql(int start, Connection connection) throws SQLException {
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

  protected int restartConnectorTask() throws IOException {
    HttpClient httpClient = new HttpClient();
    List<ConnectorStateInfo.TaskState> list = connect.connectorStatus(CONNECTOR_NAME).tasks();
    int id = list.get(0).id();
    String url = connect.endpointForResource(String.format("connectors/%s/tasks", CONNECTOR_NAME));
    url = url + String.format("/%s/restart", String.valueOf(id));
    HttpPost postRequest = new HttpPost(url);
    postRequest.addHeader(HttpHeaders.CONTENT_TYPE, MediaType.APPLICATION_JSON);
    HttpResponse httpResponse = httpClient.getHttpClient().execute(postRequest);
    return httpResponse.getStatusLine().getStatusCode();
  }

}
