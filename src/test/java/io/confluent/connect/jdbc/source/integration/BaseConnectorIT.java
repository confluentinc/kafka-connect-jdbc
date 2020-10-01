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
import org.apache.http.client.methods.HttpPost;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.json.JsonConverter;
import org.apache.kafka.connect.runtime.AbstractStatus;
import org.apache.kafka.connect.runtime.rest.entities.ConnectorStateInfo;
import org.apache.kafka.connect.util.clusters.EmbeddedConnectCluster;
import org.apache.kafka.test.IntegrationTest;
import org.apache.kafka.test.TestUtils;
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
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;

@Category(IntegrationTest.class)
public class BaseConnectorIT {

  private static final Logger log = LoggerFactory.getLogger(BaseConnectorIT.class);

  protected static final long CONSUME_MAX_DURATION_MS = TimeUnit.SECONDS.toMillis(100);
  protected static final long CONNECTOR_STARTUP_DURATION_MS = TimeUnit.SECONDS.toMillis(100);
  protected static final String CONNECTOR_NAME = "mysql-jdbc-source";
  protected static final String KAFKA_TOPIC = "testtable";
  protected static final int NUM_RECORDS = 1000;
  protected static final String MAX_TASKS = "1";
  protected EmbeddedConnectCluster connect;
  protected final JsonConverter jsonConverter = new JsonConverter();
  protected static Connection connection;

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

  protected Optional<Boolean> assertDbConnection() {
    try {
      connection = getDbConnection();
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

  /**
   * Confirm that a connector with an exact number of tasks is running.
   *
   * @param connectorName the connector
   * @param numTasks      the expected number of tasks
   * @return true if the connector and tasks are in RUNNING state; false otherwise
   */
  private Optional<Boolean> assertConnectorAndTasksRunning(String connectorName, int numTasks) {
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
  protected Map<String, String> getConnectorProperties() {
    Map<String, String> props = new HashMap<>();
    props.put("connector.class", JdbcSourceConnector.class.getName());
    props.put("tasks.max", MAX_TASKS);
    props.put("confluent.topic.replication.factor", "1");
    props.put("confluent.topic.bootstrap.servers", connect.kafka().bootstrapServers());
    props.put("connection.password", "password");
    props.put("connection.user", "user");
    props.put("connection.url", "jdbc:mysql://localhost:3307/db");
    props.put("dialect.name", "MySqlDatabaseDialect");
    props.put("value.converter", JsonConverter.class.getName());
    props.put("topic.prefix",  "sql-");
    return props;
  }


  protected Connection getDbConnection() throws SQLException {
    return DriverManager.getConnection("jdbc:mysql://localhost:3307/db" , "user", "password");
  }

  protected void assertRecords(int numOfRecords, ConsumerRecords<byte[], byte[]> records) {
    int start = 0;

    if(numOfRecords != records.count()) {
      throw new AssertionError("Invalid number of records found");
    }

    for (ConsumerRecord<byte[], byte[]> record : records) {
      assertEquals(start, record.offset());
      Struct valueStruct = ((Struct) (jsonConverter.toConnectData(KAFKA_TOPIC, record.value()).value()));
      int id = valueStruct.getInt32("id");
      assertEquals(start++, id);
    }
  }

  protected void dropTableIfExists(String tableName) throws SQLException {
    Statement st = connection.createStatement();
    st.executeQuery("drop table if exists " + tableName);
  }

  protected void createTable() throws SQLException {
    Statement st = connection.createStatement();
    String sql = "CREATE TABLE " + KAFKA_TOPIC +
            "(id int not NULL, " +
            " first_name VARCHAR(255), " +
            " last_name VARCHAR(255), " +
            " time1 TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP );";
    st.executeQuery(sql);
  }

  protected void sendTestData(int start, int numOfRecords) throws SQLException {
    String sql = "INSERT INTO testtable(id,first_name,last_name) "
            + "VALUES(?,?,?)";
    PreparedStatement pstmt = connection.prepareStatement(sql);
    for (int i=start; i<start+numOfRecords; i++) {
      pstmt.setInt(1, i);
      pstmt.setString(2, "FirstName");
      pstmt.setString(3, "LastName");
      pstmt.executeUpdate();
    }
    pstmt.close();
  }

  protected void restartConnectorTask(int taskID) {
    // Restart the task having specific 'taskID'.
    String url = connect.endpointForResource(String.format("connectors/%s/tasks", CONNECTOR_NAME));
    url = url + String.format("/%s/restart", String.valueOf(taskID));
    CloseableHttpClient httpClient = HttpClients.createDefault();
    HttpPost postRequest = new HttpPost(url);
    postRequest.addHeader(HttpHeaders.CONTENT_TYPE, MediaType.APPLICATION_JSON);
    try {
      httpClient.execute(postRequest);
    } catch (IOException e) {
      throw new RuntimeException("Unable to restart the task", e);
    }
  }

}
