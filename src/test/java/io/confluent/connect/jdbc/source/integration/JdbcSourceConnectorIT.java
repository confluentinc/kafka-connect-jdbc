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
import org.apache.kafka.clients.consumer.ConsumerRecords;
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
import java.sql.Connection;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
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
      new DockerComposeContainer(new File("src/test/docker/configA/docker-compose.yml"));

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
  public void close() throws SQLException, IOException {
    // delete connector
    connect.deleteConnector(CONNECTOR_NAME);
    connect.stop();
  }

  @AfterClass
  public static void closeConnection() throws SQLException {
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
    } catch (ClassNotFoundException | SQLException | InterruptedException e) {
      return Optional.empty();
    }
  }

  @Test
  public void testWithAutoCreateEnabled() throws Exception {
    //connect.kafka().createTopic(KAFKA_TOPIC);
    dropTableIfExists(KAFKA_TOPIC);
    sendTestDataToMysql();

    // start a source connector
    connect.configureConnector(CONNECTOR_NAME, props);

    // wait for tasks to spin up
    int expectedNumTasks = Integer.valueOf(MAX_TASKS); // or set to actual number
    waitForConnectorToStart(CONNECTOR_NAME, expectedNumTasks);

    Thread.sleep(3000);

    log.info("Waiting for records in destination topic ...");
    ConsumerRecords<byte[], byte[]> records = connect.kafka().consume(
        1,
        CONSUME_MAX_DURATION_MS,
        KAFKA_TOPIC
    );
    Assert.assertEquals(1, records.count());
  }

  private void dropTableIfExists(String kafkaTopic) throws SQLException {
    Statement st = connection.createStatement();
    st.executeQuery("drop table if exists " + "mysqlTable");
  }

  private void sendTestDataToMysql() throws SQLException {
    Statement st = connection.createStatement();
    String sql = "CREATE TABLE mysqlTable " +
        "(id INTEGER not NULL, " +
        " first_name VARCHAR(255), " +
        " last_name VARCHAR(255), " +
        " age INTEGER, " +
        " PRIMARY KEY ( id ))";
    st.executeQuery(sql);
    sql = "INSERT INTO mysqlTable(id,first_name,last_name,age) "
        + "VALUES(?,?,?,?)";
    PreparedStatement pstmt = connection.prepareStatement(sql);
    pstmt.setLong(1, 1);
    pstmt.setString(2, "FirstName");
    pstmt.setString(3, "LastName");
    pstmt.setLong(4, 20);
    pstmt.executeUpdate();
    pstmt.close();
  }
}
