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

package io.confluent.connect.jdbc;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.runtime.AbstractStatus;
import org.apache.kafka.connect.runtime.rest.entities.ConnectorStateInfo;
import org.apache.kafka.connect.util.clusters.EmbeddedConnectCluster;
import org.apache.kafka.test.IntegrationTest;
import org.apache.kafka.test.TestUtils;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.DockerComposeContainer;

import java.io.File;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

@Category(IntegrationTest.class)
public class BaseConnectorIT {

  private static final Logger log = LoggerFactory.getLogger(BaseConnectorIT.class);

  protected static final long CONSUME_MAX_DURATION_MS = TimeUnit.SECONDS.toMillis(300);
  protected static final long CONNECTOR_STARTUP_DURATION_MS = TimeUnit.SECONDS.toMillis(300);
  protected static final String CONNECTOR_NAME = "mysql-jdbc-sink";
  protected static final String KAFKA_TOPIC = "mysqlTable";

  protected static final String MAX_TASKS = "1";

  protected static final Schema SCHEMA = SchemaBuilder.struct().name("com.example.Person")
      .field("userId", Schema.OPTIONAL_INT32_SCHEMA)
      .field("firstName", Schema.STRING_SCHEMA)
      .field("lastName", Schema.STRING_SCHEMA)
      .field("age", Schema.OPTIONAL_INT32_SCHEMA)
      .build();

  protected EmbeddedConnectCluster connect;
  protected Connection connection;

  protected void startConnect() throws IOException {
    connect = new EmbeddedConnectCluster.Builder()
        .name("jdbc-connect-cluster")
        .build();

    connect.start();
  }

  protected static DockerComposeContainer pumbaPauseContainer;
  protected void startPumbaPauseContainer() {
    pumbaPauseContainer =
        new DockerComposeContainer(new File("src/test/docker/configB/pumba-pause-compose.yml"));
    pumbaPauseContainer.start();
  }

  protected static DockerComposeContainer pumbaDelayContainer;
  protected void startPumbaDelayContainer() {
    pumbaDelayContainer =
        new DockerComposeContainer(new File("src/test/docker/configB/pumba-delay-compose.yml"));
    pumbaDelayContainer.start();
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

  protected long waitForConnectorToWriteDataIntoMysql(Connection connection, String connectorName, int numTasks, String tableName, int numberOfRecords) throws InterruptedException {
    TestUtils.waitForCondition(
        () -> assertRecordsCountInMysql(connection, connectorName, numTasks, tableName, numberOfRecords).orElse(false),
        CONSUME_MAX_DURATION_MS,
        "Either writing into table has not started or row count did not matched."
    );
    return System.currentTimeMillis();
  }

  private Optional<Boolean> assertRecordsCountInMysql(Connection connection, String name, int numTasks, String tableName, int numberOfRecords) {
    try {
      Statement st = connection.createStatement();
      ResultSet rs = st.executeQuery("SELECT COUNT(*) AS rowcount FROM " + tableName);
      rs.next();
      ConnectorStateInfo info = connect.connectorStatus(name);
      boolean result = info != null
          && info.tasks().size() == numTasks
          && info.connector().state().equals(AbstractStatus.State.RUNNING.toString())
          && info.tasks().stream().allMatch(s -> s.state().equals(AbstractStatus.State.RUNNING.toString()));
      int targetRowCount = rs.getInt("rowcount");
      boolean targetRowCountStatus = rs.getInt("rowcount") == numberOfRecords;
      if (!targetRowCountStatus) {
        log.warn("Row count did not matched. Expected {}, Actual {}.", numberOfRecords, targetRowCount);
      }
      return Optional.of(result && targetRowCountStatus);
    } catch (SQLException e) {
      log.warn("Getting sql exception while counting table's row..");
      return Optional.empty();
    } catch (Exception e) {
      log.warn("Could not check connector state info, will retry shortly ...");
      return Optional.empty();
    }
  }

  protected Optional<Boolean> assertDbConnection() {
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

  protected void dropTableIfExists(String kafkaTopic) throws SQLException {
    Statement st = connection.createStatement();
    st.executeQuery("drop table if exists " + kafkaTopic);
  }

  protected boolean tableExist(Connection conn, String tableName) throws SQLException {
    boolean tExists = false;
    try (ResultSet rs = conn.getMetaData().getTables(null, null, tableName, null)) {
      while (rs.next()) {
        String tName = rs.getString("TABLE_NAME");
        if (tName != null && tName.equals(tableName)) {
          tExists = true;
          break;
        }
      }
    }
    return tExists;
  }

  protected Connection getConnection() throws SQLException {
    return DriverManager.getConnection("jdbc:mysql://localhost:3306/db" , "root", "password");
  }
}
