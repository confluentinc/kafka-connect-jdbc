/*
 * Copyright 2026 Confluent Inc.
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

import io.confluent.common.utils.IntegrationTest;
import io.confluent.connect.jdbc.JdbcSourceConnector;
import io.confluent.connect.jdbc.source.EmbeddedDerby;
import io.confluent.connect.jdbc.source.JdbcSourceConnectorConfig;
import io.confluent.connect.jdbc.source.JdbcSourceTaskConfig;
import io.confluent.connect.jdbc.util.RecordQueue;
import org.apache.kafka.connect.runtime.AbstractStatus;
import org.apache.kafka.connect.runtime.ConnectorConfig;
import org.apache.kafka.connect.runtime.rest.entities.ConnectorStateInfo;
import org.apache.kafka.connect.util.clusters.EmbeddedConnectCluster;
import org.apache.kafka.test.TestUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.Assert.assertEquals;

/**
 * On a real Connect worker, pausing the connector must stop it from querying the database. The
 * worker never calls {@code SourceTask.stop()} on pause, it only stops calling {@code poll()}.
 *
 * <p>Every execution of the connector's query calls the Derby function {@code QUERY_COUNT()}
 * once (the table holds one row), so {@link #QUERIES} is the number of queries the database ran.
 */
@Category(IntegrationTest.class)
public class PauseStopsQueryingIT {

  private static final String CONNECTOR_NAME = "jdbc-source-pause-test";
  private static final String TOPIC = "pause-test";
  private static final long POLL_INTERVAL_MS = 200;
  private static final long WINDOW_MS = 2 * RecordQueue.DEFAULT_MAX_POLL_IDLE.toMillis();
  private static final long TIMEOUT_MS = TimeUnit.SECONDS.toMillis(60);

  static final AtomicLong QUERIES = new AtomicLong();

  /** Called by Derby once per row, through the QUERY_COUNT() SQL function. */
  public static int countQuery() {
    QUERIES.incrementAndGet();
    return 1;
  }

  private EmbeddedDerby db;
  private EmbeddedConnectCluster connect;

  @Before
  public void setUp() throws Exception {
    QUERIES.set(0);
    db = new EmbeddedDerby("pause");
    db.createTable("T", "ID", "INT NOT NULL");
    db.insert("T", "ID", 1);
    try (Statement stmt = db.getConnection().createStatement()) {
      stmt.execute("CREATE FUNCTION QUERY_COUNT() RETURNS INT PARAMETER STYLE JAVA "
          + "NO SQL LANGUAGE JAVA EXTERNAL NAME '"
          + PauseStopsQueryingIT.class.getName() + ".countQuery'");
    }

    connect = new EmbeddedConnectCluster.Builder()
        .name("pause-test-connect-cluster")
        .numWorkers(1)
        .brokerProps(new Properties())
        .build();
    connect.start();
    connect.kafka().createTopic(TOPIC, 1);
  }

  @After
  public void tearDown() throws Exception {
    if (connect != null) {
      connect.stop();
    }
    db.close();
    db.dropDatabase();
  }

  @Test
  public void pausedConnectorStopsQueryingAndResumesAfterResume() throws Exception {
    connect.configureConnector(CONNECTOR_NAME, connectorProps());
    waitForState(AbstractStatus.State.RUNNING);
    waitForQueriesAbove(3);

    connect.requestPut(
        connect.endpointForResource(String.format("connectors/%s/pause", CONNECTOR_NAME)), "");
    waitForState(AbstractStatus.State.PAUSED);

    // Let anything already in flight finish and the idle limit pass, then count queries over a
    // window twice as long as the idle limit
    Thread.sleep(RecordQueue.DEFAULT_MAX_POLL_IDLE.toMillis() + 2 * POLL_INTERVAL_MS);
    long atStart = QUERIES.get();
    Thread.sleep(WINDOW_MS);
    long whilePaused = QUERIES.get() - atStart;
    assertEquals("queries run while the connector was paused", 0, whilePaused);

    connect.requestPut(
        connect.endpointForResource(String.format("connectors/%s/resume", CONNECTOR_NAME)), "");
    waitForState(AbstractStatus.State.RUNNING);
    waitForQueriesAbove(QUERIES.get() + 3);
  }

  private Map<String, String> connectorProps() {
    Map<String, String> props = new HashMap<>();
    props.put(ConnectorConfig.CONNECTOR_CLASS_CONFIG, JdbcSourceConnector.class.getName());
    props.put(ConnectorConfig.TASKS_MAX_CONFIG, "1");
    props.put(JdbcSourceConnectorConfig.CONNECTION_URL_CONFIG, db.getUrl());
    props.put(JdbcSourceConnectorConfig.MODE_CONFIG, JdbcSourceConnectorConfig.MODE_BULK);
    props.put(JdbcSourceConnectorConfig.QUERY_CONFIG, "SELECT ID, QUERY_COUNT() AS N FROM T");
    props.put(JdbcSourceTaskConfig.TOPIC_PREFIX_CONFIG, TOPIC);
    props.put(JdbcSourceConnectorConfig.POLL_INTERVAL_MS_CONFIG, Long.toString(POLL_INTERVAL_MS));
    return props;
  }

  private void waitForQueriesAbove(long count) throws InterruptedException {
    TestUtils.waitForCondition(() -> QUERIES.get() > count, TIMEOUT_MS,
        () -> "expected more than " + count + " queries, saw " + QUERIES.get());
  }

  private void waitForState(AbstractStatus.State state) throws InterruptedException {
    TestUtils.waitForCondition(() -> {
      try {
        ConnectorStateInfo info = connect.connectorStatus(CONNECTOR_NAME);
        return info.connector().state().equals(state.toString())
            && info.tasks().size() == 1
            && info.tasks().get(0).state().equals(state.toString());
      } catch (Exception e) {
        return false;
      }
    }, TIMEOUT_MS, "connector did not reach " + state);
  }
}
