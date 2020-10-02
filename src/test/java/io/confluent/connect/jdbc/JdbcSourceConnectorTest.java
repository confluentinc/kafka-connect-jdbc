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

import io.confluent.connect.jdbc.dialect.DatabaseDialects;
import io.confluent.connect.jdbc.dialect.GenericDatabaseDialect;
import org.apache.derby.iapi.db.Database;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.connect.errors.ConnectException;
import org.easymock.EasyMock;
import org.easymock.Mock;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.api.easymock.PowerMock;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import io.confluent.connect.jdbc.dialect.DatabaseDialect;
import io.confluent.connect.jdbc.source.EmbeddedDerby;
import io.confluent.connect.jdbc.source.JdbcSourceConnectorConfig;
import io.confluent.connect.jdbc.source.JdbcSourceTask;
import io.confluent.connect.jdbc.source.JdbcSourceTaskConfig;
import io.confluent.connect.jdbc.util.CachedConnectionProvider;
import io.confluent.connect.jdbc.util.ExpressionBuilder;
import io.confluent.connect.jdbc.util.TableId;

import javax.xml.crypto.Data;

import static org.easymock.EasyMock.anyInt;
import static org.easymock.EasyMock.anyObject;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

@RunWith(PowerMockRunner.class)
@PrepareForTest({JdbcSourceConnector.class, DatabaseDialect.class,  DatabaseDialects.class})
@PowerMockIgnore("javax.management.*")
public class JdbcSourceConnectorTest {

  private JdbcSourceConnector connector;
  private EmbeddedDerby db;
  private Map<String, String> connProps;
//<<<<<<< HEAD
//
//  public static class MockJdbcSourceConnector extends JdbcSourceConnector {
//    CachedConnectionProvider provider;
//    public MockJdbcSourceConnector() {}
//    public MockJdbcSourceConnector(CachedConnectionProvider provider) {
//      this.provider = provider;
//    }
//    @Override
//    protected CachedConnectionProvider connectionProvider(
//            int maxConnAttempts,
//            long retryBackoff
//    ) {
//      return provider;
//    }
//  }
//
//=======
  private AbstractConfig abstractConfig;
//>>>>>>> 6eae2969... Removed CachedConnectionProvider from Connector Thread
  @Mock
  private DatabaseDialect dialect;

  @Before
  public void setup() {
    connector = new JdbcSourceConnector();
    db = new EmbeddedDerby();
    connProps = new HashMap<>();
    connProps.put(JdbcSourceConnectorConfig.CONNECTION_URL_CONFIG, db.getUrl());
    connProps.put(JdbcSourceConnectorConfig.MODE_CONFIG, JdbcSourceConnectorConfig.MODE_BULK);
    connProps.put(JdbcSourceConnectorConfig.TOPIC_PREFIX_CONFIG, "test-");
    abstractConfig = new JdbcSourceConnectorConfig(connProps);
  }

  @After
  public void tearDown() throws Exception {
    db.close();
    db.dropDatabase();
  }

  @Test
  public void testTaskClass() {
    assertEquals(JdbcSourceTask.class, connector.taskClass());
  }

  @Test(expected = ConnectException.class)
  public void testMissingUrlConfig() throws Exception {
    HashMap<String, String> connProps = new HashMap<>();
    connProps.put(JdbcSourceConnectorConfig.MODE_CONFIG, JdbcSourceConnectorConfig.MODE_BULK);
    connector.start(connProps);
  }

  @Test(expected = ConnectException.class)
  public void testMissingModeConfig() throws Exception {
    HashMap<String, String> connProps = new HashMap<>();
    connProps.put(JdbcSourceConnectorConfig.CONNECTION_URL_CONFIG, db.getUrl());
    connector.start(Collections.<String, String>emptyMap());
  }

  @Test(expected = ConnectException.class)
  public void testStartConnectionFailure() throws Exception {
    // Invalid URL
    connector.start(Collections.singletonMap(JdbcSourceConnectorConfig.CONNECTION_URL_CONFIG, "jdbc:foo"));
  }

  @Test
  public void testStartStop() throws Exception {
//<<<<<<< HEAD
//    CachedConnectionProvider mockCachedConnectionProvider = PowerMock.createMock(CachedConnectionProvider.class);
//    connector  = new MockJdbcSourceConnector(mockCachedConnectionProvider);
//=======
//>>>>>>> 6eae2969... Removed CachedConnectionProvider from Connector Thread
    // Should request a connection, then should close it on stop(). The background thread may also
    // request connections any time it performs updates.
    connector = new JdbcSourceConnector();

    // mocked to return a mocked dialect
    PowerMock.mockStatic(DatabaseDialects.class);
    // mocked to return mocked connection
    GenericDatabaseDialect dbDialect = EasyMock.createMock(GenericDatabaseDialect.class);
    Connection conn = EasyMock.createMock(Connection.class);

    EasyMock.expect(dbDialect.getConnection()).andReturn(conn).anyTimes();

    //Stubs to keep background thread working till close is called
    EasyMock.expect(dbDialect.isConnectionValid(anyObject(), anyInt())).andReturn(true).anyTimes();
    EasyMock.expect(dbDialect.tableIds(anyObject())).andStubThrow(new SQLException());

    //deliver mocked dialect
    EasyMock.expect(
            DatabaseDialects.findBestFor(
                    db.getUrl(),
                    abstractConfig
            )
    ).andReturn(dbDialect);

    dbDialect.close();
    EasyMock.expectLastCall().times(1); // verify dbDialect closed when stopped

    EasyMock.replay(dbDialect);
    PowerMock.replayAll();

    connector.start(connProps);
    connector.stop();

    PowerMock.verifyAll(); // DatabaseDialects.findBestFor(..) called
  }

  @Test
  public void testNoTablesNoTasks() throws Exception {
    // Tests case where there are no readable tables and ensures that no tasks
    // are returned to be run
    connector.start(connProps);
    List<Map<String, String>> configs = connector.taskConfigs(3);
    assertTrue(configs.isEmpty());
    connector.stop();
  }

  @Test
  public void testPartitioningOneTable() throws Exception {
    // Tests simplest case where we have exactly 1 table and also ensures we return fewer tasks
    // if there aren't enough tables for the max # of tasks
    db.createTable("test", "id", "INT NOT NULL");
    connector.start(connProps);
    List<Map<String, String>> configs = connector.taskConfigs(10);
    assertEquals(1, configs.size());
    assertTaskConfigsHaveParentConfigs(configs);
    assertEquals(tables("test"), configs.get(0).get(JdbcSourceTaskConfig.TABLES_CONFIG));
    assertNull(configs.get(0).get(JdbcSourceTaskConfig.QUERY_CONFIG));
    connector.stop();
  }

  @Test
  public void testPartitioningManyTables() throws Exception {
    // Tests distributing tables across multiple tasks, in this case unevenly
    db.createTable("test1", "id", "INT NOT NULL");
    db.createTable("test2", "id", "INT NOT NULL");
    db.createTable("test3", "id", "INT NOT NULL");
    db.createTable("test4", "id", "INT NOT NULL");
    connector.start(connProps);
    List<Map<String, String>> configs = connector.taskConfigs(3);
    assertEquals(3, configs.size());
    assertTaskConfigsHaveParentConfigs(configs);

    assertEquals(tables("test1","test2"), configs.get(0).get(JdbcSourceTaskConfig.TABLES_CONFIG));
    assertNull(configs.get(0).get(JdbcSourceTaskConfig.QUERY_CONFIG));
    assertEquals(tables("test3"), configs.get(1).get(JdbcSourceTaskConfig.TABLES_CONFIG));
    assertNull(configs.get(1).get(JdbcSourceTaskConfig.QUERY_CONFIG));
    assertEquals(tables("test4"), configs.get(2).get(JdbcSourceTaskConfig.TABLES_CONFIG));
    assertNull(configs.get(2).get(JdbcSourceTaskConfig.QUERY_CONFIG));

    connector.stop();
  }

  @Test
  public void testPartitioningQuery() throws Exception {
    // Tests "partitioning" when config specifies running a custom query
    db.createTable("test1", "id", "INT NOT NULL");
    db.createTable("test2", "id", "INT NOT NULL");
    final String sample_query = "SELECT foo, bar FROM sample_table";
    connProps.put(JdbcSourceConnectorConfig.QUERY_CONFIG, sample_query);
    connector.start(connProps);
    List<Map<String, String>> configs = connector.taskConfigs(3);
    assertEquals(1, configs.size());
    assertTaskConfigsHaveParentConfigs(configs);

    assertEquals("", configs.get(0).get(JdbcSourceTaskConfig.TABLES_CONFIG));
    assertEquals(sample_query, configs.get(0).get(JdbcSourceTaskConfig.QUERY_CONFIG));

    connector.stop();
  }

  @Test(expected = ConnectException.class)
  public void testConflictingQueryTableSettings() {
    final String sample_query = "SELECT foo, bar FROM sample_table";
    connProps.put(JdbcSourceConnectorConfig.QUERY_CONFIG, sample_query);
    connProps.put(JdbcSourceConnectorConfig.TABLE_WHITELIST_CONFIG, "foo,bar");
    connector.start(connProps);
  }

  private void assertTaskConfigsHaveParentConfigs(List<Map<String, String>> configs) {
    for (Map<String, String> config : configs) {
      assertEquals(this.db.getUrl(),
                   config.get(JdbcSourceConnectorConfig.CONNECTION_URL_CONFIG));
    }
  }

  private String tables(String... names) {
    List<TableId> tableIds = new ArrayList<>();
    for (String name : names) {
      tableIds.add(new TableId(null, "APP", name));
    }
    ExpressionBuilder builder = ExpressionBuilder.create();
    builder.appendList().delimitedBy(",").of(tableIds);
    return builder.toString();
  }
}
