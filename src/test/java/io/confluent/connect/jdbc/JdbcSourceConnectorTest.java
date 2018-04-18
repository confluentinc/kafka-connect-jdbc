/**
 * Copyright 2015 Confluent Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 **/

package io.confluent.connect.jdbc;

import org.apache.kafka.connect.errors.ConnectException;
import org.easymock.EasyMock;
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
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import io.confluent.connect.jdbc.source.EmbeddedDerby;
import io.confluent.connect.jdbc.source.JdbcSourceConnectorConfig;
import io.confluent.connect.jdbc.source.JdbcSourceTask;
import io.confluent.connect.jdbc.source.JdbcSourceTaskConfig;
import io.confluent.connect.jdbc.util.CachedConnectionProvider;
import io.confluent.connect.jdbc.util.JdbcUtils;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

@RunWith(PowerMockRunner.class)
@PrepareForTest({JdbcSourceConnector.class, JdbcUtils.class})
@PowerMockIgnore("javax.management.*")
public class JdbcSourceConnectorTest {

  private JdbcSourceConnector connector;
  private EmbeddedDerby db;
  private Map<String, String> connProps;

  @Before
  public void setup() {
    connector = new JdbcSourceConnector();
    db = new EmbeddedDerby();
    connProps = new HashMap<>();
    connProps.put(JdbcSourceConnectorConfig.CONNECTION_URL_CONFIG, db.getUrl());
    connProps.put(JdbcSourceConnectorConfig.MODE_CONFIG, JdbcSourceConnectorConfig.MODE_BULK);
    connProps.put(JdbcSourceConnectorConfig.TOPIC_PREFIX_CONFIG, "test-");
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
    CachedConnectionProvider mockCachedConnectionProvider = PowerMock.createMock(CachedConnectionProvider.class);
    PowerMock.expectNew(CachedConnectionProvider.class, db.getUrl(), null, null,
      JdbcSourceConnectorConfig.CONNECTION_ATTEMPTS_DEFAULT, JdbcSourceConnectorConfig.CONNECTION_BACKOFF_DEFAULT).andReturn(mockCachedConnectionProvider);

    // Should request a connection, then should close it on stop(). The background thread may also
    // request connections any time it performs updates.
    Connection conn = PowerMock.createMock(Connection.class);
    EasyMock.expect(mockCachedConnectionProvider.getValidConnection()).andReturn(conn).anyTimes();

    // Since we're just testing start/stop, we don't worry about the value here but need to stub
    // something since the background thread will be started and try to lookup metadata.
    EasyMock.expect(conn.getMetaData()).andStubThrow(new SQLException());
    // Close will be invoked both for the SQLExeption and when the connector is stopped
    mockCachedConnectionProvider.closeQuietly();
    PowerMock.expectLastCall().times(2);

    PowerMock.replayAll();

    connector.start(connProps);
    connector.stop();

    PowerMock.verifyAll();
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
    assertEquals("test", configs.get(0).get(JdbcSourceTaskConfig.TABLES_CONFIG));
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

    assertEquals("test1,test2", configs.get(0).get(JdbcSourceTaskConfig.TABLES_CONFIG));
    assertNull(configs.get(0).get(JdbcSourceTaskConfig.QUERY_CONFIG));
    assertEquals("test3", configs.get(1).get(JdbcSourceTaskConfig.TABLES_CONFIG));
    assertNull(configs.get(1).get(JdbcSourceTaskConfig.QUERY_CONFIG));
    assertEquals("test4", configs.get(2).get(JdbcSourceTaskConfig.TABLES_CONFIG));
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

  @Test
  public void testPartitioningInlineViews() throws Exception {
    // Tests "partitioning" when config specifies inline views
    // Commas in inline view definitions must be escaped with \\\\ (4 anti-slashes)
    // actually inline views are not checked against the schema so table creation here is not needed
    // for the partitioning.
    db.createTable("test1", "id", "INT NOT NULL", "val1", "VARCHAR(20)");
    db.createTable("test2", "id", "INT NOT NULL", "val2", "VARCHAR(20)");
    db.createTable("test3", "id", "INT NOT NULL", "val3", "VARCHAR(20)");
    final String sampleWhiteList= "view_test1, view_test2";
    final String sampleInlineViewsDefinitions = "(SELECT id\\\\, val1\\\\, val2 FROM test1 JOIN test2 on test1.id = test2.id),"
        + "(SELECT id\\\\\\\\, val2\\\\\\\\, val3 FROM test2 JOIN test3 on test2.id = test3.id)";
    connProps.put(JdbcSourceConnectorConfig.TABLE_WHITELIST_CONFIG, sampleWhiteList);
    connProps.put(JdbcSourceConnectorConfig.INLINE_VIEWS_DEFINITIONS_CONFIG, sampleInlineViewsDefinitions);
    connector.start(connProps);
    List<Map<String, String>> configs = connector.taskConfigs(3);
    assertEquals(2, configs.size());
    assertTaskConfigsHaveParentConfigs(configs);

    List<String> views = Arrays.asList(configs.get(0).get(JdbcSourceTaskConfig.TABLES_CONFIG),
        configs.get(1).get(JdbcSourceTaskConfig.TABLES_CONFIG));
    Collections.sort(views);
    assertEquals(Arrays.asList(sampleWhiteList).toString(), views.toString());
    assertNull(configs.get(0).get(JdbcSourceTaskConfig.QUERY_CONFIG));
    assertNull(configs.get(1).get(JdbcSourceTaskConfig.QUERY_CONFIG));

    connector.stop();
  }
  
  @Test
  public void testPartitioningInlineViewsAndTables() throws Exception {
    // Tests "partitioning" when config specifies inline views mixed with tables
    // Commas in inline view definitions must be escaped with \\\\
    db.createTable("test1", "id", "INT NOT NULL", "val1", "VARCHAR(20)");
    db.createTable("test2", "id", "INT NOT NULL", "val2", "VARCHAR(20)");
    db.createTable("test3", "id", "INT NOT NULL", "val3", "VARCHAR(20)");
    final String sampleWhiteList= "test1, view_test1, test2, view_test2, test3";
    final String sampleInlineViewsDefinitions = "(SELECT id\\\\, val1\\\\, val2 FROM test1 JOIN test2 on test1.id = test2.id),"
        + "(SELECT id\\\\\\\\, val2\\\\\\\\, val3 FROM test2 JOIN test3 on test2.id = test3.id)";
    connProps.put(JdbcSourceConnectorConfig.TABLE_WHITELIST_CONFIG, sampleWhiteList);
    connProps.put(JdbcSourceConnectorConfig.INLINE_VIEWS_DEFINITIONS_CONFIG, sampleInlineViewsDefinitions);
    connector.start(connProps);
    List<Map<String, String>> configs = connector.taskConfigs(5);
    assertEquals(5, configs.size());
    assertTaskConfigsHaveParentConfigs(configs);

    List<String> tableAndViews = Arrays.asList(configs.get(0).get(JdbcSourceTaskConfig.TABLES_CONFIG),
        configs.get(1).get(JdbcSourceTaskConfig.TABLES_CONFIG),
        configs.get(2).get(JdbcSourceTaskConfig.TABLES_CONFIG),
        configs.get(3).get(JdbcSourceTaskConfig.TABLES_CONFIG),
        configs.get(4).get(JdbcSourceTaskConfig.TABLES_CONFIG));
    Collections.sort(tableAndViews);
    assertEquals(Arrays.asList("test1, test2, test3, view_test1, view_test2").toString(),
        tableAndViews.toString());

    connector.stop();
  }
  
  @Test(expected = ConnectException.class)
  public void testInlineViewsDefinitionsAreEnclosedInParentheses() throws Exception {
    // Inline views definitions must be enclosed in parentheses
    db.createTable("test1", "id", "INT NOT NULL", "val1", "VARCHAR(20)");
    db.createTable("test2", "id", "INT NOT NULL", "val2", "VARCHAR(20)");
    db.createTable("test3", "id", "INT NOT NULL", "val3", "VARCHAR(20)");
    final String sampleWhiteList= "view_test1, view_test2";
    final String sampleInlineViewsDefinitions = "(SELECT id\\\\, val1\\\\, val2 FROM test1 JOIN test2 on test1.id = test2.id),"
        + "SELECT id\\\\\\\\, val1\\\\\\\\, val2 FROM test2 JOIN test3 on test2.id = test3.id";
    connProps.put(JdbcSourceConnectorConfig.TABLE_WHITELIST_CONFIG, sampleWhiteList);
    connProps.put(JdbcSourceConnectorConfig.INLINE_VIEWS_DEFINITIONS_CONFIG, sampleInlineViewsDefinitions);
    connector.start(connProps);
  }
  
  @Test(expected = ConnectException.class)
  public void testNoInlineViewDefinitions() throws Exception {
    // Inline views definitions must not be empty when views are declared in table.whitelist
    db.createTable("test1", "id", "INT NOT NULL", "val1", "VARCHAR(20)");
    db.createTable("test2", "id", "INT NOT NULL", "val2", "VARCHAR(20)");
    db.createTable("test3", "id", "INT NOT NULL", "val3", "VARCHAR(20)");
    final String sampleWhiteList= "view_test1, view_test2";
    final String sampleInlineViewsDefinitions = "";
    connProps.put(JdbcSourceConnectorConfig.TABLE_WHITELIST_CONFIG, sampleWhiteList);
    connProps.put(JdbcSourceConnectorConfig.INLINE_VIEWS_DEFINITIONS_CONFIG, sampleInlineViewsDefinitions);
    connector.start(connProps);
    connector.stop();
  }
  
  @Test(expected = ConnectException.class)
  public void testMissingSomeInlineViewDefinitions() throws Exception {
    // The number of inline views definitions must be the same than views declared in table.whitelist
    db.createTable("test1", "id", "INT NOT NULL", "val1", "VARCHAR(20)");
    db.createTable("test2", "id", "INT NOT NULL", "val2", "VARCHAR(20)");
    db.createTable("test3", "id", "INT NOT NULL", "val3", "VARCHAR(20)");
    final String sampleWhiteList= "view_test1, view_test2";
    final String sampleInlineViewsDefinitions = "(SELECT id\\\\\\\\, val1\\\\\\\\, val2 FROM test1 JOIN test2 on test1.id = test2.id)";
    connProps.put(JdbcSourceConnectorConfig.TABLE_WHITELIST_CONFIG, sampleWhiteList);
    connProps.put(JdbcSourceConnectorConfig.INLINE_VIEWS_DEFINITIONS_CONFIG, sampleInlineViewsDefinitions);
    connector.start(connProps);
    connector.stop();
  }
  
  @Test
  public void testCustomInlineViewDefinitionTag() throws Exception {
    db.createTable("test1", "id", "INT NOT NULL", "val1", "VARCHAR(20)");
    db.createTable("test2", "id", "INT NOT NULL", "val2", "VARCHAR(20)");
    db.createTable("test3", "id", "INT NOT NULL", "val3", "VARCHAR(20)");
    final String sampleWhiteList= "V_test1, V_test2";
    final String sampleInlineViewsDefinitions = "(SELECT id\\\\, val1\\\\, val2 FROM test1 JOIN test2 on test1.id = test2.id),"
        + "(SELECT id\\\\\\\\, val2\\\\\\\\, val3 FROM test2 JOIN test3 on test2.id = test3.id)";
    connProps.put(JdbcSourceConnectorConfig.TABLE_WHITELIST_CONFIG, sampleWhiteList);
    connProps.put(JdbcSourceConnectorConfig.INLINE_VIEWS_DEFINITIONS_CONFIG, sampleInlineViewsDefinitions);
    connProps.put(JdbcSourceConnectorConfig.INLINE_VIEW_TAG_CONFIG, "V_");
    connector.start(connProps);
    List<Map<String, String>> configs = connector.taskConfigs(3);
    assertEquals(2, configs.size());
    assertTaskConfigsHaveParentConfigs(configs);
    connector.stop();
  }
  
  @Test(expected = IllegalArgumentException.class)
  public void testMissingInlineViewDefinitionTagConfig() throws Exception {
    db.createTable("test1", "id", "INT NOT NULL", "val1", "VARCHAR(20)");
    db.createTable("test2", "id", "INT NOT NULL", "val2", "VARCHAR(20)");
    db.createTable("test3", "id", "INT NOT NULL", "val3", "VARCHAR(20)");
    final String sampleWhiteList= "V_test1, V_test2";
    final String sampleInlineViewsDefinitions = "(SELECT id\\\\, val1\\\\, val2 FROM test1 JOIN test2 on test1.id = test2.id),"
        + "(SELECT id\\\\\\\\, val2\\\\\\\\, val3 FROM test2 JOIN test3 on test2.id = test3.id)";
    connProps.put(JdbcSourceConnectorConfig.TABLE_WHITELIST_CONFIG, sampleWhiteList);
    connProps.put(JdbcSourceConnectorConfig.INLINE_VIEWS_DEFINITIONS_CONFIG, sampleInlineViewsDefinitions);
    connector.start(connProps);
    List<Map<String, String>> configs = connector.taskConfigs(3);
    connector.stop();
  }
  
  @Test(expected = ConnectException.class)
  public void testConflictingQueryTableSettings() {
    final String sample_query = "SELECT foo, bar FROM sample_table";
    connProps.put(JdbcSourceConnectorConfig.QUERY_CONFIG, sample_query);
    connProps.put(JdbcSourceConnectorConfig.TABLE_WHITELIST_CONFIG, "foo,bar");
    connector.start(connProps);
  }

  @Test
  public void testSchemaPatternUsedForConfigValidation() throws Exception {
    connProps.put(JdbcSourceConnectorConfig.SCHEMA_PATTERN_CONFIG, "SOME_SCHEMA");

    PowerMock.mockStatic(JdbcUtils.class);
    EasyMock.expect(JdbcUtils.getTables(EasyMock.anyObject(Connection.class), EasyMock.eq("SOME_SCHEMA"),
            EasyMock.eq(JdbcUtils.DEFAULT_TABLE_TYPES)))
      .andReturn(new ArrayList<String>())
      .atLeastOnce();

    PowerMock.replayAll();

    connector.validate(connProps);

    PowerMock.verifyAll();
  }

  private void assertTaskConfigsHaveParentConfigs(List<Map<String, String>> configs) {
    for (Map<String, String> config : configs) {
      assertEquals(this.db.getUrl(),
                   config.get(JdbcSourceConnectorConfig.CONNECTION_URL_CONFIG));
    }
  }
}
