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

package io.confluent.connect.jdbc.source;

import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.easymock.EasyMock;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.api.easymock.PowerMock;
import org.powermock.api.easymock.annotation.Mock;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.modules.junit4.PowerMockRunner;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.SQLNonTransientException;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;

import io.confluent.connect.jdbc.util.CachedConnectionProvider;

import static org.easymock.EasyMock.anyBoolean;
import static org.easymock.EasyMock.anyLong;
import static org.easymock.EasyMock.anyObject;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.expectLastCall;
import static org.easymock.EasyMock.replay;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.IsInstanceOf.instanceOf;
import static org.hamcrest.core.StringContains.containsString;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;


@RunWith(PowerMockRunner.class)
@PowerMockIgnore("javax.management.*")
public class JdbcSourceTaskLifecycleTest extends JdbcSourceTaskTestBase {

  @Mock
  private CachedConnectionProvider mockCachedConnectionProvider;

  @Mock
  private Connection conn;

  @Test(expected = ConnectException.class)
  public void testMissingParentConfig() {
    Map<String, String> props = singleTableConfig();
    props.remove(JdbcSourceConnectorConfig.CONNECTION_URL_CONFIG);
    task.start(props);
  }

  @Test(expected = ConfigException.class)
  public void testMissingTables() {
    Map<String, String> props = singleTableConfig();
    props.remove(JdbcSourceTaskConfig.TABLES_CONFIG);
    task.start(props);
  }

  @Test
  public void testStartStopDifferentThreads() throws Exception {
    db.createTable(SINGLE_TABLE_NAME, "id", "INT");

    // Minimal start/stop functionality
    task = new JdbcSourceTask(time) {
      @Override
      protected CachedConnectionProvider connectionProvider(
          int maxConnAttempts,
          long retryBackoff
      ) {
        return mockCachedConnectionProvider;
      }
    };

    // Should request a connection, then should close it on stop()
    EasyMock.expect(mockCachedConnectionProvider.getConnection()).andReturn(db.getConnection()).anyTimes();
    mockCachedConnectionProvider.close(true);

    PowerMock.expectLastCall();

    PowerMock.replayAll();

    ExecutorService executor = Executors.newSingleThreadExecutor();
    Object lock = new Object();
    AtomicBoolean running = new AtomicBoolean(true);

    executor.submit(() -> {
      task.start(singleTableConfig());
      while (running.get()) {
        task.poll();

        synchronized (lock) {
          lock.notifyAll();
        }
      }
      return null;
    });

    synchronized (lock) {
      lock.wait();
    }

    try {
      task.stop();
      synchronized (lock) {
          lock.wait();
      }
      running.set(false);
    } finally {
      executor.shutdown();
    }

    PowerMock.verifyAll();
  }

  @Test
  public void testStartStopSameThread() {
    // Minimal start/stop functionality
    task = new JdbcSourceTask(time) {
      @Override
      protected CachedConnectionProvider connectionProvider(
          int maxConnAttempts,
          long retryBackoff
      ) {
        return mockCachedConnectionProvider;
      }
    };

    // Should request a connection, then should close it on stop()
    EasyMock.expect(mockCachedConnectionProvider.getConnection()).andReturn(db.getConnection());
    EasyMock.expect(mockCachedConnectionProvider.getConnection()).andReturn(db.getConnection());
    EasyMock.expect(mockCachedConnectionProvider.getConnection()).andReturn(db.getConnection());
    mockCachedConnectionProvider.close(true);

    PowerMock.expectLastCall();

    PowerMock.replayAll();

    task.start(singleTableConfig());
    task.stop();

    PowerMock.verifyAll();
  }

  // Note: testPollInterval test was removed because the new RecordQueue-based architecture
  // makes precise timing control impossible. The TableQuerierProcessor runs asynchronously
  // in a background thread, so poll() timing is no longer directly controllable.
  // This matches the Snowflake implementation which also removed this test.


  @Test
  public void testSingleUpdateMultiplePoll() throws Exception {
    // Test that splitting up a table update query across multiple poll() calls works

    db.createTable(SINGLE_TABLE_NAME, "id", "INT");

    Map<String, String> taskConfig = singleTableConfig();
    taskConfig.put(JdbcSourceConnectorConfig.BATCH_MAX_ROWS_CONFIG, "1");
    db.insert(SINGLE_TABLE_NAME, "id", 1);
    db.insert(SINGLE_TABLE_NAME, "id", 2);
    task.start(taskConfig);

    Thread.sleep(100);

    // Two entries should get split across multiple poll() calls
    List<SourceRecord> records = task.poll();
    assertEquals(1, records.size());
    assertEquals(1, ((Struct)(records.get(0)).value()).get("id"));
    records = task.poll();
    assertEquals(1, records.size());
    assertEquals(2, ((Struct)(records.get(0)).value()).get("id"));

  }

  @Test
  public void testMultipleTables() throws Exception {
    db.createTable(SINGLE_TABLE_NAME, "id", "INT");
    db.createTable(SECOND_TABLE_NAME, "id", "INT");

    db.insert(SINGLE_TABLE_NAME, "id", 1);
    db.insert(SECOND_TABLE_NAME, "id", 2);

    Map<String, String> taskConfigs = twoTableConfig();
    taskConfigs.put(JdbcSourceConnectorConfig.BATCH_MAX_ROWS_CONFIG, "1");
    task.start(taskConfigs);

    Thread.sleep(100);

    // Both tables should be polled, in order
    List<SourceRecord> records = task.poll();
    assertEquals(1, records.size());
    assertEquals(SINGLE_TABLE_PARTITION, records.get(0).sourcePartition());
    records = task.poll();
    assertEquals(1, records.size());
    assertEquals(SECOND_TABLE_PARTITION, records.get(0).sourcePartition());

    validatePollResultTable(records, 1, SECOND_TABLE_NAME);

  }

  @Test
  public void testMultipleTablesMultiplePolls() throws Exception {
    // Check correct handling of multiple tables when the tables require multiple poll() calls to
    // return one query's data

    db.createTable(SINGLE_TABLE_NAME, "id", "INT");
    db.createTable(SECOND_TABLE_NAME, "id", "INT");

    Map<String, String> taskConfig = twoTableConfig();
    taskConfig.put(JdbcSourceConnectorConfig.BATCH_MAX_ROWS_CONFIG, "1");

    db.insert(SINGLE_TABLE_NAME, "id", 1);
    db.insert(SINGLE_TABLE_NAME, "id", 2);
    db.insert(SECOND_TABLE_NAME, "id", 3);
    db.insert(SECOND_TABLE_NAME, "id", 4);

    task.start(taskConfig);

    // wait for the records to be available in the table
    Thread.sleep(100);

    // Both tables should be polled, in order
    for(int i = 0; i < 2; i++) {
      List<SourceRecord> records = task.poll();
      validatePollResultTable(records, 1, SINGLE_TABLE_NAME);
    }
    for(int i = 0; i < 2; i++) {
      List<SourceRecord> records = task.poll();
      validatePollResultTable(records, 1, SECOND_TABLE_NAME);
    }

    // wait to allow the table querier to query the tables again and fill in the internal queue
    Thread.sleep(100);

    // Subsequent poll should continue processing
    for(int i = 0; i < 2; i++) {
      List<SourceRecord> records = task.poll();
      validatePollResultTable(records, 1, SINGLE_TABLE_NAME);
    }
    for(int i = 0; i < 2; i++) {
      List<SourceRecord> records = task.poll();
      validatePollResultTable(records, 1, SECOND_TABLE_NAME);
    }
  }

  @Test
  public void testMultipleTablesNothingToDoReturns() throws Exception {
    db.createTable(SINGLE_TABLE_NAME, "id", "INT");
    db.createTable(SECOND_TABLE_NAME, "id", "INT");

    task.start(twoTableConfig());

    assertTrue(task.poll().isEmpty());
  }

  @Test
  public void testNonTransientSQLExceptionThrows() throws Exception {
    db.createTable(SINGLE_TABLE_NAME, "id", "INT");

    Map<String, String> config = singleTableConfig();
    config.put(JdbcSourceTaskConfig.TABLES_CONFIG, "not_existing_table");
    task.start(config);

    Thread.sleep(100);

    ConnectException e = assertThrows(ConnectException.class, () -> {
      task.poll();
    });
    assertThat(e.getCause(), instanceOf(ConnectException.class));
    assertThat(e.getCause().getCause(), instanceOf(SQLNonTransientException.class));
    assertThat(e.getCause().getMessage(), containsString("not_existing_table"));
  }

  @Test(expected = ConnectException.class)
  public void testTransientSQLExceptionRetries() throws Exception {

    int retryMax = 2; //max times to retry
    TableQuerier bulkTableQuerier = EasyMock.createMock(BulkTableQuerier.class);

    for (int i = 0; i < retryMax+1; i++) {
      expect(bulkTableQuerier.querying()).andReturn(true);
      bulkTableQuerier.maybeStartQuery(anyObject());
      expectLastCall().andThrow(new SQLException("This is a transient exception"));

      expect(bulkTableQuerier.getAttemptedRetryCount()).andReturn(i);
      // Called another time in error logging
      expect(bulkTableQuerier.getAttemptedRetryCount()).andReturn(i);
      bulkTableQuerier.incrementRetryCount();
      expectLastCall().once();
      bulkTableQuerier.reset(anyLong(), anyBoolean());
    }

    replay(bulkTableQuerier);
    JdbcSourceTask mockedTask = setUpMockedTask(bulkTableQuerier, retryMax);

    for (int i = 0; i < retryMax+1; i++) {
      mockedTask.poll();
    }
  }


  private JdbcSourceTask setUpMockedTask(TableQuerier bulkTableQuerier, int retryMax) throws Exception {
    CachedConnectionProvider mockCachedConnectionProvider = EasyMock.createMock(CachedConnectionProvider.class);
    for (int i = 0; i < retryMax+1; i++) {
      expect(mockCachedConnectionProvider.getConnection()).andReturn(null);
    }
    replay(mockCachedConnectionProvider);

    PriorityQueue<TableQuerier> priorityQueue = new PriorityQueue<>();
    priorityQueue.add(bulkTableQuerier);


    JdbcSourceTask mockedTask = new JdbcSourceTask(time);
    mockedTask.start(singleTableConfig());

    mockedTask.tableQueue = priorityQueue;
    mockedTask.cachedConnectionProvider = mockCachedConnectionProvider;

    return mockedTask;
  }

  private static void validatePollResultTable(List<SourceRecord> records,
                                              int expected, String table) {
    assertEquals(expected, records.size());
    for (SourceRecord record : records) {
      assertEquals(table, record.sourcePartition().get(JdbcSourceConnectorConstants.TABLE_NAME_KEY));
    }
  }
}
