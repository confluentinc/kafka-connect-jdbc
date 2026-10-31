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

import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.connect.connector.ConnectorContext;
import org.apache.kafka.connect.errors.ConnectException;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;
import org.mockito.stubbing.Answer;
import org.mockito.stubbing.OngoingStubbing;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import io.confluent.connect.jdbc.dialect.DatabaseDialect;
import io.confluent.connect.jdbc.util.ConnectionProvider;
import io.confluent.connect.jdbc.util.ExpressionBuilder;
import io.confluent.connect.jdbc.util.TableId;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.atLeast;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class TableMonitorThreadTest {
  private static final long STARTUP_LIMIT = 50;
  private static final long POLL_INTERVAL = 100;

  private final static TableId FOO = new TableId(null, null, "foo");
  private final static TableId BAR = new TableId(null, null, "bar");
  private final static TableId BAZ = new TableId(null, null, "baz");

  private final static TableId DUP1 = new TableId(null, "dup1", "dup");
  private final static TableId DUP2 = new TableId(null, "dup2", "dup");

  private static final List<TableId> LIST_EMPTY = Collections.emptyList();
  private static final List<TableId> LIST_FOO = Collections.singletonList(FOO);
  private static final List<TableId> LIST_FOO_BAR = Arrays.asList(FOO, BAR);
  private static final List<TableId> LIST_FOO_BAR_BAZ = Arrays.asList(FOO, BAR, BAZ);
  private static final List<TableId> LIST_DUP_ONLY = Arrays.asList(DUP1, DUP2);
  private static final List<TableId> LIST_DUP_WITH_ALL = Arrays.asList(DUP1, FOO, DUP2, BAR, BAZ);

  private static final List<String> FIRST_TOPIC_LIST = Arrays.asList("foo");
  private static final List<String> VIEW_TOPIC_LIST = Arrays.asList("");
  private static final List<String> SECOND_TOPIC_LIST = Arrays.asList("foo", "bar");
  private static final List<String> THIRD_TOPIC_LIST = Arrays.asList("foo", "bar", "baz");
  public static final Set<String> DEFAULT_TABLE_TYPES = Collections.unmodifiableSet(
          new HashSet<>(Arrays.asList("TABLE"))
  );
  public static final Set<String> VIEW_TABLE_TYPES = Collections.unmodifiableSet(
          new HashSet<>(Arrays.asList("VIEW"))
  );
  private TableMonitorThread tableMonitorThread;

  @Mock private ConnectionProvider connectionProvider;
  @Mock private Connection connection;
  @Mock private DatabaseDialect dialect;
  @Mock private ConnectorContext context;
  @Mock private Time time;

  private OngoingStubbing<List<TableId>> tableIdsStubbing;

  @Test
  public void testSingleLookup() throws Exception {
    when(dialect.expressionBuilder()).thenReturn(ExpressionBuilder.create());
    tableMonitorThread = new TableMonitorThread(dialect, connectionProvider, context,
        STARTUP_LIMIT, POLL_INTERVAL, null, null, MockTime.SYSTEM);
    expectTableNames(LIST_FOO, shutdownThread());

    tableMonitorThread.start();
    tableMonitorThread.join();
    checkTableNames("foo").execute();

    verify(connectionProvider).getConnection();
    verify(dialect).tableIds(connection);
  }

  @Test
  public void testTablesBlockingTimeoutOnUpdateThread() throws Exception {
    // lenient(): this test never gets far enough to build a query, so the stub goes unused -
    // the original EasyMock version tolerated that too, via .anyTimes() (min 0 calls).
    lenient().when(dialect.expressionBuilder()).thenReturn(ExpressionBuilder.create());
    tableMonitorThread = new TableMonitorThread(dialect, connectionProvider, context,
        STARTUP_LIMIT, 0, null, null, time);

    CountDownLatch connectionRequested = new CountDownLatch(1);
    CountDownLatch connectionCompleted = new CountDownLatch(1);
    // Only the first update cycle is expected; unlike EasyMock (which threw on unrecorded
    // calls), Mockito repeats the last thenReturn() forever, which would leave the background
    // thread's poll loop (pollMs=0 here, i.e. no throttling at all) spinning as fast as
    // possible instead of dying and letting join() return.
    when(dialect.tableIds(eq(connection)))
        .thenReturn(Collections.emptyList())
        .thenThrow(new RuntimeException("no further table updates expected in this test"));
    when(connectionProvider.getConnection()).thenAnswer(invocation -> {
      connectionRequested.countDown();
      connectionCompleted.await();
      return connection;
    });

    when(time.milliseconds()).thenReturn(0L);
    doThrow(new TimeoutException())
        .when(time).waitObject(any(), any(), eq(STARTUP_LIMIT));

    // Haven't had a chance to start the first table read; should return null to signify that no
    // attempt to list tables on the database has succeeded yet
    assertNull(
        "Should not have even started any table reads yet",
        tableMonitorThread.tables()
    );
    tableMonitorThread.start();

    assertTrue(
        "Should have attempted to establish database connection by now",
        connectionRequested.await(10, TimeUnit.SECONDS)
    );
    // Have initiated a table read, but haven't been able to connect to the database yet;
    // should still return
    assertNull(
        "Should not have completed any table reads yet",
        tableMonitorThread.tables()
    );

    connectionCompleted.countDown();
    tableMonitorThread.join();
    // Have completed a table read; should return an empty list (instead of null) to signify that
    // we've been able to read the tables from the database, but just can't find any to query
    assertEquals(Collections.emptyList(), tableMonitorThread.tables());
  }

  @Test
  public void testTablesBlockingWithDeadlineOnUpdateThread() throws Exception {
    // lenient(): this test never gets far enough to build a query, so the stub goes unused -
    // the original EasyMock version tolerated that too, via .anyTimes() (min 0 calls).
    lenient().when(dialect.expressionBuilder()).thenReturn(ExpressionBuilder.create());
    tableMonitorThread = new TableMonitorThread(dialect, connectionProvider, context,
        STARTUP_LIMIT, POLL_INTERVAL, null, null, time);

    // Only the first update cycle is expected; unlike EasyMock (which threw on unrecorded
    // calls), Mockito repeats the last thenReturn() forever, which would leave the background
    // thread's poll loop spinning indefinitely instead of dying and letting join() return.
    when(dialect.tableIds(eq(connection)))
        .thenReturn(Collections.emptyList())
        .thenThrow(new RuntimeException("no further table updates expected in this test"));
    when(connectionProvider.getConnection()).thenReturn(connection);

    long currentTime = System.currentTimeMillis();
    when(time.milliseconds()).thenReturn(currentTime);

    tableMonitorThread.start();
    tableMonitorThread.join();

    assertEquals(Collections.emptyList(), tableMonitorThread.tables());

    verify(time).milliseconds();
    verify(time).waitObject(any(), any(), eq(currentTime + STARTUP_LIMIT));
  }

  @Test
  public void testWhitelist() throws Exception {
    Set<String> whitelist = new HashSet<>(Arrays.asList("foo", "bar"));
    when(dialect.expressionBuilder()).thenReturn(ExpressionBuilder.create());
    tableMonitorThread = new TableMonitorThread(dialect, connectionProvider, context,
        STARTUP_LIMIT, POLL_INTERVAL, whitelist, null, MockTime.SYSTEM);
    expectTableNames(LIST_FOO_BAR, shutdownThread());

    tableMonitorThread.start();
    tableMonitorThread.join();
    checkTableNames("foo", "bar").execute();

    verify(connectionProvider).getConnection();
    verify(dialect).tableIds(connection);
  }

  @Test
  public void testBlacklist() throws Exception {
    Set<String> blacklist = new HashSet<>(Arrays.asList("bar", "baz"));
    when(dialect.expressionBuilder()).thenReturn(ExpressionBuilder.create());
    tableMonitorThread = new TableMonitorThread(dialect, connectionProvider, context,
        STARTUP_LIMIT, POLL_INTERVAL, null, blacklist, MockTime.SYSTEM);
    expectTableNames(LIST_FOO_BAR_BAZ, shutdownThread());

    tableMonitorThread.start();
    tableMonitorThread.join();
    checkTableNames("foo").execute();

    verify(connectionProvider).getConnection();
    verify(dialect).tableIds(connection);
  }

  @Test
  public void testReconfigOnUpdate() throws Exception {
    when(dialect.expressionBuilder()).thenReturn(ExpressionBuilder.create());
    tableMonitorThread = new TableMonitorThread(dialect, connectionProvider, context,
        STARTUP_LIMIT, POLL_INTERVAL, null, null, MockTime.SYSTEM);
    expectTableNames(LIST_FOO);
    expectTableNames(LIST_FOO, checkTableNames("foo"));

    // Change the result to trigger a task reconfiguration
    expectTableNames(LIST_FOO_BAR);

    // Changing again should result in another task reconfiguration
    expectTableNames(LIST_FOO, checkTableNames("foo", "bar"), shutdownThread());

    tableMonitorThread.start();
    tableMonitorThread.join();
    checkTableNames("foo").execute();

    // The background thread polls on its own timer, so an extra poll can legitimately land
    // before shutdown() takes effect; only the reconfiguration count (tied to actual table
    // list changes) needs to be exact.
    verify(connectionProvider, atLeast(3)).getConnection();
    verify(dialect, atLeast(3)).tableIds(connection);
    verify(context, times(3)).requestTaskReconfiguration();
  }

  @Test
  public void testInvalidConnection() throws Exception {
    tableMonitorThread = new TableMonitorThread(dialect, connectionProvider, context,
        STARTUP_LIMIT, POLL_INTERVAL, null, null, MockTime.SYSTEM);
    when(connectionProvider.getConnection()).thenThrow(new ConnectException("Simulated error with the db."));

    CountDownLatch errorLatch = new CountDownLatch(1);
    doAnswer(invocation -> {
      errorLatch.countDown();
      return null;
    }).when(context).raiseError(any());

    tableMonitorThread.start();
    assertTrue("Connector should have failed by now", errorLatch.await(10, TimeUnit.SECONDS));
    tableMonitorThread.join();

    verify(connectionProvider).getConnection();
    verify(context).raiseError(any());
  }

  @Test
  public void testDuplicates() throws Exception {
    when(dialect.expressionBuilder()).thenReturn(ExpressionBuilder.create());
    tableMonitorThread = new TableMonitorThread(dialect, connectionProvider, context,
        STARTUP_LIMIT, POLL_INTERVAL, null, null, MockTime.SYSTEM);
    expectTableNames(LIST_DUP_WITH_ALL, shutdownThread());
    tableMonitorThread.start();
    tableMonitorThread.join();
    assertThrows(ConnectException.class, tableMonitorThread::tables);
    verify(connectionProvider).getConnection();
    verify(dialect).tableIds(connection);
    verify(context).requestTaskReconfiguration();
    verify(context).raiseError(any());
  }

  @Test
  public void testDuplicateWithUnqualifiedWhitelist() throws Exception {
    Set<String> whitelist = new HashSet<>(Arrays.asList("dup"));
    when(dialect.expressionBuilder()).thenReturn(ExpressionBuilder.create());
    tableMonitorThread = new TableMonitorThread(dialect, connectionProvider, context,
        STARTUP_LIMIT, POLL_INTERVAL, whitelist, null, MockTime.SYSTEM);
    expectTableNames(LIST_DUP_ONLY, shutdownThread());

    tableMonitorThread.start();
    tableMonitorThread.join();
    assertThrows(ConnectException.class, tableMonitorThread::tables);
    verify(connectionProvider).getConnection();
    verify(dialect).tableIds(connection);
    verify(context).requestTaskReconfiguration();
    verify(context).raiseError(any());
  }

  @Test
  public void testDuplicateWithUnqualifiedBlacklist() throws Exception {
    Set<String> blacklist = new HashSet<>(Arrays.asList("foo"));
    when(dialect.expressionBuilder()).thenReturn(ExpressionBuilder.create());
    tableMonitorThread = new TableMonitorThread(dialect, connectionProvider, context,
        STARTUP_LIMIT, POLL_INTERVAL, null, blacklist, MockTime.SYSTEM);
    expectTableNames(LIST_DUP_WITH_ALL, shutdownThread());

    tableMonitorThread.start();
    tableMonitorThread.join();
    assertThrows(ConnectException.class, tableMonitorThread::tables);
    verify(connectionProvider).getConnection();
    verify(dialect).tableIds(connection);
    verify(context).requestTaskReconfiguration();
    verify(context).raiseError(any());
  }

  @Test
  public void testDuplicateWithQualifiedWhitelist() throws Exception {
    Set<String> whitelist = new HashSet<>(Arrays.asList("dup1.dup", "foo"));
    when(dialect.expressionBuilder()).thenReturn(ExpressionBuilder.create());
    tableMonitorThread = new TableMonitorThread(dialect, connectionProvider, context,
        STARTUP_LIMIT, POLL_INTERVAL, whitelist, null, MockTime.SYSTEM);
    expectTableNames(LIST_DUP_WITH_ALL, shutdownThread());

    tableMonitorThread.start();
    tableMonitorThread.join();
    checkTableIds(DUP1, FOO);
    verify(connectionProvider).getConnection();
    verify(dialect).tableIds(connection);
  }

  @Test
  public void testDuplicateWithQualifiedBlacklist() throws Exception {
    Set<String> blacklist = new HashSet<>(Arrays.asList("dup1.dup", "foo"));
    when(dialect.expressionBuilder()).thenReturn(ExpressionBuilder.create());
    tableMonitorThread = new TableMonitorThread(dialect, connectionProvider, context,
        STARTUP_LIMIT, POLL_INTERVAL, null, blacklist, MockTime.SYSTEM);
    expectTableNames(LIST_DUP_WITH_ALL, shutdownThread());

    tableMonitorThread.start();
    tableMonitorThread.join();
    checkTableIds(DUP2, BAR, BAZ);
    verify(connectionProvider).getConnection();
    verify(dialect).tableIds(connection);
  }

  private interface Op {
    void execute();
  }

  protected Op shutdownThread() {
    return new Op() {
      @Override
      public void execute() {
        tableMonitorThread.shutdown();
      }
    };
  }

  protected Op checkTableNames(final String...expectedTableNames) {
    return new Op() {
      @Override
      public void execute() {
        List<TableId> expectedTableIds = new ArrayList<>();
        for (String expectedTableName: expectedTableNames) {
          TableId id = new TableId(null, null, expectedTableName);
          expectedTableIds.add(id);
        }
        assertEquals(expectedTableIds, tableMonitorThread.tables());
      }
    };
  }

  protected void checkTableIds(final TableId...expectedTables) {
    assertEquals(Arrays.asList(expectedTables), tableMonitorThread.tables());
  }

  protected void expectTableNames(final List<TableId> expectedTableIds, final Op...operations) throws SQLException {
    when(connectionProvider.getConnection()).thenReturn(connection);
    Answer<List<TableId>> answer = invocation -> {
      if (operations != null) {
        for (Op op : operations) {
          op.execute();
        }
      }
      return expectedTableIds;
    };
    // dialect.tableIds(...) may be stubbed multiple times within the same test (e.g. to
    // simulate successive polls returning different table lists), so the answers must be
    // chained onto the same stubbing rather than each call replacing the previous one.
    if (tableIdsStubbing == null) {
      tableIdsStubbing = when(dialect.tableIds(eq(connection))).thenAnswer(answer);
    } else {
      tableIdsStubbing = tableIdsStubbing.thenAnswer(answer);
    }
  }
}
