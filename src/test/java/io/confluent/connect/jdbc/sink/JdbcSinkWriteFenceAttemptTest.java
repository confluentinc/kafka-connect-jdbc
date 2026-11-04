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
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package io.confluent.connect.jdbc.sink;

import static org.easymock.EasyMock.anyObject;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.expectLastCall;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.confluent.connect.jdbc.dialect.DatabaseDialect;
import io.confluent.connect.jdbc.dialect.SqliteDatabaseDialect;
import io.confluent.connect.jdbc.util.CachedConnectionProvider;
import io.confluent.connect.jdbc.util.ConnectionProvider;
import io.confluent.connect.jdbc.util.TableId;
import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.CompletableFuture;
import java.util.function.LongSupplier;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.errors.RetriableException;
import org.apache.kafka.connect.sink.ErrantRecordReporter;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTaskContext;
import org.easymock.Capture;
import org.easymock.EasyMockSupport;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class JdbcSinkWriteFenceAttemptTest extends EasyMockSupport {

  private static final Schema KEY_SCHEMA = Schema.INT64_SCHEMA;
  private static final Schema VALUE_SCHEMA = SchemaBuilder.struct()
      .field("title", Schema.STRING_SCHEMA)
      .build();

  private final SqliteHelper sqliteHelper = new SqliteHelper(getClass().getSimpleName());
  private final SinkTaskContext ctx = createMock(SinkTaskContext.class);
  private final JdbcDbWriter mockWriter = createMock(JdbcDbWriter.class);
  private ManualNanoClock clock;

  @Before
  public void setUp() throws IOException, SQLException {
    sqliteHelper.setUp();
    clock = new ManualNanoClock();
  }

  @After
  public void tearDown() throws IOException, SQLException {
    sqliteHelper.tearDown();
  }

  @Test
  public void disabledFenceKeepsExistingRetryBehavior() throws SQLException {
    List<SinkRecord> records = Collections.singletonList(record(1L, "first", 0));
    SQLException failure = new SQLException("transient");

    mockWriter.write(records);
    expectLastCall().andThrow(failure);
    mockWriter.closeQuietly();
    expectLastCall();
    ctx.timeout(1);
    expectLastCall();
    expect(ctx.errantRecordReporter()).andReturn(null);
    replayAll();

    TestSinkTask task = new TestSinkTask(clock, mockWriter);
    task.initialize(ctx);
    Map<String, String> props = eligibleProps(0);
    props.put(JdbcSinkConfig.WRITE_FENCE_TIMEOUT_MS, "0");
    props.put(JdbcSinkConfig.MAX_RETRIES, "1");
    props.put(JdbcSinkConfig.RETRY_BACKOFF_MS, "1");
    task.start(props);

    assertThrows(RetriableException.class, () -> task.put(records));
    verifyAll();
  }

  @Test
  public void failedWriteAtExactFenceBoundaryFailsTerminallyInsteadOfRetrying()
      throws SQLException {
    List<SinkRecord> records = Collections.singletonList(record(1L, "first", 0));
    SQLException failure = new SQLException("lost ack");

    mockWriter.write(anyObject(), anyObject());
    expectLastCall().andAnswer(() -> {
      clock.advanceMillis(10);
      throw failure;
    });
    mockWriter.closeQuietly();
    expectLastCall();
    expect(ctx.errantRecordReporter()).andReturn(null);
    replayAll();

    TestSinkTask task = new TestSinkTask(clock, mockWriter);
    task.initialize(ctx);
    Map<String, String> props = eligibleProps(10);
    props.put(JdbcSinkConfig.MAX_RETRIES, "1");
    task.start(props);

    ConnectException thrown = assertThrows(ConnectException.class, () -> task.put(records));
    assertTrue(thrown instanceof WriteFenceTimeoutException);
    assertFalse(thrown instanceof RetriableException);

    assertSame(thrown, assertThrows(ConnectException.class, () -> task.put(records)));
    verifyAll();
  }

  @Test
  public void failedWriteOverFenceBoundaryFailsTerminallyInsteadOfRetrying()
      throws SQLException {
    List<SinkRecord> records = Collections.singletonList(record(1L, "first", 0));

    mockWriter.write(anyObject(), anyObject());
    expectLastCall().andAnswer(() -> {
      clock.advanceMillis(11);
      throw new SQLException("overlong");
    });
    mockWriter.closeQuietly();
    expectLastCall();
    expect(ctx.errantRecordReporter()).andReturn(null);
    replayAll();

    TestSinkTask task = new TestSinkTask(clock, mockWriter);
    task.initialize(ctx);
    Map<String, String> props = eligibleProps(10);
    props.put(JdbcSinkConfig.MAX_RETRIES, "1");
    task.start(props);

    assertTrue(assertThrows(ConnectException.class, () -> task.put(records))
        instanceof WriteFenceTimeoutException);
    verifyAll();
  }

  @Test
  public void repeatedUnderBudgetRetryAttemptsDoNotAccumulateAcrossPuts()
      throws SQLException {
    List<SinkRecord> records = Collections.singletonList(record(1L, "first", 0));

    mockWriter.write(anyObject(), anyObject());
    expectLastCall().andAnswer(() -> {
      clock.advanceMillis(9);
      throw new SQLException("transient-1");
    });
    mockWriter.closeQuietly();
    expectLastCall();
    mockWriter.write(anyObject(), anyObject());
    expectLastCall().andAnswer(() -> {
      clock.advanceMillis(9);
      throw new SQLException("transient-2");
    });
    mockWriter.closeQuietly();
    expectLastCall();
    ctx.timeout(1);
    expectLastCall().times(2);
    expect(ctx.errantRecordReporter()).andReturn(null);
    replayAll();

    TestSinkTask task = new TestSinkTask(clock, mockWriter);
    task.initialize(ctx);
    Map<String, String> props = eligibleProps(10);
    props.put(JdbcSinkConfig.MAX_RETRIES, "2");
    props.put(JdbcSinkConfig.RETRY_BACKOFF_MS, "1");
    task.start(props);

    assertThrows(RetriableException.class, () -> task.put(records));
    assertThrows(RetriableException.class, () -> task.put(records));
    verifyAll();
  }

  @Test
  public void cleanupTimeIsCountedBeforeRetryBoundary() throws SQLException {
    List<SinkRecord> records = Collections.singletonList(record(1L, "first", 0));

    mockWriter.write(anyObject(), anyObject());
    expectLastCall().andAnswer(() -> {
      clock.advanceMillis(9);
      throw new SQLException("almost stale");
    });
    mockWriter.closeQuietly();
    expectLastCall().andAnswer(() -> {
      clock.advanceMillis(1);
      return null;
    });
    mockWriter.closeQuietly();
    expectLastCall();
    expect(ctx.errantRecordReporter()).andReturn(null);
    replayAll();

    TestSinkTask task = new TestSinkTask(clock, mockWriter);
    task.initialize(ctx);
    Map<String, String> props = eligibleProps(10);
    props.put(JdbcSinkConfig.MAX_RETRIES, "1");
    task.start(props);

    assertTrue(assertThrows(ConnectException.class, () -> task.put(records))
        instanceof WriteFenceTimeoutException);
    verifyAll();
  }

  @Test
  public void overlongConnectExceptionBecomesTerminalFence() throws SQLException {
    List<SinkRecord> records = Collections.singletonList(record(1L, "first", 0));

    mockWriter.write(anyObject(), anyObject());
    expectLastCall().andAnswer(() -> {
      clock.advanceMillis(10);
      throw new ConnectException("connection acquisition failed late");
    });
    mockWriter.closeQuietly();
    expectLastCall();
    expect(ctx.errantRecordReporter()).andReturn(null);
    replayAll();

    TestSinkTask task = new TestSinkTask(clock, mockWriter);
    task.initialize(ctx);
    task.start(eligibleProps(10));

    assertTrue(assertThrows(ConnectException.class, () -> task.put(records))
        instanceof WriteFenceTimeoutException);
    assertSame(assertThrows(ConnectException.class, () -> task.put(records)),
        assertThrows(ConnectException.class, () -> task.put(records)));
    verifyAll();
  }

  @Test
  public void unrollAndErrantRecordReportingCannotSwallowFence()
      throws SQLException {
    List<SinkRecord> records = new ArrayList<>();
    records.add(record(1L, "first", 0));
    records.add(record(2L, "second", 1));
    ErrantRecordReporter reporter = createMock(ErrantRecordReporter.class);

    mockWriter.write(anyObject(), anyObject());
    expectLastCall().andAnswer(() -> {
      clock.advanceMillis(5);
      throw new SQLException("batch failure");
    });
    mockWriter.closeQuietly();
    expectLastCall();
    mockWriter.write(anyObject(), anyObject());
    expectLastCall().andAnswer(() -> {
      clock.advanceMillis(5);
      throw new SQLException("single stale failure");
    });
    mockWriter.closeQuietly();
    expectLastCall();
    expect(ctx.errantRecordReporter()).andReturn(reporter);
    replayAll();

    TestSinkTask task = new TestSinkTask(clock, mockWriter);
    task.initialize(ctx);
    Map<String, String> props = eligibleProps(10);
    props.put(JdbcSinkConfig.MAX_RETRIES, "0");
    task.start(props);

    assertTrue(assertThrows(ConnectException.class, () -> task.put(records))
        instanceof WriteFenceTimeoutException);
    verifyAll();
  }

  @Test
  public void underBudgetUnrollReportsSqlFailuresNormally() throws SQLException {
    List<SinkRecord> records = new ArrayList<>();
    records.add(record(1L, "first", 0));
    records.add(record(2L, "second", 1));
    ErrantRecordReporter reporter = createMock(ErrantRecordReporter.class);
    Capture<Throwable> reported = Capture.newInstance();

    mockWriter.write(anyObject(), anyObject());
    expectLastCall().andAnswer(() -> {
      clock.advanceMillis(1);
      throw new SQLException("batch failure");
    });
    mockWriter.closeQuietly();
    expectLastCall();
    mockWriter.write(anyObject(), anyObject());
    expectLastCall().andAnswer(() -> {
      clock.advanceMillis(1);
      throw new SQLException("single failure");
    });
    expect(reporter.report(anyObject(), org.easymock.EasyMock.capture(reported)))
        .andReturn(CompletableFuture.completedFuture(null));
    mockWriter.closeQuietly();
    expectLastCall();
    mockWriter.write(anyObject(), anyObject());
    expectLastCall();
    expect(ctx.errantRecordReporter()).andReturn(reporter);
    replayAll();

    TestSinkTask task = new TestSinkTask(clock, mockWriter);
    task.initialize(ctx);
    Map<String, String> props = eligibleProps(10);
    props.put(JdbcSinkConfig.MAX_RETRIES, "0");
    task.start(props);

    task.put(records);

    assertTrue(reported.getValue() instanceof SQLException);
    verifyAll();
  }

  @Test
  public void successfulLateWriterReturnIsNotFailedByTask() throws SQLException {
    List<SinkRecord> records = Collections.singletonList(record(1L, "first", 0));

    mockWriter.write(anyObject(), anyObject());
    expectLastCall().andAnswer(() -> {
      clock.advanceMillis(100);
      return null;
    });
    expect(ctx.errantRecordReporter()).andReturn(null);
    replayAll();

    TestSinkTask task = new TestSinkTask(clock, mockWriter);
    task.initialize(ctx);
    task.start(eligibleProps(10));

    task.put(records);
    verifyAll();
  }

  @Test
  public void connectionAcquisitionTimeIsCountedByRealTaskWriterPath()
      throws Exception {
    sqliteHelper.createTable("CREATE TABLE books(id INTEGER PRIMARY KEY, title TEXT)");
    JdbcSinkTask task = new JdbcSinkTask(clock) {
      @Override
      void initWriter() {
        dialect = new SqliteDatabaseDialect(config);
        DbStructure dbStructure = new DbStructure(dialect);
        writer = new JdbcDbWriter(config, dialect, dbStructure) {
          @Override
          protected CachedConnectionProvider connectionProvider(
              int maxConnAttempts,
              long retryBackoff
          ) {
            return new CachedConnectionProvider(dialect, maxConnAttempts, retryBackoff) {
              @Override
              public synchronized Connection getConnection() {
                clock.advanceMillis(10);
                return sqliteHelper.connection;
              }
            };
          }
        };
      }
    };
    task.initialize(ctx);
    expect(ctx.errantRecordReporter()).andReturn(null);
    replayAll();
    task.start(eligibleProps(10));

    assertTrue(assertThrows(ConnectException.class,
        () -> task.put(Collections.singletonList(record(1L, "first", 0))))
        instanceof WriteFenceTimeoutException);
    assertEquals(0, countRows("books"));
    verifyAll();
  }

  @Test
  public void writerRollsBackAndInvalidatesConnectionWhenFenceExpiresBeforeCommit()
      throws SQLException {
    ManualNanoClock writerClock = new ManualNanoClock();
    Connection connection = org.mockito.Mockito.mock(Connection.class);
    PreparedStatement statement = org.mockito.Mockito.mock(PreparedStatement.class);
    SQLException rollbackFailure = new SQLException("rollback failed");
    SQLException closeFailure = new SQLException("close failed");
    doThrow(rollbackFailure).when(connection).rollback();
    doThrow(closeFailure).when(connection).close();

    Map<String, String> props = eligibleProps(10);
    JdbcSinkConfig config = new JdbcSinkConfig(props);
    DatabaseDialect dialect = org.mockito.Mockito.mock(DatabaseDialect.class);
    DbStructure dbStructure = org.mockito.Mockito.mock(DbStructure.class);
    JdbcDbWriter writer = new JdbcDbWriter(config, dialect, dbStructure) {
      @Override
      protected CachedConnectionProvider connectionProvider(int maxConnAttempts, long retryBackoff) {
        return new CloseVerifyingConnectionProvider(connection);
      }
    };
    when(dialect.parseTableIdentifier(any())).thenReturn(new TableId(null, null, "books"));
    when(dialect.createPreparedStatement(any(), any())).thenReturn(statement);
    when(dialect.statementBinder(
        any(),
        any(),
        any(),
        any(),
        any(),
        any(),
        org.mockito.Matchers.anyBoolean()
    )).thenReturn(org.mockito.Mockito.mock(DatabaseDialect.StatementBinder.class));
    when(statement.executeBatch()).thenReturn(new int[]{Statement.SUCCESS_NO_INFO});
    when(dbStructure.tableDefinition(any(), any())).thenReturn(null);

    WriteFence fence = WriteFence.start(writerClock, 10_000_000L);
    writerClock.advanceMillis(10);

    ConnectException thrown = assertThrows(ConnectException.class,
        () -> writer.write(Collections.singletonList(record(1L, "first", 0)), fence));

    assertTrue(thrown instanceof WriteFenceTimeoutException);
    assertEquals(1, thrown.getSuppressed().length);
    assertSame(rollbackFailure, thrown.getSuppressed()[0]);
    verify(connection, times(1)).rollback();
    verify(connection, times(1)).close();
  }

  @Test
  public void enabledFenceRejectsUnsupportedMysqlDialectBeforeDml() {
    JdbcSinkTask task = new JdbcSinkTask(clock);
    task.initialize(ctx);
    Map<String, String> props = eligibleProps(10);
    props.put(JdbcSinkConfig.CONNECTION_URL, "jdbc:mysql://example/db");
    props.put(JdbcSinkConfig.PK_MODE, "kafka");

    ConfigException thrown = assertThrows(ConfigException.class, () -> task.start(props));
    assertTrue(thrown.getMessage().contains(JdbcSinkConfig.WRITE_FENCE_TIMEOUT_MS));
  }

  @Test
  public void enabledFenceRejectsAutoCreateBeforeDml() {
    JdbcSinkTask task = new JdbcSinkTask(clock);
    task.initialize(ctx);
    Map<String, String> props = eligibleProps(10);
    props.put(JdbcSinkConfig.AUTO_CREATE, "true");

    ConfigException thrown = assertThrows(ConfigException.class, () -> task.start(props));
    assertTrue(thrown.getMessage().contains(JdbcSinkConfig.AUTO_CREATE));
  }

  @Test
  public void enabledFenceRejectsInsertModeBeforeDml() {
    JdbcSinkTask task = new JdbcSinkTask(clock);
    task.initialize(ctx);
    Map<String, String> props = eligibleProps(10);
    props.put(JdbcSinkConfig.INSERT_MODE, "insert");

    ConfigException thrown = assertThrows(ConfigException.class, () -> task.start(props));
    assertTrue(thrown.getMessage().contains(JdbcSinkConfig.INSERT_MODE));
  }

  @Test
  public void enabledFenceRejectsMissingPrimaryKeyBeforeDml() {
    JdbcSinkTask task = new JdbcSinkTask(clock);
    task.initialize(ctx);
    Map<String, String> props = eligibleProps(10);
    props.put(JdbcSinkConfig.PK_MODE, "none");

    ConfigException thrown = assertThrows(ConfigException.class, () -> task.start(props));
    assertTrue(thrown.getMessage().contains(JdbcSinkConfig.PK_MODE));
  }

  @Test
  public void enabledFenceAcceptsReceiptResetConfigButIgnoresIt() throws SQLException {
    List<SinkRecord> records = Collections.singletonList(record(1L, "first", 0));
    mockWriter.write(anyObject(), anyObject());
    expectLastCall();
    expect(ctx.errantRecordReporter()).andReturn(null);
    replayAll();

    TestSinkTask task = new TestSinkTask(clock, mockWriter);
    task.initialize(ctx);
    Map<String, String> props = eligibleProps(10);
    props.put(JdbcSinkConfig.WRITE_FENCE_RECEIPT_RESET_ON_ASSIGNMENT, "true");
    task.start(props);

    task.put(records);
    verifyAll();
  }

  private int countRows(String table) throws SQLException {
    final int[] count = new int[1];
    sqliteHelper.select("SELECT COUNT(*) FROM " + table, rs -> count[0] = rs.getInt(1));
    return count[0];
  }

  private SinkRecord record(long id, String title, long offset) {
    Struct value = new Struct(VALUE_SCHEMA).put("title", title);
    return new SinkRecord("books", 0, KEY_SCHEMA, id, VALUE_SCHEMA, value, offset);
  }

  private Map<String, String> eligibleProps(long timeoutMs) {
    Map<String, String> props = new HashMap<>();
    props.put(JdbcSinkConfig.CONNECTION_URL, sqliteHelper.sqliteUri());
    props.put(JdbcSinkConfig.INSERT_MODE, "upsert");
    props.put(JdbcSinkConfig.PK_MODE, "record_key");
    props.put(JdbcSinkConfig.PK_FIELDS, "id");
    props.put(JdbcSinkConfig.AUTO_CREATE, "false");
    props.put(JdbcSinkConfig.AUTO_EVOLVE, "false");
    props.put(JdbcSinkConfig.WRITE_FENCE_TIMEOUT_MS, String.valueOf(timeoutMs));
    return props;
  }

  private static final class ManualNanoClock implements LongSupplier {
    private long nanos;

    @Override
    public long getAsLong() {
      return nanos;
    }

    void advanceMillis(long millis) {
      nanos += millis * 1_000_000L;
    }
  }

  private static final class TestSinkTask extends JdbcSinkTask {
    private final Queue<JdbcDbWriter> writers;
    private JdbcDbWriter lastWriter;

    TestSinkTask(LongSupplier clock, JdbcDbWriter... writers) {
      super(clock);
      this.writers = new ArrayDeque<>();
      Collections.addAll(this.writers, writers);
    }

    @Override
    void initWriter() {
      dialect = new SqliteDatabaseDialect(config);
      if (!writers.isEmpty()) {
        lastWriter = writers.remove();
      }
      writer = lastWriter;
    }
  }

  private static final class CloseVerifyingConnectionProvider extends CachedConnectionProvider {
    private final Connection connection;

    CloseVerifyingConnectionProvider(Connection connection) {
      super(org.mockito.Mockito.mock(ConnectionProvider.class), 1, 0);
      this.connection = connection;
    }

    @Override
    public synchronized Connection getConnection() {
      return connection;
    }

    @Override
    public synchronized void close() {
      try {
        connection.close();
      } catch (SQLException ignored) {
        // CachedConnectionProvider.close() ignores close failures.
      }
    }
  }
}
