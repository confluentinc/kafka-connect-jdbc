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

package io.confluent.connect.jdbc.sink;

import io.confluent.connect.jdbc.dialect.DatabaseDialect;
import io.confluent.connect.jdbc.dialect.SqliteDatabaseDialect;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.errors.RetriableException;
import org.apache.kafka.connect.sink.ErrantRecordReporter;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTaskContext;
import org.junit.Test;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.function.LongSupplier;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class JdbcSinkTaskWriteFenceTest {

  @Test
  public void repeatedShortRetriesFenceWhenFirstReceiptAgeCrossesBudget() {
    MutableClock clock = new MutableClock();
    TestTask task = task(clock, throwSql(), throwSql());
    task.initialize(contextWithoutReporter());
    task.start(props(10, false, 5));
    List<SinkRecord> records = Collections.singletonList(record("topic", 0, 1L));

    assertThrows(RetriableException.class, () -> task.put(records));
    clock.set(ms(5));
    assertThrows(RetriableException.class, () -> task.put(records));
    clock.set(ms(10));

    assertThrows(JdbcWriteFenceException.class, () -> task.put(records));
    assertEquals(2, task.writes.size());
  }

  @Test
  public void fencesUserExampleBeforeSecondAttemptWrites() {
    MutableClock clock = new MutableClock();
    TestTask task = task(
        clock,
        records -> {
          clock.set(ms(90));
          throw new SQLException("first attempt failed after 0.9D");
        }
    );
    task.initialize(contextWithoutReporter());
    task.start(props(100, false, 5));
    List<SinkRecord> records = Collections.singletonList(record("topic", 0, 1L));

    assertThrows(RetriableException.class, () -> task.put(records));
    clock.set(ms(101));

    assertThrows(JdbcWriteFenceException.class, () -> task.put(records));
    assertEquals(1, task.writes.size());
  }

  @Test
  public void firstReceiptStartsBeforeRecordInspection() {
    MutableClock clock = new MutableClock();
    TestTask task = task(clock, success());
    task.initialize(contextWithoutReporter());
    task.start(props(100, false, 5));
    SinkRecord delayedInspection = new SinkRecord("topic", 0, null, null, null, null, 1L) {
      @Override
      public String topic() {
        clock.set(ms(100));
        return super.topic();
      }
    };

    assertThrows(
        JdbcWriteFenceException.class,
        () -> task.put(Collections.singletonList(delayedInspection))
    );
    assertEquals(0, task.writes.size());
  }

  @Test
  public void successfulBatchRemovesReceiptSoNextBatchStartsFresh() {
    MutableClock clock = new MutableClock();
    TestTask task = task(clock, success(), success());
    task.initialize(contextWithoutReporter());
    task.start(props(100, false, 0));
    List<SinkRecord> records = Collections.singletonList(record("topic", 0, 1L));

    task.put(records);
    clock.set(ms(100));
    task.put(records);

    assertEquals(2, task.writes.size());
  }

  @Test
  public void closeBeforeRetryPreservesPendingReceiptAndTerminalFenceSurvivesCallbacks() {
    MutableClock clock = new MutableClock();
    TestTask task = task(clock, throwSql());
    task.initialize(contextWithoutReporter());
    task.start(props(100, false, 5));
    List<SinkRecord> records = Collections.singletonList(record("topic", 0, 1L));

    assertThrows(RetriableException.class, () -> task.put(records));
    task.close(Collections.singletonList(new TopicPartition("topic", 0)));
    clock.set(ms(100));
    assertThrows(JdbcWriteFenceException.class, () -> task.put(records));
    task.open(Collections.singletonList(new TopicPartition("topic", 0)));
    task.close(Collections.singletonList(new TopicPartition("topic", 0)));
    assertThrows(JdbcWriteFenceException.class, () -> task.put(records));

    assertEquals(1, task.writes.size());
  }

  @Test
  public void resetOnAssignmentClearsOnlyOpenedSourcePartitionReceipt() {
    MutableClock clock = new MutableClock();
    TestTask task = task(clock, throwSql(), success());
    task.initialize(contextWithoutReporter());
    task.start(props(100, true, 5));
    List<SinkRecord> records = Collections.singletonList(record("topic", 0, 1L));

    assertThrows(RetriableException.class, () -> task.put(records));
    clock.set(ms(100));
    task.open(Collections.singletonList(new TopicPartition("topic", 0)));
    task.put(records);

    assertEquals(2, task.writes.size());
  }

  @Test
  public void retryIsNotScheduledWhenFailedCommitBecomesExpired() {
    MutableClock clock = new MutableClock();
    SinkTaskContext context = contextWithoutReporter();
    TestTask task = task(
        clock,
        records -> {
          clock.set(ms(100));
          throw new SQLException("ambiguous commit acknowledgement");
        }
    );
    task.initialize(context);
    task.start(props(100, false, 5));

    assertThrows(
        JdbcWriteFenceException.class,
        () -> task.put(Collections.singletonList(record("topic", 0, 1L)))
    );
    verify(context, never()).timeout(anyLong());
  }

  @Test
  public void expiryDuringConnectionRetirementIsTerminalBeforeRetry() {
    MutableClock clock = new MutableClock();
    SinkTaskContext context = contextWithoutReporter();
    TestTask task = task(clock, records -> {
      clock.set(ms(90));
      throw new SQLException("short failure before slow connection retirement");
    });
    task.initialize(context);
    task.start(props(100, true, 5));
    ((ScriptedWriter) task.writer).closeAction = () -> clock.set(ms(110));
    List<SinkRecord> records = Collections.singletonList(record("topic", 0, 1L));

    assertThrows(JdbcWriteFenceException.class, () -> task.put(records));
    task.open(Collections.singletonList(new TopicPartition("topic", 0)));
    assertThrows(JdbcWriteFenceException.class, () -> task.put(records));
    verify(context, never()).timeout(anyLong());
    assertEquals(1, task.writes.size());
  }

  @Test
  public void unrollAndDlqDoNotSwallowFence() {
    MutableClock clock = new MutableClock();
    ErrantRecordReporter reporter = mock(ErrantRecordReporter.class);
    SinkTaskContext context = mock(SinkTaskContext.class);
    when(context.errantRecordReporter()).thenReturn(reporter);
    TestTask task = task(
        clock,
        throwSql(),
        records -> {
          clock.set(ms(100));
          throw new SQLException("single record failed after budget");
        }
    );
    task.initialize(context);
    task.start(props(100, false, 0));

    assertThrows(
        JdbcWriteFenceException.class,
        () -> task.put(Collections.singletonList(record("topic", 0, 1L)))
    );
    verify(reporter, never()).report(any(SinkRecord.class), any(Throwable.class));
    assertEquals(2, task.writes.size());
  }

  @Test
  public void disabledFenceDoesNotApplyEligibilityRestrictions() {
    JdbcSinkTask task = new JdbcSinkTask();
    task.initialize(contextWithoutReporter());
    Map<String, String> props = props(0, false, 0);
    props.put(JdbcSinkConfig.CONNECTION_URL, "jdbc:mysql://something");
    props.put(JdbcSinkConfig.INSERT_MODE, "insert");
    props.put(JdbcSinkConfig.PK_MODE, "none");

    task.start(props);
    task.stop();
  }

  @Test
  public void enabledFenceRejectsUnsupportedConfigurationsBeforeDml() {
    assertUnsupported("jdbc:mysql://something", "upsert", "kafka", "false", "false");
    assertUnsupported("jdbc:sqlite:memory", "insert", "kafka", "false", "false");
    assertUnsupported("jdbc:sqlite:memory", "upsert", "none", "false", "false");
    assertUnsupported("jdbc:sqlite:memory", "upsert", "kafka", "true", "false");
    assertUnsupported("jdbc:sqlite:memory", "upsert", "kafka", "false", "true");
    assertUnsupported("jdbc:sqlite:memory", "upsert", "kafka", "false", "false", "view");
    assertUnsupported(
        "jdbc:sqlite:memory",
        "upsert",
        "kafka",
        "false",
        "false",
        "partitioned table"
    );
    assertUnsupported("jdbc:sqlite:memory", "upsert", "kafka", "false", "false", "table,view");
  }

  @Test
  public void enabledFenceAcceptsSupportedTransactionalTargetsBeforeDml() {
    JdbcSinkTask sqliteTask = new JdbcSinkTask();
    sqliteTask.initialize(contextWithoutReporter());
    sqliteTask.start(props(100, false, 0));
    sqliteTask.stop();

    JdbcSinkTask postgresDeleteTask = new JdbcSinkTask();
    postgresDeleteTask.initialize(contextWithoutReporter());
    Map<String, String> props = props(100, false, 0);
    props.put(JdbcSinkConfig.CONNECTION_URL, "jdbc:postgresql://localhost:5432/test");
    props.put(JdbcSinkConfig.PK_MODE, "record_key");
    props.put(JdbcSinkConfig.DELETE_ENABLED, "true");
    postgresDeleteTask.start(props);
    postgresDeleteTask.stop();
  }

  private static void assertUnsupported(
      String connectionUrl,
      String insertMode,
      String pkMode,
      String autoCreate,
      String autoEvolve
  ) {
    assertUnsupported(connectionUrl, insertMode, pkMode, autoCreate, autoEvolve, "table");
  }

  private static void assertUnsupported(
      String connectionUrl,
      String insertMode,
      String pkMode,
      String autoCreate,
      String autoEvolve,
      String tableTypes
  ) {
    JdbcSinkTask task = new JdbcSinkTask();
    task.initialize(contextWithoutReporter());
    Map<String, String> props = props(100, false, 0);
    props.put(JdbcSinkConfig.CONNECTION_URL, connectionUrl);
    props.put(JdbcSinkConfig.INSERT_MODE, insertMode);
    props.put(JdbcSinkConfig.PK_MODE, pkMode);
    props.put(JdbcSinkConfig.AUTO_CREATE, autoCreate);
    props.put(JdbcSinkConfig.AUTO_EVOLVE, autoEvolve);
    props.put(JdbcSinkConfig.TABLE_TYPES_CONFIG, tableTypes);

    assertThrows(ConfigException.class, () -> task.start(props));
    task.stop();
  }

  private static SinkTaskContext contextWithoutReporter() {
    SinkTaskContext context = mock(SinkTaskContext.class);
    when(context.errantRecordReporter()).thenReturn(null);
    return context;
  }

  private static TestTask task(MutableClock clock, WriteAction... actions) {
    List<WriteAction> remainingActions = new ArrayList<>();
    Collections.addAll(remainingActions, actions);
    return new TestTask(clock, remainingActions);
  }

  private static WriteAction success() {
    return records -> {
    };
  }

  private static WriteAction throwSql() {
    return records -> {
      throw new SQLException("write failed");
    };
  }

  private static SinkRecord record(String topic, int partition, long offset) {
    return new SinkRecord(topic, partition, null, null, null, null, offset);
  }

  private static Map<String, String> props(
      long timeoutMs,
      boolean resetOnAssignment,
      int maxRetries
  ) {
    Map<String, String> props = new HashMap<>();
    props.put(JdbcSinkConfig.CONNECTION_URL, "jdbc:sqlite:memory");
    props.put(JdbcSinkConfig.INSERT_MODE, "upsert");
    props.put(JdbcSinkConfig.PK_MODE, "kafka");
    props.put(JdbcSinkConfig.MAX_RETRIES, String.valueOf(maxRetries));
    props.put(JdbcSinkConfig.RETRY_BACKOFF_MS, "0");
    props.put(JdbcSinkConfig.WRITE_FENCE_TIMEOUT_MS, String.valueOf(timeoutMs));
    props.put(
        JdbcSinkConfig.WRITE_FENCE_RECEIPT_RESET_ON_ASSIGNMENT,
        String.valueOf(resetOnAssignment)
    );
    return props;
  }

  private static long ms(long millis) {
    return millis * 1_000_000L;
  }

  private interface WriteAction {
    void write(Collection<SinkRecord> records) throws SQLException, TableAlterOrCreateException;
  }

  private static class MutableClock implements LongSupplier {
    private long nanos;

    void set(long nanos) {
      this.nanos = nanos;
    }

    @Override
    public long getAsLong() {
      return nanos;
    }
  }

  private static class TestTask extends JdbcSinkTask {
    private final List<WriteAction> actions;
    private final List<List<SinkRecord>> writes = new ArrayList<>();

    TestTask(MutableClock clock, List<WriteAction> actions) {
      super(clock);
      this.actions = actions;
    }

    @Override
    void initWriter() {
      this.dialect = new SqliteDatabaseDialect(config);
      this.writer = new ScriptedWriter(config, actions, writes);
    }
  }

  private static class ScriptedWriter extends JdbcDbWriter {
    private final List<WriteAction> actions;
    private final List<List<SinkRecord>> writes;
    private Runnable closeAction = () -> { };

    ScriptedWriter(
        JdbcSinkConfig config,
        List<WriteAction> actions,
        List<List<SinkRecord>> writes
    ) {
      super(config, mock(DatabaseDialect.class), mock(DbStructure.class));
      this.actions = actions;
      this.writes = writes;
    }

    @Override
    void write(Collection<SinkRecord> records) throws SQLException, TableAlterOrCreateException {
      writes.add(new ArrayList<>(records));
      if (!actions.isEmpty()) {
        actions.remove(0).write(records);
      }
    }

    @Override
    void closeQuietly() {
      closeAction.run();
    }
  }
}
