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

import static org.easymock.EasyMock.anyObject;
import static org.easymock.EasyMock.capture;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.expectLastCall;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.io.StringWriter;
import java.sql.BatchUpdateException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.time.ZoneId;
import java.util.concurrent.CompletableFuture;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.data.Timestamp;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.errors.RetriableException;
import org.apache.kafka.connect.sink.ErrantRecordReporter;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTaskContext;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.PatternLayout;
import org.apache.log4j.WriterAppender;
import org.easymock.Capture;
import org.easymock.EasyMockSupport;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import io.confluent.connect.jdbc.util.DateTimeUtils;

public class JdbcSinkTaskTest extends EasyMockSupport {
  private static final String REDACTED = "<redacted>";
  private static final String RETRY_CANARY = "retry-secret";
  private static final String SQL_SERVER_CANARY = "sqlserver-secret@example.com";
  private static final String MYSQL_CANARY = "mysql-secret@example.com";

  private final SqliteHelper sqliteHelper = new SqliteHelper(getClass().getSimpleName());
  private final JdbcDbWriter mockWriter = createMock(JdbcDbWriter.class);
  private final SinkTaskContext ctx = createMock(SinkTaskContext.class);
  private final Logger taskLogger = Logger.getLogger(JdbcSinkTask.class);

  private Level taskLogLevel;
  private StringWriter taskLogOutput;
  private WriterAppender taskLogAppender;

  private static final Schema SCHEMA = SchemaBuilder.struct().name("com.example.Person")
      .field("firstName", Schema.STRING_SCHEMA)
      .field("lastName", Schema.STRING_SCHEMA)
      .field("age", Schema.OPTIONAL_INT32_SCHEMA)
      .field("bool", Schema.OPTIONAL_BOOLEAN_SCHEMA)
      .field("short", Schema.OPTIONAL_INT16_SCHEMA)
      .field("byte", Schema.OPTIONAL_INT8_SCHEMA)
      .field("long", Schema.OPTIONAL_INT64_SCHEMA)
      .field("float", Schema.OPTIONAL_FLOAT32_SCHEMA)
      .field("double", Schema.OPTIONAL_FLOAT64_SCHEMA)
      .field("modified", Timestamp.SCHEMA)
      .build();
  private static final SinkRecord RECORD = new SinkRecord(
      "stub",
      0,
      null,
      null,
      null,
      null,
      0
  );

  @Before
  public void setUp() throws IOException, SQLException {
    sqliteHelper.setUp();
  }

  @After
  public void tearDown() throws IOException, SQLException {
    stopCapturingTaskLogs();
    sqliteHelper.tearDown();
  }

  @Test
  public void putPropagatesToDbWithAutoCreateAndPkModeKafka() throws Exception {
    Map<String, String> props = new HashMap<>();
    props.put("connection.url", sqliteHelper.sqliteUri());
    props.put("auto.create", "true");
    props.put("pk.mode", "kafka");
    props.put("pk.fields", "kafka_topic,kafka_partition,kafka_offset");
    String timeZoneID = "America/Los_Angeles";
    ZoneId zoneId = ZoneId.of(timeZoneID);
    props.put("db.timezone", timeZoneID);

    JdbcSinkTask task = new JdbcSinkTask();
    task.initialize(mock(SinkTaskContext.class));

    task.start(props);

    final Struct struct = new Struct(SCHEMA)
        .put("firstName", "Alex")
        .put("lastName", "Smith")
        .put("bool", true)
        .put("short", (short) 1234)
        .put("byte", (byte) -32)
        .put("long", 12425436L)
        .put("float", (float) 2356.3)
        .put("double", -2436546.56457)
        .put("age", 21)
        .put("modified", new Date(1474661402123L));

    final String topic = "atopic";

    task.put(Collections.singleton(
        new SinkRecord(topic, 1, null, null, SCHEMA, struct, 42)
    ));

    assertEquals(
        1,
        sqliteHelper.select(
            "SELECT * FROM " + topic,
            new SqliteHelper.ResultSetReadCallback() {
              @Override
              public void read(ResultSet rs) throws SQLException {
                assertEquals(topic, rs.getString("kafka_topic"));
                assertEquals(1, rs.getInt("kafka_partition"));
                assertEquals(42, rs.getLong("kafka_offset"));
                assertEquals(struct.getString("firstName"), rs.getString("firstName"));
                assertEquals(struct.getString("lastName"), rs.getString("lastName"));
                assertEquals(struct.getBoolean("bool"), rs.getBoolean("bool"));
                assertEquals(struct.getInt8("byte").byteValue(), rs.getByte("byte"));
                assertEquals(struct.getInt16("short").shortValue(), rs.getShort("short"));
                assertEquals(struct.getInt32("age").intValue(), rs.getInt("age"));
                assertEquals(struct.getInt64("long").longValue(), rs.getLong("long"));
                assertEquals(struct.getFloat32("float"), rs.getFloat("float"), 0.01);
                assertEquals(struct.getFloat64("double"), rs.getDouble("double"), 0.01);
                java.sql.Timestamp dbTimestamp = rs.getTimestamp(
                    "modified",
                    DateTimeUtils.getZoneIdCalendar(zoneId)
                );
                assertEquals(((java.util.Date) struct.get("modified")).getTime(), dbTimestamp.getTime());
              }
            }
        )
    );
  }

  @Test
  public void putPropagatesToDbWithPkModeRecordValue() throws Exception {
    Map<String, String> props = new HashMap<>();
    props.put("connection.url", sqliteHelper.sqliteUri());
    props.put("pk.mode", "record_value");
    props.put("pk.fields", "firstName,lastName");

    JdbcSinkTask task = new JdbcSinkTask();
    task.initialize(mock(SinkTaskContext.class));

    final String topic = "atopic";

    sqliteHelper.createTable(
        "CREATE TABLE " + topic + "(" +
        "    firstName  TEXT," +
        "    lastName  TEXT," +
        "    age INTEGER," +
        "    bool  NUMERIC," +
        "    byte  INTEGER," +
        "    short INTEGER NULL," +
        "    long INTEGER," +
        "    float NUMERIC," +
        "    double NUMERIC," +
        "    bytes BLOB," +
        "    modified DATETIME, "+
        "PRIMARY KEY (firstName, lastName));"
    );

    task.start(props);

    final Struct struct = new Struct(SCHEMA)
        .put("firstName", "Christina")
        .put("lastName", "Brams")
        .put("bool", false)
        .put("byte", (byte) -72)
        .put("long", 8594L)
        .put("double", 3256677.56457d)
        .put("age", 28)
        .put("modified", new Date(1474661402123L));

    task.put(Collections.singleton(new SinkRecord(topic, 1, null, null, SCHEMA, struct, 43)));

    assertEquals(
        1,
        sqliteHelper.select(
            "SELECT * FROM " + topic + " WHERE firstName='" + struct.getString("firstName") + "' and lastName='" + struct.getString("lastName") + "'",
            new SqliteHelper.ResultSetReadCallback() {
              @Override
              public void read(ResultSet rs) throws SQLException {
                assertEquals(struct.getBoolean("bool"), rs.getBoolean("bool"));
                rs.getShort("short");
                assertTrue(rs.wasNull());
                assertEquals(struct.getInt8("byte").byteValue(), rs.getByte("byte"));
                assertEquals(struct.getInt32("age").intValue(), rs.getInt("age"));
                assertEquals(struct.getInt64("long").longValue(), rs.getLong("long"));
                rs.getShort("float");
                assertTrue(rs.wasNull());
                assertEquals(struct.getFloat64("double"), rs.getDouble("double"), 0.01);
                java.sql.Timestamp dbTimestamp = rs.getTimestamp(
                    "modified",
                    DateTimeUtils.getZoneIdCalendar(ZoneOffset.UTC)
                );
                assertEquals(((java.util.Date) struct.get("modified")).getTime(), dbTimestamp.getTime());
              }
            }
        )
    );
  }

  @Test
  public void retries() throws SQLException {
    final int maxRetries = 2;
    final int retryBackoffMs = 1000;

    List<SinkRecord> records = createRecordsList(1);

    mockWriter.write(records);
    SQLException chainedException = new SQLException(RETRY_CANARY + "-1", "42000", 10);
    chainedException.setNextException(new SQLException(RETRY_CANARY + "-2", "42001", 20));
    chainedException.setNextException(new SQLException(RETRY_CANARY + "-3", "42002", 30));
    expectLastCall().andThrow(chainedException).times(1 + maxRetries);

    ctx.timeout(retryBackoffMs);
    expectLastCall().times(maxRetries);

    mockWriter.closeQuietly();
    expectLastCall().times(maxRetries);

    JdbcSinkTask task = new JdbcSinkTask() {
      @Override
      void initWriter() {
        this.writer = mockWriter;
      }
    };
    task.initialize(ctx);
    expect(ctx.errantRecordReporter()).andReturn(null);
    replayAll();

    Map<String, String> props = setupBasicProps(maxRetries, retryBackoffMs);
    task.start(props);

    RetriableException firstRetry =
        assertThrows(RetriableException.class, () -> task.put(records));
    assertTaskExceptionRedacted(firstRetry, RETRY_CANARY, "42000", 10);

    RetriableException secondRetry =
        assertThrows(RetriableException.class, () -> task.put(records));
    assertTaskExceptionRedacted(secondRetry, RETRY_CANARY, "42000", 10);

    ConnectException exhausted =
        assertThrows(ConnectException.class, () -> task.put(records));
    assertFalse(exhausted instanceof RetriableException);
    assertTaskExceptionRedacted(exhausted, RETRY_CANARY, "42000", 10);

    verifyAll();
  }

  @Test
  public void errorReportingRedactsPlainSqlException() throws SQLException {
    List<SinkRecord> records = createRecordsList(1);

    mockWriter.write(records);
    SQLException exception = new SQLException(
        "Duplicate entry '" + MYSQL_CANARY + "' for key 'email'",
        "23000",
        1062
    );
    expectLastCall().andThrow(exception);
    mockWriter.closeQuietly();
    expectLastCall();
    mockWriter.write(anyObject());
    expectLastCall().andThrow(exception);

    JdbcSinkTask task = new JdbcSinkTask() {
      @Override
      void initWriter() {
        this.writer = mockWriter;
      }
    };
    task.initialize(ctx);
    ErrantRecordReporter reporter = createMock(ErrantRecordReporter.class);
    Capture<Throwable> reportedException = Capture.newInstance();
    expect(ctx.errantRecordReporter()).andReturn(reporter);
    expect(reporter.report(anyObject(), capture(reportedException)))
        .andReturn(CompletableFuture.completedFuture(null));
    mockWriter.closeQuietly();
    expectLastCall();
    replayAll();

    Map<String, String> props = setupBasicProps(0, 0);
    task.start(props);
    captureTaskLogs();
    task.put(records);

    assertTrue(reportedException.hasCaptured());
    assertTrue(reportedException.getValue() instanceof SQLException);
    assertRedactedSqlException(
        (SQLException) reportedException.getValue(),
        MYSQL_CANARY,
        "23000",
        1062
    );
    String logs = capturedTaskLogs();
    assertTrue(logs.contains(REDACTED));
    assertFalse(logs.contains(MYSQL_CANARY));
    verifyAll();
  }

  @Test
  public void errorReportingTableAlterOrCreateException() throws SQLException {
    List<SinkRecord> records = createRecordsList(1);

    mockWriter.write(records);
    TableAlterOrCreateException exception = new TableAlterOrCreateException("cause 1");
    expectLastCall().andThrow(exception);
    mockWriter.closeQuietly();
    expectLastCall();
    mockWriter.write(anyObject());
    expectLastCall().andThrow(exception);

    JdbcSinkTask task = new JdbcSinkTask() {
      @Override
      void initWriter() {
        this.writer = mockWriter;
      }
    };
    task.initialize(ctx);
    ErrantRecordReporter reporter = createMock(ErrantRecordReporter.class);
    expect(ctx.errantRecordReporter()).andReturn(reporter);
    expect(reporter.report(anyObject(), anyObject())).andReturn(CompletableFuture.completedFuture(null));
    mockWriter.closeQuietly();
    expectLastCall();
    replayAll();

    Map<String, String> props = setupBasicProps(0, 0);
    task.start(props);
    task.put(records);
    verifyAll();
  }

  @Test
  public void batchErrorReporting() throws SQLException {
    final int batchSize = 3;

    List<SinkRecord> records = createRecordsList(batchSize);

    mockWriter.write(records);
    SQLException exception = new SQLException("cause 1");
    expectLastCall().andThrow(exception);
    mockWriter.closeQuietly();
    expectLastCall();
    mockWriter.write(anyObject());
    expectLastCall().andThrow(exception).times(batchSize);

    JdbcSinkTask task = new JdbcSinkTask() {
      @Override
      void initWriter() {
        this.writer = mockWriter;
      }
    };
    task.initialize(ctx);
    ErrantRecordReporter reporter = createMock(ErrantRecordReporter.class);
    expect(ctx.errantRecordReporter()).andReturn(reporter);
    expect(reporter.report(anyObject(), anyObject())).andReturn(CompletableFuture.completedFuture(null)).times(batchSize);
    for (int i = 0; i < batchSize; i++) {
      mockWriter.closeQuietly();
      expectLastCall();
    }
    replayAll();

    Map<String, String> props = setupBasicProps(0, 0);
    task.start(props);
    task.put(records);
    verifyAll();
  }

  @Test
  public void oneInBatchErrorReporting() throws SQLException {
    final int batchSize = 3;

    List<SinkRecord> records = createRecordsList(batchSize);

    mockWriter.write(records);
    SQLException exception = new SQLException("cause 1");
    expectLastCall().andThrow(exception);
    mockWriter.closeQuietly();
    expectLastCall();
    mockWriter.write(anyObject());
    expectLastCall().times(2);
    expectLastCall().andThrow(exception);

    JdbcSinkTask task = new JdbcSinkTask() {
      @Override
      void initWriter() {
        this.writer = mockWriter;
      }
    };
    task.initialize(ctx);
    ErrantRecordReporter reporter = createMock(ErrantRecordReporter.class);
    expect(ctx.errantRecordReporter()).andReturn(reporter);
    expect(reporter.report(anyObject(), anyObject())).andReturn(CompletableFuture.completedFuture(null));
    mockWriter.closeQuietly();
    expectLastCall();
    replayAll();

    Map<String, String> props = setupBasicProps(0, 0);
    task.start(props);
    task.put(records);
    verifyAll();
  }

  @Test
  public void oneInMiddleBatchErrorReporting() throws SQLException {
    final int batchSize = 3;

    List<SinkRecord> records = createRecordsList(batchSize);

    mockWriter.write(records);
    SQLException exception = new SQLException("cause 1");
    expectLastCall().andThrow(exception);
    mockWriter.closeQuietly();
    expectLastCall();
    mockWriter.write(anyObject());
    expectLastCall();
    mockWriter.write(anyObject());
    expectLastCall().andThrow(exception);
    mockWriter.write(anyObject());
    expectLastCall();

    JdbcSinkTask task = new JdbcSinkTask() {
      @Override
      void initWriter() {
        this.writer = mockWriter;
      }
    };
    task.initialize(ctx);
    ErrantRecordReporter reporter = createMock(ErrantRecordReporter.class);
    expect(ctx.errantRecordReporter()).andReturn(reporter);
    expect(reporter.report(anyObject(), anyObject())).andReturn(CompletableFuture.completedFuture(null));
    mockWriter.closeQuietly();
    expectLastCall();
    replayAll();

    Map<String, String> props = setupBasicProps(0, 0);
    task.start(props);
    task.put(records);
    verifyAll();
  }

  @Test
  public void putRedactsNonPostgresBatchExceptionBeforeThrowingAndLogging()
      throws SQLException {
    List<SinkRecord> records = createRecordsList(1);
    BatchUpdateException exception = new BatchUpdateException(
        "Violation of UNIQUE KEY constraint 'UQ_email'. Cannot insert duplicate key in object "
            + "'dbo.orders'. The duplicate key value is (" + SQL_SERVER_CANARY + ").",
        "23000",
        2627,
        new int[]{Statement.EXECUTE_FAILED}
    );

    mockWriter.write(records);
    expectLastCall().andThrow(exception);

    JdbcSinkTask task = new JdbcSinkTask() {
      @Override
      void initWriter() {
        this.writer = mockWriter;
      }
    };
    task.initialize(ctx);
    expect(ctx.errantRecordReporter()).andReturn(null);
    replayAll();

    Map<String, String> props = setupBasicProps(0, 0);
    task.start(props);
    captureTaskLogs();
    ConnectException thrown =
        assertThrows(ConnectException.class, () -> task.put(records));

    SQLException redacted = assertTaskExceptionRedacted(
        thrown,
        SQL_SERVER_CANARY,
        "23000",
        2627
    );
    assertTrue(redacted instanceof BatchUpdateException);
    assertArrayEquals(
        exception.getUpdateCounts(),
        ((BatchUpdateException) redacted).getUpdateCounts()
    );
    assertArrayEquals(exception.getStackTrace(), redacted.getStackTrace());
    String logs = capturedTaskLogs();
    assertTrue(logs.contains(REDACTED));
    assertFalse(logs.contains(SQL_SERVER_CANARY));
    verifyAll();
  }

  private SQLException assertTaskExceptionRedacted(
      Throwable taskException,
      String sensitiveValue,
      String sqlState,
      int errorCode
  ) {
    assertTrue(taskException.getCause() instanceof SQLException);
    return assertRedactedSqlException(
        (SQLException) taskException.getCause(),
        sensitiveValue,
        sqlState,
        errorCode
    );
  }

  private SQLException assertRedactedSqlException(
      SQLException allMessagesException,
      String sensitiveValue,
      String sqlState,
      int errorCode
  ) {
    assertTrue(allMessagesException.getMessage().contains(REDACTED));
    assertFalse(allMessagesException.getMessage().contains(sensitiveValue));
    SQLException redactedException = allMessagesException.getNextException();
    assertNotNull(redactedException);
    assertEquals(sqlState, redactedException.getSQLState());
    assertEquals(errorCode, redactedException.getErrorCode());
    for (Throwable current : redactedException) {
      assertEquals(REDACTED, current.getMessage());
      assertFalse(current.getMessage().contains(sensitiveValue));
    }
    return redactedException;
  }

  private void captureTaskLogs() {
    taskLogLevel = taskLogger.getLevel();
    taskLogOutput = new StringWriter();
    taskLogAppender = new WriterAppender(new PatternLayout("%m%n"), taskLogOutput);
    taskLogger.setLevel(Level.DEBUG);
    taskLogger.addAppender(taskLogAppender);
  }

  private String capturedTaskLogs() {
    return taskLogOutput.toString();
  }

  private void stopCapturingTaskLogs() {
    if (taskLogAppender != null) {
      taskLogger.removeAppender(taskLogAppender);
      taskLogAppender.close();
      taskLogger.setLevel(taskLogLevel);
      taskLogAppender = null;
    }
  }

  private List<SinkRecord> createRecordsList(int batchSize) {
    List<SinkRecord> records = new ArrayList<>();
    for (int i = 0; i < batchSize; i++) {
      records.add(RECORD);
    }
    return records;
  }

  private Map<String, String> setupBasicProps(int maxRetries, long retryBackoffMs) {
    Map<String, String> props = new HashMap<>();
    props.put(JdbcSinkConfig.CONNECTION_URL, "stub");
    props.put(JdbcSinkConfig.MAX_RETRIES, String.valueOf(maxRetries));
    props.put(JdbcSinkConfig.RETRY_BACKOFF_MS, String.valueOf(retryBackoffMs));
    props.put(JdbcSinkConfig.TRIM_SENSITIVE_LOG_ENABLED, "false");
    return props;
  }
}
