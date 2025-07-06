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

import static io.confluent.connect.jdbc.sink.JdbcSinkConfig.TABLE_NAME_FORMAT;
import static io.confluent.connect.jdbc.sink.JdbcSinkConfig.TABLE_NAME_FORMAT_RECORD_HEADER;
import static org.easymock.EasyMock.anyObject;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.expectLastCall;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.sql.BatchUpdateException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;
import java.util.concurrent.CompletableFuture;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.data.Timestamp;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.errors.RetriableException;
import org.apache.kafka.connect.sink.ErrantRecordReporter;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTaskContext;
import org.easymock.EasyMockSupport;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import io.confluent.connect.jdbc.util.DateTimeUtils;

public class JdbcSinkTaskTest extends EasyMockSupport {
  private final SqliteHelper sqliteHelper = new SqliteHelper(getClass().getSimpleName());
  private final JdbcDbWriter mockWriter = createMock(JdbcDbWriter.class);
  private final SinkTaskContext ctx = createMock(SinkTaskContext.class);

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
    TimeZone timeZone = TimeZone.getTimeZone(timeZoneID);
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
                    DateTimeUtils.getTimeZoneCalendar(timeZone)
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
                    DateTimeUtils.getTimeZoneCalendar(TimeZone.getTimeZone(ZoneOffset.UTC))
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
    SQLException chainedException = new SQLException("cause 1");
    chainedException.setNextException(new SQLException("cause 2"));
    chainedException.setNextException(new SQLException("cause 3"));
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

    try {
      task.put(records);
      fail();
    } catch (RetriableException expected) {
      assertEquals(SQLException.class, expected.getCause().getClass());
      int i = 0;
      for (Throwable t : (SQLException) expected.getCause()) {
        ++i;
        StringWriter sw = new StringWriter();
        t.printStackTrace(new PrintWriter(sw));
        System.out.println("Chained exception " + i + ": " + sw);
      }
    }

    try {
      task.put(records);
      fail();
    } catch (RetriableException expected) {
      assertEquals(SQLException.class, expected.getCause().getClass());
      int i = 0;
      for (Throwable t : (SQLException) expected.getCause()) {
        ++i;
        StringWriter sw = new StringWriter();
        t.printStackTrace(new PrintWriter(sw));
        System.out.println("Chained exception " + i + ": " + sw);
      }
    }

    try {
      task.put(records);
      fail();
    } catch (RetriableException e) {
      fail("Non-retriable exception expected");
    } catch (ConnectException expected) {
      assertEquals(SQLException.class, expected.getCause().getClass());
      int i = 0;
      for (Throwable t : (SQLException) expected.getCause()) {
        ++i;
        StringWriter sw = new StringWriter();
        t.printStackTrace(new PrintWriter(sw));
        System.out.println("Chained exception " + i + ": " + sw);
      }
    }

    verifyAll();
  }

  @Test
  public void errorReporting() throws SQLException {
    List<SinkRecord> records = createRecordsList(1);

    mockWriter.write(records);
    SQLException exception = new SQLException("cause 1");
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
  public void testGetAllMessagesExceptionWithoutTrim() throws NoSuchMethodException, InvocationTargetException, IllegalAccessException {
    JdbcSinkTask task = new JdbcSinkTask();
    SQLException exception = new BatchUpdateException("Batch entry 0 INSERT INTO \"abc\" (\"c1\",\"c2\",\"c3\",\"c4\") " +
            "VALUES ('1','2','3',NULL) was aborted: ERROR: null value in column \"c4\" violates not-null constraint\n" +
            "  Detail: Failing row contains (1, 2, 3, null).  Call getNextException to see other errors in the batch.",
            new int[0]);
    String exceptedExceptionMessage = "Exception chain:" + System.lineSeparator() + exception + System.lineSeparator();
    Method privateMethod = JdbcSinkTask.class.getDeclaredMethod("getAllMessagesException", SQLException.class);
    task.shouldTrimSensitiveLogs = false;
    privateMethod.setAccessible(true);

    SQLException result = (SQLException) privateMethod.invoke(task, exception);
    assertEquals(exceptedExceptionMessage, result.getMessage());

    privateMethod.setAccessible(false);
  }

  @Test
  public void testGetAllMessagesExceptionTrimmed() throws NoSuchMethodException, InvocationTargetException, IllegalAccessException {
    JdbcSinkTask task = new JdbcSinkTask();
    SQLException exception = new BatchUpdateException("Batch entry 0 INSERT INTO \"abc\" (\"c1\",\"c2\",\"c3\",\"c4\") " +
            "VALUES ('1','2','3',NULL) was aborted: ERROR: null value in column \"c4\" violates not-null constraint\n" +
            "  Detail: Failing row contains (1, 2, 3, null).  Call getNextException to see other errors in the batch.",
            new int[0]);
    Method privateMethod = JdbcSinkTask.class.getDeclaredMethod("getAllMessagesException", SQLException.class);
    privateMethod.setAccessible(true);
    task.shouldTrimSensitiveLogs = true;

    SQLException result = (SQLException) privateMethod.invoke(task, exception);
    assertTrue(!result.getLocalizedMessage().contains("VALUES"));

    privateMethod.setAccessible(false);
  }
  private List<SinkRecord> createRecordsList(int batchSize) {
    List<SinkRecord> records = new ArrayList<>();
    for (int i = 0; i < batchSize; i++) {
      records.add(RECORD);
    }
    return records;
  }

  @Test
  public void putWithMultipleTableRoutingWithPkModeKafka() throws Exception {
    Map<String, String> props = new HashMap<>();
    props.put("connection.url", sqliteHelper.sqliteUri());
    props.put(TABLE_NAME_FORMAT, TABLE_NAME_FORMAT_RECORD_HEADER);
    props.put("auto.create", "true");
    props.put("pk.mode", "kafka");
    props.put("pk.fields", "kafka_topic,kafka_partition,kafka_offset");

    JdbcSinkTask task = new JdbcSinkTask();
    task.initialize(mock(SinkTaskContext.class));
    task.start(props);

    final Struct struct1 = new Struct(SCHEMA)
        .put("firstName", "Alice")
        .put("lastName", "Johnson")
        .put("age", 28)
        .put("modified", new Date(1474661402123L));

    final Struct struct2 = new Struct(SCHEMA)
        .put("firstName", "Bob")
        .put("lastName", "Williams")
        .put("age", 35)
        .put("modified", new Date(1474661402123L));

    final String topic = "source_topic";

    // Create records with different target tables
    SinkRecord record1 = new SinkRecord(topic, 1, null, null, SCHEMA, struct1, 44);
    record1.headers().add(TABLE_NAME_FORMAT, new SchemaAndValue(Schema.STRING_SCHEMA, "users"));

    SinkRecord record2 = new SinkRecord(topic, 1, null, null, SCHEMA, struct2, 45);
    record2.headers().add(TABLE_NAME_FORMAT, new SchemaAndValue(Schema.STRING_SCHEMA, "employees"));

    List<SinkRecord> records = new ArrayList<>();
    records.add(record1);
    records.add(record2);

    task.put(records);

    // Verify first record went to 'users' table
    assertEquals(
        1,
        sqliteHelper.select(
            "SELECT * FROM users",
            new SqliteHelper.ResultSetReadCallback() {
              @Override
              public void read(ResultSet rs) throws SQLException {
                assertEquals(struct1.getString("firstName"), rs.getString("firstName"));
                assertEquals(struct1.getString("lastName"), rs.getString("lastName"));
                assertEquals(44, rs.getLong("kafka_offset"));
              }
            }
        )
    );

    // Verify second record went to 'employees' table
    assertEquals(
        1,
        sqliteHelper.select(
            "SELECT * FROM employees",
            new SqliteHelper.ResultSetReadCallback() {
              @Override
              public void read(ResultSet rs) throws SQLException {
                assertEquals(struct2.getString("firstName"), rs.getString("firstName"));
                assertEquals(struct2.getString("lastName"), rs.getString("lastName"));
                assertEquals(45, rs.getLong("kafka_offset"));
              }
            }
        )
    );
  }

  @Test
  public void putWithMultipleTableRoutingWithPkModeRecordKey() throws Exception {
    Map<String, String> props = new HashMap<>();
    props.put("connection.url", sqliteHelper.sqliteUri());
    props.put(TABLE_NAME_FORMAT, TABLE_NAME_FORMAT_RECORD_HEADER);
    props.put("auto.create", "true");
    props.put("pk.mode", "record_key");
    props.put("pk.fields", ""); // Empty for record_key mode

    JdbcSinkTask task = new JdbcSinkTask();
    task.initialize(mock(SinkTaskContext.class));
    task.start(props);

    final Struct struct1 = new Struct(SCHEMA)
            .put("firstName", "Alice")
            .put("lastName", "Johnson")
            .put("age", 28)
            .put("modified", new Date(1474661402123L));

    final Struct struct2 = new Struct(SCHEMA)
            .put("firstName", "Bob")
            .put("lastName", "Williams")
            .put("age", 35)
            .put("modified", new Date(1474661402123L));

    final String topic = "source_topic";

    // Define key schemas for record keys
    final Schema keySchema = SchemaBuilder.struct()
            .field("id", Schema.INT32_SCHEMA)
            .field("type", Schema.STRING_SCHEMA)
            .build();

    // Create record keys
    final Struct key1 = new Struct(keySchema)
            .put("id", 1001)
            .put("type", "user");

    final Struct key2 = new Struct(keySchema)
            .put("id", 2001)
            .put("type", "employee");

    // Create records with record keys and different target tables
    SinkRecord record1 = new SinkRecord(topic, 1, keySchema, key1, SCHEMA, struct1, 44);
    record1.headers().add(TABLE_NAME_FORMAT, new SchemaAndValue(Schema.STRING_SCHEMA, "users"));

    SinkRecord record2 = new SinkRecord(topic, 1, keySchema, key2, SCHEMA, struct2, 45);
    record2.headers().add(TABLE_NAME_FORMAT, new SchemaAndValue(Schema.STRING_SCHEMA, "employees"));

    List<SinkRecord> records = new ArrayList<>();
    records.add(record1);
    records.add(record2);

    task.put(records);

    // Verify first record went to 'users' table with record key as PK
    assertEquals(
            1,
            sqliteHelper.select(
                    "SELECT * FROM users",
                    new SqliteHelper.ResultSetReadCallback() {
                      @Override
                      public void read(ResultSet rs) throws SQLException {
                        // Verify data fields
                        assertEquals(struct1.getString("firstName"), rs.getString("firstName"));
                        assertEquals(struct1.getString("lastName"), rs.getString("lastName"));
                        assertEquals(struct1.getInt32("age").intValue(), rs.getInt("age"));

                        // Verify record key fields are used as primary key
                        assertEquals(key1.getInt32("id").intValue(), rs.getInt("id"));
                        assertEquals(key1.getString("type"), rs.getString("type"));
                      }
                    }
            )
    );

    // Verify second record went to 'employees' table with record key as PK
    assertEquals(
            1,
            sqliteHelper.select(
                    "SELECT * FROM employees",
                    new SqliteHelper.ResultSetReadCallback() {
                      @Override
                      public void read(ResultSet rs) throws SQLException {
                        // Verify data fields
                        assertEquals(struct2.getString("firstName"), rs.getString("firstName"));
                        assertEquals(struct2.getString("lastName"), rs.getString("lastName"));
                        assertEquals(struct2.getInt32("age").intValue(), rs.getInt("age"));

                        // Verify record key fields are used as primary key
                        assertEquals(key2.getInt32("id").intValue(), rs.getInt("id"));
                        assertEquals(key2.getString("type"), rs.getString("type"));
                      }
                    }
            )
    );
  }

  @Test
  public void putWithInvalidHeaderShouldFail() throws ConnectException {
    Map<String, String> props = new HashMap<>();
    props.put("connection.url", sqliteHelper.sqliteUri());
    props.put(TABLE_NAME_FORMAT, TABLE_NAME_FORMAT_RECORD_HEADER);
    props.put("auto.create", "true");

    JdbcSinkTask task = new JdbcSinkTask();
    task.initialize(mock(SinkTaskContext.class));
    task.start(props);

    final Struct struct = new Struct(SCHEMA)
            .put("firstName", "Test")
            .put("lastName", "User")
            .put("age", 30)
            .put("modified", new Date(1474661402123L));

    // Test case 1: Missing header
    SinkRecord recordWithoutHeader = new SinkRecord("source_topic", 1, null, null, SCHEMA, struct, 46);
    try {
      task.put(Collections.singleton(recordWithoutHeader));
      fail("Expected ConnectException for missing header");
    } catch (ConnectException e) {
      assertTrue("Exception should mention header issue", e.getMessage().contains("Header 'table.name.format'"));
    }

    // Test case 2: Empty header value
    SinkRecord recordWithEmptyHeader = new SinkRecord("source_topic", 1, null, null, SCHEMA, struct, 47);
    recordWithEmptyHeader.headers().add(TABLE_NAME_FORMAT, new SchemaAndValue(Schema.STRING_SCHEMA, ""));
    try {
      task.put(Collections.singleton(recordWithEmptyHeader));
      fail("Expected ConnectException for empty header value");
    } catch (ConnectException e) {
      assertTrue("Exception should mention header issue", e.getMessage().contains("Header 'table.name.format'"));
    }
  }

  private Map<String, String> setupBasicProps(int maxRetries, long retryBackoffMs) {
    Map<String, String> props = new HashMap<>();
    props.put(JdbcSinkConfig.CONNECTION_URL, "stub");
    props.put(JdbcSinkConfig.MAX_RETRIES, String.valueOf(maxRetries));
    props.put(JdbcSinkConfig.RETRY_BACKOFF_MS, String.valueOf(retryBackoffMs));
    return props;
  }
}
