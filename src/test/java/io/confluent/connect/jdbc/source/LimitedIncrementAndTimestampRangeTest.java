package io.confluent.connect.jdbc.source;

import static io.confluent.connect.jdbc.source.JdbcSourceConnectorConfig.INCREMENTING_COLUMN_NAME_CONFIG;
import static io.confluent.connect.jdbc.source.JdbcSourceConnectorConfig.INCREMENTING_SPAN_MAX_CONFIG;
import static io.confluent.connect.jdbc.source.JdbcSourceConnectorConfig.INCREMENTING_START_CONFIG;
import static io.confluent.connect.jdbc.source.JdbcSourceConnectorConfig.MODE_CONFIG;
import static io.confluent.connect.jdbc.source.JdbcSourceConnectorConfig.TIMESTAMP_COLUMN_NAME_CONFIG;
import static io.confluent.connect.jdbc.source.JdbcSourceConnectorConfig.TIMESTAMP_SPAN_DAYS_MAX_CONFIG;
import static io.confluent.connect.jdbc.source.JdbcSourceConnectorConfig.TIMESTAMP_START_CONFIG;
import static io.confluent.connect.jdbc.source.TimestampUtils.newTimestamp;
import static io.confluent.connect.jdbc.source.TimestampUtils.plusDays;
import static io.confluent.connect.jdbc.util.DateTimeUtils.formatUtcTimestamp;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;
import org.powermock.api.easymock.PowerMock;

public class LimitedIncrementAndTimestampRangeTest extends JdbcSourceTaskTestBase {

  @Rule
  public Timeout timelimit = Timeout.seconds(30);

  @Test
  public void incrementStartSettingIsRespected() throws SQLException, InterruptedException {
    // given we have some data increments that start at 1000 with a fixed timestamp (which forces
    // the first clause to become active when we also set TIMESTAMP_START to the same)
    // note for this to work the timestampStartSettingIsRespected test must be succeeding too.
    final String incrementingColumnName = "id";
    final String timestampColumnName = "modified";
    final int totalRowCount = 5;
    final Timestamp fixedTimestamp = newTimestamp("2018-01-20T15:32:46.000", 0);
    final IdTimestampTuple[] tuples = new IdTimestampTuple[totalRowCount];
    for (int i = 0; i < totalRowCount; i++) {
      tuples[i] = new IdTimestampTuple(1000 + i, fixedTimestamp);
    }
    initialiseAndFeedTable(SINGLE_TABLE_NAME, incrementingColumnName, timestampColumnName, tuples);

    // when we initialise a span limited timestamp+incrementing source task with
    // an increment start of 999 exclusive with enough span to cover the whole test
    // table in one step only when it starts from the increment start setting.

    long maxIncrementSpan = 10;
    JdbcSourceTask sourceTask = startSpanLimitedIncrementingAndTimestampSourceTask(
        incrementingColumnName, timestampColumnName,
        maxIncrementSpan, 10000,
        999, fixedTimestamp);

    // then we will get all rows in the initial poll
    assertEquals("Incrementing Start setting not respected for mode timestamp+incrementing.",
        totalRowCount, sourceTask.poll().size());

    // note that a failure here will most likely result in the timeout rule being triggered
    // rather than an assertion failure.
  }

  @Test
  public void incrementSpanMaxSettingIsRespected() throws SQLException, InterruptedException {
    // given we have some data increments that start at 1000 with a fixed timestamp (which forces
    // the first clause to become active when I also set TIMESTAMP_START to the same)

    final String incrementingColumnName = "id";
    final String timestampColumnName = "modified";
    final Timestamp fixedTimestamp = newTimestamp("2018-01-20T15:32:46.000", 0);
    final IdTimestampTuple[] tuples = new IdTimestampTuple[5];
    for (int i = 0; i < 5; i++) {
      tuples[i] = new IdTimestampTuple(1 + i, fixedTimestamp);
    }
    initialiseAndFeedTable(SINGLE_TABLE_NAME, incrementingColumnName, timestampColumnName, tuples);

    // when we initialise a span limited incrementing source task with
    // a span that will only cover the first 3 rows given a start of 0.
    long maxIncrementSpan = 3;
    JdbcSourceTask sourceTask = startSpanLimitedIncrementingAndTimestampSourceTask(
        incrementingColumnName, timestampColumnName,
        maxIncrementSpan, 10000,
        0, fixedTimestamp);

    // then we will get 3 rows in the initial poll
    assertEquals("Incrementing Span setting not respected for mode timestamp+incrementing.",
        maxIncrementSpan - 1, sourceTask.poll().size());
  }

  @Test
  public void timestampStartSettingIsRespected() throws SQLException, InterruptedException {

    // given we have some arbitrary increments that start at 1000 with
    // a moving stamp starting greater than the TIMESTAMP_START setting

    final String incrementingColumnName = "id";
    final String timestampColumnName = "modified";
    final int totalRowCount = 5;
    final Timestamp startTimestamp = newTimestamp("2018-01-20T15:32:46.000", 0);
    final IdTimestampTuple[] tuples = new IdTimestampTuple[totalRowCount];
    for (int i = 0; i < totalRowCount; i++) {
      tuples[i] = new IdTimestampTuple(1000 + i, plusDays(startTimestamp, i + 1, 0));
    }
    initialiseAndFeedTable(SINGLE_TABLE_NAME, incrementingColumnName, timestampColumnName, tuples);

    // when we initialise a days limited timestamp+increment source task with a start of
    // the day before the first tuple with enough span to cover the whole test table in one step
    // only when it starts from the timestamp start setting. Note that the increment start and
    // span settings are irrelevant in this case.
    long maxTimestampDaySpan = 10;
    JdbcSourceTask sourceTask = startSpanLimitedIncrementingAndTimestampSourceTask(
        incrementingColumnName, timestampColumnName,
        0, maxTimestampDaySpan,
        -1, startTimestamp);

    // then we will get all rows in the initial poll
    assertEquals("Timestamp Start setting not respected for mode timestamp+incrementing.",
        totalRowCount, sourceTask.poll().size());

    // note that a failure here will most likely result in the timeout rule being triggered
    // rather than an assertion failure.
  }

  @Test
  public void timestampSpanMaxDaysSettingIsRespected() throws SQLException, InterruptedException {
    // given we have some arbitrary increments that start at 1000 with
    // a moving stamp starting greater than the TIMESTAMP_START setting

    final String incrementingColumnName = "id";
    final String timestampColumnName = "modified";
    final int totalRowCount = 5;
    final Timestamp startTimestamp = newTimestamp("2018-01-20T15:32:46.000", 0);
    final IdTimestampTuple[] tuples = new IdTimestampTuple[totalRowCount];
    for (int i = 0; i < totalRowCount; i++) {
      tuples[i] = new IdTimestampTuple(1000 + i, plusDays(startTimestamp, i + 1, 0));
    }
    initialiseAndFeedTable(SINGLE_TABLE_NAME, incrementingColumnName, timestampColumnName, tuples);

    // when we initialise a days limited timestamp+increment source task with a start of the day
    // before the first tuple with enough span to cover only the first 3 rows of the test table
    // in one step. Note that the increment start and span settings are irrelevant in this case.
    long maxTimestampDaySpan = 3;
    JdbcSourceTask sourceTask = startSpanLimitedIncrementingAndTimestampSourceTask(
        incrementingColumnName, timestampColumnName,
        0, maxTimestampDaySpan,
        -1, startTimestamp);

    // then we will get all rows in the initial poll
    assertEquals("Timestamp span setting not respected for mode timestamp+incrementing.",
        maxTimestampDaySpan - 1, sourceTask.poll().size());
  }

  @Test
  public void tumblingSpansDoNotMissBoundaryIncrements() throws SQLException, InterruptedException {
    // given we have some data increments that start at 1000 with a fixed timestamp (which forces
    // the first clause to become active when we also set TIMESTAMP_START to the same)
    // note for this to work the timestampStartSettingIsRespected test must be succeeding too.
    final String incrementingColumnName = "id";
    final String timestampColumnName = "modified";
    final int totalRowCount = 10;
    final Timestamp fixedTimestamp = newTimestamp("2018-01-20T15:32:46.000", 0);
    final IdTimestampTuple[] tuples = new IdTimestampTuple[totalRowCount];
    for (int i = 0; i < totalRowCount; i++) {
      tuples[i] = new IdTimestampTuple(1 + i, fixedTimestamp);
    }

    initialiseAndFeedTable(SINGLE_TABLE_NAME, incrementingColumnName, timestampColumnName, tuples);

    // when we initialise a span limited timestamp+incrementing source task with
    // an increment start of 0 and give it insufficient increment span to cover the whole test
    // table in one step

    long maxIncrementSpan = 7;
    JdbcSourceTask sourceTask = startSpanLimitedIncrementingAndTimestampSourceTask(
        incrementingColumnName, timestampColumnName,
        maxIncrementSpan, 10000,
        -1, fixedTimestamp);

    // and poll enough times that we should collected all the rows - without polling too
    // much which would cause a timeout by remaining in progress until some rows turned up
    Set<Integer> records = new HashSet<>();
    for (int i = 0; i < 2; i++) {
      for (SourceRecord sourceRecord : sourceTask.poll()) {
        records.add((Integer) ((Struct) sourceRecord.value()).get(incrementingColumnName));
      }
    }

    // then we get all rows with no breaks
    for (IdTimestampTuple tuple : tuples) {
      assertTrue("Skipped record " + tuple, records.contains(tuple.id));
    }
  }

  @Test
  public void tumblingSpansDoNotMissBoundaryTimestamps() throws SQLException, InterruptedException {
    // given we have some arbitrary increments that start at 1000 with
    // a moving stamp starting greater than the TIMESTAMP_START setting

    final String incrementingColumnName = "id";
    final String timestampColumnName = "modified";
    final int totalRowCount = 5;
    final Timestamp startTimestamp = newTimestamp("2018-01-20T15:32:46.000", 0);
    final IdTimestampTuple[] tuples = new IdTimestampTuple[totalRowCount];
    for (int i = 0; i < totalRowCount; i++) {
      tuples[i] = new IdTimestampTuple(1000 + i, plusDays(startTimestamp, i + 1, 0));
    }
    initialiseAndFeedTable(SINGLE_TABLE_NAME, incrementingColumnName, timestampColumnName, tuples);

    // when we initialise a days limited timestamp+increment source task with a start of the day
    // before the first tuple with enough span to require several cycles to get all the rows
    long maxTimestampDaySpan = 4;
    JdbcSourceTask sourceTask = startSpanLimitedIncrementingAndTimestampSourceTask(
        incrementingColumnName, timestampColumnName,
        0, maxTimestampDaySpan,
        -1, startTimestamp);

    // and poll enough times that we should collected all the rows - without polling too
    // much which would cause a timeout by remaining in progress until some rows turned up
    Set<Integer> records = new HashSet<>();
    for (int i = 0; i < 2; i++) {
      for (SourceRecord sourceRecord : sourceTask.poll()) {
        records.add((Integer) ((Struct) sourceRecord.value()).get(incrementingColumnName));
      }
    }

    // then we get all rows with no breaks
    for (IdTimestampTuple tuple : tuples) {
      assertTrue("Skipped record " + tuple, records.contains(tuple.id));
    }
  }


  private JdbcSourceTask startSpanLimitedIncrementingAndTimestampSourceTask(
      String incrementingColumnName,
      String timestampColumnName,
      long maxIncrementSpan,
      long maxDaysSpan,
      long incrementStartingFrom,
      Timestamp timestampStartingFrom) {
    final Map<String, String> taskConfig = singleTableConfig();

    // and we're querying in incrementing mode
    taskConfig.put(MODE_CONFIG, "timestamp+incrementing");
    taskConfig.put(INCREMENTING_COLUMN_NAME_CONFIG, incrementingColumnName);
    taskConfig.put(TIMESTAMP_COLUMN_NAME_CONFIG, timestampColumnName);

    // and we limit the span to n
    taskConfig.put(INCREMENTING_SPAN_MAX_CONFIG, String.valueOf(maxIncrementSpan));
    taskConfig.put(INCREMENTING_START_CONFIG, String.valueOf(incrementStartingFrom));


    taskConfig.put(TIMESTAMP_SPAN_DAYS_MAX_CONFIG, String.valueOf(maxDaysSpan));
    taskConfig.put(TIMESTAMP_START_CONFIG,
        formatUtcTimestamp(plusDays(timestampStartingFrom, 0, 0)));


    // and we initialise and start the task
    JdbcSourceTask sourceTask = new JdbcSourceTask();
    sourceTask.initialize(taskContext);
    sourceTask.start(taskConfig);

    return sourceTask;
  }



  @Before
  @Override
  public void setup() throws Exception {
    super.setup();

    expectInitializeNoOffsets(Collections.singletonList(SINGLE_TABLE_PARTITION));

    PowerMock.replayAll();
  }


}
