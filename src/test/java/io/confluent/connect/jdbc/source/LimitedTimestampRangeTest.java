package io.confluent.connect.jdbc.source;

import static io.confluent.connect.jdbc.source.JdbcSourceConnectorConfig.MODE_CONFIG;
import static io.confluent.connect.jdbc.source.JdbcSourceConnectorConfig.TIMESTAMP_COLUMN_NAME_CONFIG;
import static io.confluent.connect.jdbc.source.JdbcSourceConnectorConfig.TIMESTAMP_SPAN_DAYS_MAX_CONFIG;
import static io.confluent.connect.jdbc.source.JdbcSourceConnectorConfig.TIMESTAMP_START_CONFIG;
import static io.confluent.connect.jdbc.util.DateTimeUtils.formatUtcTimestamp;
import static org.junit.Assert.assertEquals;

import io.confluent.connect.jdbc.util.DateTimeUtils;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;

import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.Before;
import org.junit.Test;
import org.powermock.api.easymock.PowerMock;

public class LimitedTimestampRangeTest extends JdbcSourceTaskTestBase {

  @Before
  @Override
  public void setup() throws Exception {
    super.setup();

    expectInitializeNoOffsets(Collections.singletonList(SINGLE_TABLE_PARTITION));

    PowerMock.replayAll();
  }

  @Test
  public void startTimeStampConfigIsRespected() throws SQLException, InterruptedException {

    // given we have a timestamp row in the database
    final String timestampColumnName = "MODIFIED";
    final int totalRowCount = 1;

    final String startingFrom = "2018-01-20T15:23:46";
    final int withNanos = 123456;
    final Timestamp[] timestamps = getTimestamps(startingFrom, withNanos, totalRowCount, 1, 0);
    initialiseAndFeedTable(SINGLE_TABLE_NAME, timestampColumnName, timestamps);

    // when we initialise a span limited incrementing source task with a startTime that falls
    // within a single span of the row in the database
    long maxTimestampSpan = 2;
    JdbcSourceTask sourceTask =
        startSpanLimitedIncrementingSourceTask(timestampColumnName, maxTimestampSpan,
            plusDays(timestamps[0], -1, 0));

    // then we should get that row in a single poll cycle
    assertEquals("Failed to respect timstampStart config setting.",
        totalRowCount, sourceTask.poll().size());
  }

  @Test
  public void maxTimespanDaysSettingIsRespected() throws SQLException, InterruptedException {

    // given we're at first start and have a 'totalRowCount' row table with even increments of 1
    final String timestampColumnName = "MODIFIED";
    final int totalRowCount = 10;

    final String startingFrom = "2018-01-20T15:23:46";
    final int withNanos = 123456;
    final Timestamp[] timestamps = getTimestamps(startingFrom, withNanos, totalRowCount, 1, 0);
    initialiseAndFeedTable(SINGLE_TABLE_NAME, timestampColumnName, timestamps);

    // when we initialise a span limited incrementing source task
    long maxTimestampSpan = 4;
    final int startTimeAdjustment = -1;
    JdbcSourceTask sourceTask =
        startSpanLimitedIncrementingSourceTask(timestampColumnName, maxTimestampSpan, plusDays(timestamps[0], startTimeAdjustment, 0));

    // we get 4 rows in the first poll after adjusting the start timestamp to the day before
    // the earliest timestamp
    final List<SourceRecord> poll1 = sourceTask.poll();
    assertEquals(maxTimestampSpan, poll1.size());

    // then 4 rows each subsequent poll
    final List<SourceRecord> poll2 = sourceTask.poll();
    assertEquals(maxTimestampSpan, poll2.size());

    // until we reach the end and get only what is left
    final List<SourceRecord> poll3 = sourceTask.poll();
    assertEquals(totalRowCount - 2 * maxTimestampSpan, poll3.size());
  }

  /**
   * This test verifies that where a timestamp falls exactly on an offset boundary it is still
   * collected.
   */
  @Test
  public void tumblingSpansDoNotMissBoundaryMoments() throws SQLException, InterruptedException {

    // given we're at first start and have a 'totalRowCount' row table with even increments of 1
    final String timestampColumnName = "MODIFIED";
    final int totalRowCount = 5;

    final String startingFrom = "2018-01-20T15:23:46";
    final int withNanos = 123456;
    final Timestamp[] timestamps = getTimestamps(startingFrom, withNanos, totalRowCount, 1, 0);
    initialiseAndFeedTable(SINGLE_TABLE_NAME, timestampColumnName, timestamps);

    // when we initialise a span limited incrementing source task
    long maxTimestampSpan = 2;
    JdbcSourceTask sourceTask =
        startSpanLimitedIncrementingSourceTask(timestampColumnName, maxTimestampSpan, plusDays(timestamps[0], -1, 0));

    // and we poll enough times to get all the row
    final Set<Object> records = new HashSet<>();
    for (int i = 0; i < 3; i++) {
      final List<SourceRecord> pollResult = sourceTask.poll();
      for (SourceRecord sourceRecord : pollResult) {
        records.add(((Struct) sourceRecord.value()).get(timestampColumnName));
      }
    }

    // then all the rows are collected
    assertEquals("Not all expected records were collected in tumbling timestamp spans.",
        totalRowCount, records.size());
  }

  private Timestamp[] getTimestamps(String isoDateString, int nanos, int howMany, int gap, int maxJitter) {
    Timestamp[] result = new Timestamp[howMany];
    Timestamp startFrom = newTimestamp(isoDateString, nanos);
    result[0] = startFrom;

    for (int i = 1; i < howMany; i++) {
      result[i] = plusDays(startFrom, i, maxJitter);
    }

    return result;
  }

  private JdbcSourceTask startSpanLimitedIncrementingSourceTask(String timestampColumnName,
                                                                long maxDaysSpan,
                                                                Timestamp startingFrom) {
    final Map<String, String> taskConfig = singleTableConfig();

    // and we're querying in incrementing mode
    taskConfig.put(MODE_CONFIG, "timestamp");
    taskConfig.put(TIMESTAMP_COLUMN_NAME_CONFIG, timestampColumnName);

    // and we limit the span to n
    taskConfig.put(TIMESTAMP_SPAN_DAYS_MAX_CONFIG, String.valueOf(maxDaysSpan));
    taskConfig.put(TIMESTAMP_START_CONFIG,
        DateTimeUtils.formatUtcTimestamp(plusDays(startingFrom, 0, 0)));


    // and we initialise and start the task
    JdbcSourceTask sourceTask = new JdbcSourceTask();
    sourceTask.initialize(taskContext);
    sourceTask.start(taskConfig);

    return sourceTask;
  }

  private void initialiseAndFeedTable(String tableName, String timestampColumn, Timestamp... timestamps)
      throws SQLException {

    // Need extra column to be able to insert anything, extra is ignored.
    db.createTable(tableName,
        "ID", "INT NOT NULL GENERATED ALWAYS AS IDENTITY",
        timestampColumn, "TIMESTAMP NOT NULL");

    for (Timestamp timestamp : timestamps) {
      db.insert(tableName, timestampColumn, formatUtcTimestamp(timestamp));
    }
  }

  private Timestamp newTimestamp(String isoDateString, int nanos) {
    try {
      Timestamp result = new Timestamp(new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss").parse(isoDateString).getTime());
      result.setNanos(nanos);

      return result;
    } catch (ParseException e) {
      throw new RuntimeException(e);
    }
  }

  private Timestamp plusDays(Timestamp startingFrom, int days, int maxJitter) {
    Timestamp result = new Timestamp(startingFrom.getTime()
        + (days * 1000 * 3600 * 24)
        - (maxJitter > 0 ? new Random().nextInt(maxJitter) : 0));
    result.setNanos(startingFrom.getNanos());

    return result;
  }
}

