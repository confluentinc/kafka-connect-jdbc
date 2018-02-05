/**
 * Copyright 2015 Confluent Inc.
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 **/

package io.confluent.connect.jdbc.source;

import static io.confluent.connect.jdbc.source.JdbcSourceConnectorConfig.INCREMENTING_COLUMN_NAME_CONFIG;
import static io.confluent.connect.jdbc.source.JdbcSourceConnectorConfig.INCREMENTING_SPAN_MAX_CONFIG;
import static io.confluent.connect.jdbc.source.JdbcSourceConnectorConfig.INCREMENTING_START_CONFIG;
import static io.confluent.connect.jdbc.source.JdbcSourceConnectorConfig.MODE_CONFIG;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;
import org.powermock.api.easymock.PowerMock;

/**
 * Limited Increment Range means you can limit the number of rows a source will swallow in a single
 * cycle which is important when hitting very large database tables for the first time. Having this
 * requires also having an Incrementing Start setting to prevent unnecessary cycling from the
 * default start point of -1 where increments start with a high number.
 */
public class LimitedIncrementRangeTest extends JdbcSourceTaskTestBase {

  @Rule
  public Timeout timelimit = Timeout.seconds(30);

  @Test
  public void incrementStartConfigSettingIsRespected() throws SQLException, InterruptedException {

    // given we have some data increments that start at 1000
    final String incrementingColumnName = "id";
    final int totalRowCount = 5;
    initialiseAndFeedTable(SINGLE_TABLE_NAME, incrementingColumnName, 1000, 1001, 1002, 1003, 1004);

    // when we initialise a span limited incrementing source task with an increment start of
    // 999 exclusive with enough span to cover the whole test table in one step when it starts
    // in the right place
    long maxIncrementSpan = 10;
    JdbcSourceTask sourceTask = startSpanLimitedIncrementingSourceTask(incrementingColumnName,
        maxIncrementSpan, 999);

    // then we will get all rows in the initial poll
    assertEquals("Incrementing Start setting not respected.",
        totalRowCount, sourceTask.poll().size());
  }


  @Test
  public void maxIncrementSpanConfigSettingIsRespected() throws SQLException, InterruptedException {

    // given we're at first start and have a 'totalRowCount' row table with even increments of 1
    final String incrementingColumnName = "id";
    final int totalRowCount = 11;
    initialiseAndFeedTable(SINGLE_TABLE_NAME, incrementingColumnName, totalRowCount);

    // when we initialise a span limited incrementing source task
    long maxIncrementSpan = 5;
    JdbcSourceTask sourceTask =
        startSpanLimitedIncrementingSourceTask(incrementingColumnName, maxIncrementSpan);

    // then (accounting for initial increment value of -1 and a span of 5)

    // we get 4 rows in the first poll
    long startingPoint = -1;
    long firstCycleRowCount = maxIncrementSpan + startingPoint;
    assertEquals(firstCycleRowCount, sourceTask.poll().size());

    // then 5 rows each subsequent poll
    long secondCycleRowCount = maxIncrementSpan;
    assertEquals(secondCycleRowCount, sourceTask.poll().size());

    // until we reach the end and get only what is left
    long finalCycleRowCount = totalRowCount - secondCycleRowCount - firstCycleRowCount;
    assertEquals(finalCycleRowCount, sourceTask.poll().size());
  }

  @Test
  public void tumblingSpansDoNotMissBoundaryIncrements() throws SQLException, InterruptedException {

    // given we're at first start and have a 'totalRowCount' row table with even increments of 1
    final String incrementingColumnName = "id";
    final int totalRowCount = 10;
    initialiseAndFeedTable(SINGLE_TABLE_NAME, incrementingColumnName, totalRowCount);

    // when we initialise a span limited incrementing source task
    long maxIncrementSpan = 4;
    JdbcSourceTask sourceTask =
        startSpanLimitedIncrementingSourceTask(incrementingColumnName, maxIncrementSpan);

    // and we poll enough times to get all the rows
    final List<Object> records = new ArrayList<>();
    for (int i = 0; i < 4; i++) {
      for (SourceRecord sourceRecord : sourceTask.poll()) {
        records.add(((Struct) sourceRecord.value()).get(incrementingColumnName));
      }
    }

    // then all the rows are collected - i.e. none are missed
    for (int i = 1; i <= totalRowCount; i++) {
      assertTrue("record id '" + i + "' missing.", records.contains(i));
    }
  }


  private JdbcSourceTask startSpanLimitedIncrementingSourceTask(String incrementingColumnName,
                                                                long maxIncrementSpan) {
    return startSpanLimitedIncrementingSourceTask(incrementingColumnName, maxIncrementSpan, -1);
  }

  private JdbcSourceTask startSpanLimitedIncrementingSourceTask(String incrementingColumnName,
                                                                long maxIncrementSpan,
                                                                long incrementStart) {
    final Map<String, String> taskConfig = singleTableConfig();

    // and we're querying in incrementing mode
    taskConfig.put(MODE_CONFIG, "incrementing");
    taskConfig.put(INCREMENTING_COLUMN_NAME_CONFIG, incrementingColumnName);

    // and we limit the span to n
    taskConfig.put(INCREMENTING_SPAN_MAX_CONFIG, String.valueOf(maxIncrementSpan));
    taskConfig.put(INCREMENTING_START_CONFIG, String.valueOf(incrementStart));


    // and we initialise and start the task
    JdbcSourceTask sourceTask = new JdbcSourceTask();
    sourceTask.initialize(taskContext);
    sourceTask.start(taskConfig);

    return sourceTask;
  }

  private void initialiseAndFeedTable(String tableName, String incrementingColumn, int rowCount)
      throws SQLException {
    Integer[] ids = new Integer[rowCount];
    for (int i = 1; i <= rowCount; i++) {
      // emulate the default db identity incrementer which starts at 1 rather than 0
      ids[i - 1] = i;
    }

    initialiseAndFeedTable(tableName, incrementingColumn, ids);
  }

  private void initialiseAndFeedTable(String tableName, String incrementingColumn, Integer... ids)
      throws SQLException {

    // Need extra column to be able to insert anything, extra is ignored.
    String extraColumn = "extra";
    db.createTable(tableName,
        incrementingColumn, "INT NOT NULL",
        extraColumn, "VARCHAR(20)");

    for (Integer id : ids) {
      db.insert(tableName, incrementingColumn, id, extraColumn, "unimportant data");

    }
  }

  @Before
  @Override
  public void setup() throws Exception {
    super.setup();

    expectInitializeNoOffsets(Collections.singletonList(SINGLE_TABLE_PARTITION));

    PowerMock.replayAll();
  }
}

