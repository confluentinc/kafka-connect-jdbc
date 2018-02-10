package io.confluent.connect.jdbc.source;

import static io.confluent.connect.jdbc.source.JdbcSourceConnectorConfig.MODE_CONFIG;
import static io.confluent.connect.jdbc.source.JdbcSourceConnectorConfig.QUERY_CONFIG;
import static io.confluent.connect.jdbc.source.JdbcSourceConnectorConfig.TIMESTAMP_COLUMN_NAME_CONFIG;
import static io.confluent.connect.jdbc.source.JdbcSourceConnectorConfig.TIMESTAMP_COLUMN_NAME_QUALIFIER_CONFIG;
import static io.confluent.connect.jdbc.source.JdbcSourceTaskConfig.TABLES_CONFIG;
import static io.confluent.connect.jdbc.source.TimestampUtils.newTimestamp;
import static io.confluent.connect.jdbc.source.TimestampUtils.plusDays;
import static org.junit.Assert.assertEquals;

import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.Collections;
import java.util.Map;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;
import org.junit.runner.RunWith;
import org.powermock.api.easymock.PowerMock;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

/**
 * Tests to verify that a qualified column name can be used for incremental and timestamp columns.
 * <p/>
 * This is required to support queries with joins that may have colliding column names within the
 * query and so must be qualified via the table name in the FROM clause.
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest({JdbcSourceTask.class})
@PowerMockIgnore("javax.management.*")
public class QualifiedTimestampColumnNameTest extends JdbcSourceTaskTestBase {

  @Rule
  public Timeout timelimit = Timeout.seconds(30);

  @Test
  public void timestampColumnCollisionsInCustomQueryAreHandledByQualifyingTheColumns()
      throws SQLException, InterruptedException {

    // given a table structure with some data that can be joined but has colliding
    // column names when combined in a query
    final int totalRowCount = 10;
    final Timestamp startTimestamp = newTimestamp("2018-01-20T15:32:46.000", 0);
    final IdTimestampTuple[] tuples = new IdTimestampTuple[totalRowCount];
    for (int i = 0; i < totalRowCount; i++) {
      tuples[i] = new IdTimestampTuple(1000 + i, plusDays(startTimestamp, i + 1, 0));
    }
    final String incrementingColumn = "ID";
    final String timestampColumn = "MODIFIED";
    initialiseAndFeedTable("ROOT_TABLE", incrementingColumn, timestampColumn, tuples);
    initialiseAndFeedTable("CHILD_TABLE", incrementingColumn, timestampColumn, tuples);

    // when we're using a join query with colliding incrementing column names
    String query =
        "select RT.ID, RT.MODIFIED " +
            " from ROOT_TABLE RT " +
            " inner join CHILD_TABLE CT on RT.ID = CT.ID ";

    // and we qualify the timestamp column
    JdbcSourceTask sourceTask = startTimestampQuerySourceTask(timestampColumn, "RT", query);

    // then the task can find the appropriate timestamp column
    assertEquals(totalRowCount, sourceTask.poll().size());
  }

  private JdbcSourceTask startTimestampQuerySourceTask(String timestampColumnName,
                                                       String timestampColumnQualifier,
                                                       String query) {
    final Map<String, String> taskConfig = singleTableConfig();

    // and we're querying in incrementing mode
    taskConfig.put(MODE_CONFIG, "timestamp");
    taskConfig.put(QUERY_CONFIG, query);
    taskConfig.put(TABLES_CONFIG, "");
    taskConfig.put(TIMESTAMP_COLUMN_NAME_CONFIG, timestampColumnName);
    taskConfig.put(TIMESTAMP_COLUMN_NAME_QUALIFIER_CONFIG, timestampColumnQualifier);

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

    expectInitializeNoOffsets(Collections.singletonList(JOIN_QUERY_PARTITION));

    PowerMock.replayAll();
  }
}
