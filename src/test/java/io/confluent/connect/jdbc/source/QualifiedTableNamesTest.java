package io.confluent.connect.jdbc.source;

import static io.confluent.connect.jdbc.source.JdbcSourceConnectorConfig.INCREMENTING_COLUMN_NAME_CONFIG;
import static io.confluent.connect.jdbc.source.JdbcSourceConnectorConfig.INCREMENTING_COLUMN_NAME_QUALIFIER_CONFIG;
import static io.confluent.connect.jdbc.source.JdbcSourceConnectorConfig.MODE_CONFIG;
import static io.confluent.connect.jdbc.source.JdbcSourceConnectorConfig.QUERY_CONFIG;
import static io.confluent.connect.jdbc.source.JdbcSourceTaskConfig.TABLES_CONFIG;
import static org.junit.Assert.assertEquals;

import java.sql.SQLException;
import java.util.Collections;
import java.util.Map;

import org.junit.Test;
import org.powermock.api.easymock.PowerMock;

/**
 * Tests to verify that a qualified column name can be used for incremental and timestamp columns.
 * <p/>
 * This is required to support queries with joins that may have colliding column names within the
 * query and so must be qualified via the table name in the FROM clause.
 */
public class QualifiedTableNamesTest extends JdbcSourceTaskTestBase {

  @Test
  public void incrementColumnCollisionsInCustomQueryAreHandledByQualifyingTheColumns() throws SQLException, InterruptedException {
    expectInitializeNoOffsets(Collections.singletonList(JOIN_QUERY_PARTITION));

    PowerMock.replayAll();

    // given
    final int expectedRecordCount = 10;
    initialiseAndFeedTable("ROOT_TABLE", "ID", expectedRecordCount);
    initialiseAndFeedTable("CHILD_TABLE", "ID", expectedRecordCount);

    // when we're using a join query with colliding incrementing column names
    String query =
        "select RT.ID " +
            " from ROOT_TABLE RT " +
            " inner join CHILD_TABLE CT on RT.ID = CT.ID ";

    // and we qualify the incrementing column
    JdbcSourceTask sourceTask = startIncrementingQuerySourceTask("ID", "RT", query);

    // then the task can find the appropriate incrementing column
    assertEquals(expectedRecordCount, sourceTask.poll().size());
  }

  private JdbcSourceTask startIncrementingQuerySourceTask(String incrementingColumnName,
                                                          String incrementingColumnNameQualifier,
                                                          String query) {
    final Map<String, String> taskConfig = singleTableConfig();

    // and we're querying in incrementing mode
    taskConfig.put(MODE_CONFIG, "incrementing");
    taskConfig.put(QUERY_CONFIG, query);
    taskConfig.put(TABLES_CONFIG, "");
    taskConfig.put(INCREMENTING_COLUMN_NAME_CONFIG, incrementingColumnName);
    taskConfig.put(INCREMENTING_COLUMN_NAME_QUALIFIER_CONFIG, incrementingColumnNameQualifier);

    // and we initialise and start the task
    JdbcSourceTask sourceTask = new JdbcSourceTask();
    sourceTask.initialize(taskContext);
    sourceTask.start(taskConfig);

    return sourceTask;
  }

  private void initialiseAndFeedTable(String tableName, String incrementingColumn, long rowCount)
      throws SQLException {

    // Need extra column to be able to insert anything, extra is ignored.
    String extraColumn = "EXTRA";
    db.createTable(tableName,
        incrementingColumn, "INT NOT NULL GENERATED ALWAYS AS IDENTITY",
        extraColumn, "VARCHAR(20)");

    for (int i = 0; i < rowCount; i++) {
      db.insert(tableName, extraColumn, "unimportant data");
    }
  }
}
