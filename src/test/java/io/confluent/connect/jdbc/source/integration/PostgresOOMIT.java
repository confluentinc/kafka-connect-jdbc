package io.confluent.connect.jdbc.source.integration;

import static junit.framework.TestCase.assertTrue;
import static org.junit.Assert.assertEquals;

import io.confluent.common.utils.IntegrationTest;
import io.confluent.connect.jdbc.source.JdbcSourceConnectorConfig;
import io.confluent.connect.jdbc.source.JdbcSourceTask;
import io.confluent.connect.jdbc.source.JdbcSourceTaskConfig;
import io.zonky.test.db.postgres.junit.EmbeddedPostgresRules;
import io.zonky.test.db.postgres.junit.SingleInstancePostgresRule;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Integration test for Postgres OOM conditions
 * Expects to be run with -Xmx64M or -Xmx128M for minimum runtime
 * but will behave correctly with any heap size (just with less performance)
 */
@Category(IntegrationTest.class)
public class PostgresOOMIT {

  private static Logger log = LoggerFactory.getLogger(PostgresOOMIT.class);

  public static final long MAX_MEMORY = Runtime.getRuntime().maxMemory();

  public static final int BYTES_PER_ROW = 1024;
  // enough rows to take up the whole heap
  public static final long LARGE_QUERY_ROW_COUNT = MAX_MEMORY / BYTES_PER_ROW;

  public static final String LARGE_QUERY;

  static {
    StringBuilder qb = new StringBuilder();
    qb.append("select");
    qb.append(" '");
      for (int i = 0; i < BYTES_PER_ROW; i++) {
        qb.append('a');
      }
    qb.append("' ");
    qb.append("from generate_series(1, ");
    qb.append(LARGE_QUERY_ROW_COUNT);
    qb.append(") s(i)");
    LARGE_QUERY = qb.toString();
    log.info(
        "Large query will generate "
            + MAX_MEMORY
            + " bytes across "
            + LARGE_QUERY_ROW_COUNT + " rows"
    );
  }

  @Rule
  public SingleInstancePostgresRule pg = EmbeddedPostgresRules.singleInstance();

  public Map<String, String> props;
  public JdbcSourceTask task;

  @Before
  public void before() {
    props = new HashMap<>();
    String jdbcURL = String.format("jdbc:postgresql://localhost:%s/postgres", pg.getEmbeddedPostgres().getPort());
    props.put(JdbcSourceConnectorConfig.CONNECTION_URL_CONFIG, jdbcURL);
    props.put(JdbcSourceConnectorConfig.CONNECTION_USER_CONFIG, "postgres");
    props.put(JdbcSourceConnectorConfig.MODE_CONFIG, JdbcSourceConnectorConfig.MODE_BULK);
    props.put(JdbcSourceTaskConfig.TOPIC_PREFIX_CONFIG, "topic_");
  }

  public void startTask() {

    task = new JdbcSourceTask();
    task.start(props);
  }

  @After
  public void after() {
    if (task != null) {
      task.stop();
    }
  }

  @Test(expected = OutOfMemoryError.class)
  public void assertOutOfMemoryWithLargeBatch() throws InterruptedException {
    props.put(JdbcSourceTaskConfig.TABLES_CONFIG, "");
    props.put(JdbcSourceConnectorConfig.QUERY_CONFIG, LARGE_QUERY);
    props.put(JdbcSourceConnectorConfig.BATCH_MAX_ROWS_CONFIG, Long.toString(LARGE_QUERY_ROW_COUNT));
    startTask();
    task.poll();
  }

  @Test
  public void testStreamingReads() throws InterruptedException {
    props.put(JdbcSourceTaskConfig.TABLES_CONFIG, "");
    props.put(JdbcSourceConnectorConfig.QUERY_CONFIG, LARGE_QUERY);
    startTask();
    assertTrue(task.poll().size() > 0);
  }

  @Test
  public void testTableLocksWithStreamingReads() throws InterruptedException, SQLException {
    createTestTable();
    props.put(JdbcSourceTaskConfig.TABLES_CONFIG, "test_table");
    startTask();
    assertNoLocksOpen(task);
    assertTrue(task.poll().size() > 0);
    assertNoLocksOpen(task);
    task.stop();
    assertNoLocksOpen(task);
  }

  private void createTestTable() throws SQLException {
    log.info("Creating test table");
    try (Connection c = pg.getEmbeddedPostgres().getPostgresDatabase().getConnection()) {
      try (Statement s = c.createStatement()) {
        s.execute("CREATE TABLE test_table ( c1 text )");
        s.execute("INSERT INTO test_table VALUES ( 'Hello World' )");
      }
    }
    log.info("Created table");
  }

  private void assertNoLocksOpen(JdbcSourceTask task) throws SQLException {
    log.info("Checking for orphaned locks");
    int count = 0;
    try (Connection c = pg.getEmbeddedPostgres().getPostgresDatabase().getConnection()) {
      try (Statement s = c.createStatement()) {
        try (ResultSet rs = s.executeQuery(
            "select * from pg_stat_activity where state like 'idle in%'"
        )) {
          int columnCount = rs.getMetaData().getColumnCount();
          StringBuilder header = new StringBuilder();
          for (int i = 1; i <= columnCount; i++) {
            header.append(rs.getMetaData().getColumnName(i));
            header.append("\t");
          }
          log.debug(header.toString());
          while (rs.next()) {
            count++;
            StringBuilder row = new StringBuilder();
            for (int i = 1; i <= columnCount; i++) {
              row.append(rs.getObject(i));
              row.append("\t");
            }
            log.debug(row.toString());
          }
        }
      }
    }
    assertEquals("Found idle locks left open", 0, count);
    log.info("No locks found");
  }
}
