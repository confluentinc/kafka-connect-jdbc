package io.confluent.connect.jdbc.source.integration;

import io.confluent.common.utils.IntegrationTest;
import io.confluent.connect.jdbc.source.JdbcSourceConnectorConfig;
import io.confluent.connect.jdbc.source.JdbcSourceTask;
import io.confluent.connect.jdbc.source.JdbcSourceTaskConfig;
import io.confluent.connect.jdbc.util.MapUtils;
import io.zonky.test.db.postgres.junit.EmbeddedPostgresRules;
import io.zonky.test.db.postgres.junit.SingleInstancePostgresRule;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Integration test for Postgres OOM conditions.
 */
@Category(IntegrationTest.class)
public class PostgresIT {

  public Map<String, String> props;
  public JdbcSourceTask task;

  private static Logger log = LoggerFactory.getLogger(PostgresIT.class);

  @Rule
  public SingleInstancePostgresRule pg = EmbeddedPostgresRules.singleInstance();

  @Before
  public void before() {
    props = new HashMap<>();
    String jdbcURL = String
        .format("jdbc:postgresql://localhost:%s/postgres", pg.getEmbeddedPostgres().getPort());
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
  public void stopTask() {
    if (task != null) {
      task.stop();
    }
  }

  @Test
  public void test() throws InterruptedException, SQLException {
    createTestTable();
    props.put(JdbcSourceTaskConfig.TABLES_CONFIG, "test_table");
    props.put(JdbcSourceTaskConfig.TABLES_FETCHED, "true");
    startTask();
    List<SourceRecord> records = task.poll();
    List<Map<String, Object>> actual = records.stream()
            .map(ConnectRecord::value)
            .map(Struct.class::cast)
            .map(s -> s.schema()
                    .fields()
                    .stream()
                    .map(f -> Pair.of(f.name(), s.get(f)))
                    .collect(Collectors.toMap(Pair::getKey, Pair::getValue)))
            .collect(Collectors.toList());
    List<Map<String, Object>> expected = Arrays.asList(
            MapUtils.of("c1", "Hello World", "c2", "FOO", "c3", 1),
            MapUtils.of("c1", "Hello World 1", "c2", "BAR", "c3", 1),
            MapUtils.of("c1", "Hello World 2", "c2", "BAZ", "c3", 2)
    );
    Assert.assertEquals(expected, actual);
    task.stop();
  }

  private void createTestTable() throws SQLException {
    log.info("Creating test table");
    try (Connection c = pg.getEmbeddedPostgres().getPostgresDatabase().getConnection()) {
      try (Statement s = c.createStatement()) {
        s.execute("CREATE TABLE test_table ( c1 text, c2 text NOT NULL DEFAULT 'FOO', c3 int NOT NULL DEFAULT 1 )");
        s.execute("INSERT INTO test_table VALUES ( 'Hello World' )");
        s.execute("INSERT INTO test_table VALUES ( 'Hello World 1', 'BAR' )");
        s.execute("INSERT INTO test_table VALUES ( 'Hello World 2', 'BAZ', 2 )");
      }
    }
    log.info("Created table");
  }
}
