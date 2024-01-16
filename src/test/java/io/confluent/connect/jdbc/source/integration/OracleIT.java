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
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.OracleContainer;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static junit.framework.TestCase.assertTrue;

/**
 * Integration test for Postgres OOM conditions.
 */
@Category(IntegrationTest.class)
public class OracleIT {

  @ClassRule
  public static OracleContainer oracle = new OracleContainer();

  public Map<String, String> props;
  public JdbcSourceTask task;

  private static Logger log = LoggerFactory.getLogger(OracleIT.class);


  @Before
  public void before() {
    props = new HashMap<>();
    props.put(JdbcSourceConnectorConfig.CONNECTION_URL_CONFIG, oracle.getJdbcUrl());
    props.put(JdbcSourceConnectorConfig.CONNECTION_USER_CONFIG, oracle.getUsername());
    props.put(JdbcSourceConnectorConfig.CONNECTION_PASSWORD_CONFIG, oracle.getPassword());
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
            MapUtils.of("c1", "Hello World", "c2", "FOO", "c3", new BigDecimal(1)),
            MapUtils.of("c1", "Hello World 1", "c2", "BAR", "c3", new BigDecimal(1)),
            MapUtils.of("c1", "Hello World 2", "c2", "BAZ", "c3", new BigDecimal(2))
    );
    Assert.assertEquals(expected, actual);
    task.stop();
  }

  private void createTestTable() throws SQLException {
    log.info("Creating test table");
    try (Connection c = oracle.createConnection("")) {
      try (Statement s = c.createStatement()) {
        s.execute("CREATE TABLE \"test_table\"( \"c1\" VARCHAR2(100), " +
                "\"c2\" VARCHAR2(100) DEFAULT 'FOO' NOT NULL, " +
                "\"c3\" NUMBER(10) DEFAULT 1 NOT NULL )");
        s.execute("INSERT INTO \"test_table\"(\"c1\") VALUES ( 'Hello World' )");
        s.execute("INSERT INTO \"test_table\"(\"c1\", \"c2\") VALUES ( 'Hello World 1', 'BAR' )");
        s.execute("INSERT INTO \"test_table\"(\"c1\", \"c2\", \"c3\") VALUES ( 'Hello World 2', 'BAZ', 2 )");
      }
    }
    log.info("Created table");
  }
}
