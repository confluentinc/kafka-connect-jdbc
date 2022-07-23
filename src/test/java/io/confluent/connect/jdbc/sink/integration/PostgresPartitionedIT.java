package io.confluent.connect.jdbc.sink.integration;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Calendar;
import java.util.Collections;
import java.util.Map;
import java.util.TimeZone;
import java.util.concurrent.TimeUnit;

import io.confluent.common.utils.IntegrationTest;
import io.confluent.connect.jdbc.integration.BaseConnectorIT;
import io.confluent.connect.jdbc.sink.JdbcSinkConfig;

import org.apache.kafka.connect.data.Date;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.json.JsonConverter;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.testcontainers.containers.PostgreSQLContainer;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@Category(IntegrationTest.class)
public class PostgresPartitionedIT extends BaseConnectorIT {

  @Rule
  public PostgreSQLContainer<?> postgresql = new PostgreSQLContainer<>("postgres:14-bullseye");

  Connection connection;
  private JsonConverter jsonConverter;
  private Map<String, String> props;

  private static final String KEY = "key";
  private static final String MEASUREMENTS = "measurements";

  @Before
  public void setup() throws SQLException {
    startConnect();

    jsonConverter = jsonConverter();

    props = baseSinkProps();
    props.put(JdbcSinkConfig.CONNECTION_URL, postgresql.getJdbcUrl());
    props.put(JdbcSinkConfig.CONNECTION_USER, postgresql.getUsername());
    props.put(JdbcSinkConfig.CONNECTION_PASSWORD, postgresql.getPassword());
    props.put(JdbcSinkConfig.PK_MODE, "record_value");
    props.put(JdbcSinkConfig.PK_FIELDS, KEY);
    props.put(JdbcSinkConfig.AUTO_CREATE, "false");
    props.put(JdbcSinkConfig.INSERT_MODE, "insert");
    props.put(JdbcSinkConfig.TABLE_TYPES_CONFIG, "PARTITIONED TABLE");
    props.put("topics", MEASUREMENTS);

    // create topic in Kafka
    connect.kafka().createTopic(MEASUREMENTS, 1);

    createPartitionedTableAndPartition();
  }

  private void createPartitionedTableAndPartition() throws SQLException {
    // Create table taken from https://www.postgresql.org/docs/14/ddl-partitioning.html
    connection = DriverManager.getConnection(
        postgresql.getJdbcUrl(),
        postgresql.getUsername(),
        postgresql.getPassword());
    try (Statement s = connection.createStatement()) {
      s.execute("CREATE TABLE " + MEASUREMENTS + " ("
          + "    key             int not null,"
          + "    city_id         int not null,"
          + "    logdate         date not null,"
          + "    peaktemp        int,"
          + "    unitsales       int"
          + ") PARTITION BY RANGE (logdate);");
      s.execute("CREATE TABLE " + MEASUREMENTS + "_y1970"
          + "    PARTITION OF " + MEASUREMENTS
          + "    FOR VALUES FROM ('1970-01-01') TO ('1971-01-01');");
    }
  }

  @After
  public void tearDown() throws SQLException {
    connection.close();
    stopConnect();
  }

  @Test
  public void shouldInsertMeasurement() throws Exception {
    Schema schema = SchemaBuilder.struct()
        .field(KEY, Schema.INT32_SCHEMA)
        .field("city_id", Schema.INT32_SCHEMA)
        .field("logdate", Date.SCHEMA)
        .field("peaktemp", Schema.INT32_SCHEMA)
        .field("unitsales", Schema.INT32_SCHEMA)
        .build();

    java.util.Date logdate = new java.util.Date(0);

    Struct value = new Struct(schema)
        .put(KEY, 1)
        .put("city_id", 2)
        .put("logdate", logdate)
        .put("peaktemp", 4)
        .put("unitsales", 5);

    String connectorName = "jdbc-sink-connector";
    connect.configureConnector(connectorName, props);
    waitForConnectorToStart(connectorName, 1);
    
    produceRecord(schema, value);
    
    waitForCommittedRecords(connectorName, Collections.singleton(MEASUREMENTS), 1, 1, TimeUnit.MINUTES.toMillis(3));

    assertSinkRecordValues(value);
  }

  private void assertSinkRecordValues(Struct value) throws SQLException, Exception {
    try (Statement s = connection.createStatement();
        ResultSet rs = s.executeQuery("SELECT * FROM " + MEASUREMENTS
            + " ORDER BY KEY DESC FETCH FIRST 1 ROWS ONLY")) {
      assertTrue(rs.next());

      assertEquals(value.getInt32(KEY).intValue(), rs.getInt(1));
      assertEquals(value.getInt32("city_id").intValue(), rs.getInt(2));
      Calendar calendar = Calendar.getInstance(TimeZone.getTimeZone("UTC"));
      assertEquals(value.get("logdate"), rs.getDate(3, calendar));
      assertEquals(value.getInt32("peaktemp").intValue(), rs.getInt(4));
      assertEquals(value.getInt32("unitsales").intValue(), rs.getInt(5));
    }
  }

  private void produceRecord(Schema schema, Struct struct) {
    String kafkaValue = new String(jsonConverter.fromConnectData(MEASUREMENTS, schema, struct));
    connect.kafka().produce(MEASUREMENTS, null, kafkaValue);
  }
}
