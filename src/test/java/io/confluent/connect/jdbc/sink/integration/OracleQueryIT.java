package io.confluent.connect.jdbc.sink.integration;

import io.confluent.common.utils.IntegrationTest;
import io.confluent.connect.jdbc.integration.BaseConnectorIT;
import io.confluent.connect.jdbc.sink.JdbcSinkConfig;
import org.apache.kafka.connect.data.Date;
import org.apache.kafka.connect.data.Decimal;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.data.Time;
import org.apache.kafka.connect.data.Timestamp;
import org.apache.kafka.connect.json.JsonConverter;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.testcontainers.containers.OracleContainer;
import org.testcontainers.utility.ThrowingFunction;

import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.ZoneId;
import java.util.Calendar;
import java.util.Collections;
import java.util.Map;
import java.util.TimeZone;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

@Category(IntegrationTest.class)
public class OracleQueryIT extends BaseConnectorIT {
    @SuppressWarnings( "deprecation" )
    @Rule
    public OracleContainer oracle = new OracleContainer();

    Connection connection;
    private JsonConverter jsonConverter;
    private Map<String, String> props;
    private String tableName;

    @Before
    public void setup() throws SQLException {
        startConnect();

        jsonConverter = jsonConverter();
        props = baseSinkProps();

        tableName = "TEST";
        props.put(JdbcSinkConfig.CONNECTION_URL, oracle.getJdbcUrl());
        props.put(JdbcSinkConfig.CONNECTION_USER, oracle.getUsername());
        props.put(JdbcSinkConfig.CONNECTION_PASSWORD, oracle.getPassword());
        props.put(JdbcSinkConfig.PK_MODE, "record_value");
        props.put(JdbcSinkConfig.PK_FIELDS, "KEY");
        props.put(JdbcSinkConfig.AUTO_CREATE, "false");
        props.put(JdbcSinkConfig.MAX_RETRIES, "0");
        props.put("topics", tableName);

        // create topic in Kafka
        connect.kafka().createTopic(tableName, 1);

        connection = DriverManager.getConnection(oracle.getJdbcUrl(),
            oracle.getUsername(), oracle.getPassword());
    }

    @After
    public void tearDown() throws SQLException {
        connection.close();

        stopConnect();
    }

    @Test
    public void testQuoteIdentifierNeverConfig() throws Exception {
        String mixedCaseTopicName = "TestTopic";

        connect.kafka().createTopic(mixedCaseTopicName, 1);

        props.put(JdbcSinkConfig.AUTO_CREATE, "true");
        props.put("topics", mixedCaseTopicName);
        props.put(JdbcSinkConfig.QUERY, "INSERT INTO person(key, firstname, lastname) values " +
                "($.value.key, $.value.firstname, $.value.lastname)");
        connect.configureConnector("jdbc-sink-connector", props);

        waitForConnectorToStart("jdbc-sink-connector", 1);

        final Schema schema = SchemaBuilder.struct().name("com.example.Person")
            .field("firstname", Schema.STRING_SCHEMA)
            .field("lastname", Schema.STRING_SCHEMA)
            .field("KEY", Schema.INT32_SCHEMA)
            .build();
        final Struct struct = new Struct(schema)
            .put("firstname", "Christina")
            .put("lastname", "Brams")
            .put("KEY", 1);

        String kafkaValue = new String(jsonConverter.fromConnectData(mixedCaseTopicName, schema, struct));
        connect.kafka().produce(mixedCaseTopicName, null, kafkaValue);

        waitForCommittedRecords("jdbc-sink-connector", Collections.singleton(mixedCaseTopicName), 1, 1,
            TimeUnit.MINUTES.toMillis(2));

        String autoCreatedTableName = mixedCaseTopicName.toUpperCase();
        try (Statement s = connection.createStatement()) {
            ResultSet rs = s.executeQuery(
                String.format("SELECT * FROM \"%s\" ORDER BY KEY DESC FETCH FIRST 1 ROWS ONLY", autoCreatedTableName));
            assertTrue(rs.next());
            assertEquals(struct.getString("firstname"), rs.getString("firstname"));
            assertEquals(struct.getString("lastname"), rs.getString("lastname"));
        }
    }

    private void assertProduced(
        Schema schema, Struct value, ThrowingFunction<ResultSet, Void> assertion) throws Exception {

        connect.configureConnector("jdbc-sink-connector", props);
        waitForConnectorToStart("jdbc-sink-connector", 1);

        produceRecord(schema, value);

        waitForCommittedRecords("jdbc-sink-connector", Collections.singleton(tableName),
            1, 1,
            TimeUnit.MINUTES.toMillis(3));

        try (Statement s = connection.createStatement()) {
            ResultSet rs = s.executeQuery(
                "SELECT * FROM " + tableName + " ORDER BY KEY DESC FETCH FIRST 1 ROWS ONLY");
            assertTrue(rs.next());
            assertion.apply(rs);
        }
    }

    private void produceRecord(Schema schema, Struct struct) {
        String kafkaValue = new String(jsonConverter.fromConnectData(tableName, schema, struct));
        connect.kafka().produce(tableName, null, kafkaValue);
    }
}
