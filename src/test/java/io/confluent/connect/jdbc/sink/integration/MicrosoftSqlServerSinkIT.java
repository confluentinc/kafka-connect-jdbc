package io.confluent.connect.jdbc.sink.integration;

import io.confluent.connect.jdbc.integration.BaseConnectorIT;
import io.confluent.connect.jdbc.sink.JdbcSinkConfig;
import java.sql.ResultSet;
import java.sql.Statement;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.json.JsonConverter;
import org.apache.kafka.connect.storage.StringConverter;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.FixedHostPortGenericContainer;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Duration;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.apache.kafka.connect.runtime.ConnectorConfig.CONNECTOR_CLASS_CONFIG;
import static org.apache.kafka.connect.runtime.ConnectorConfig.KEY_CONVERTER_CLASS_CONFIG;
import static org.apache.kafka.connect.runtime.ConnectorConfig.VALUE_CONVERTER_CLASS_CONFIG;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class MicrosoftSqlServerSinkIT extends BaseConnectorIT {
    private static final Logger log = LoggerFactory.getLogger(MicrosoftSqlServerSinkIT.class);
    private static final String CONNECTOR_NAME = "jdbc-sink-connector";
    private static final String MSSQL_URL = "jdbc:sqlserver://0.0.0.0:1433";
    private Map<String, String> props;
    private Connection connection;
    private JsonConverter jsonConverter;

    private static final String USER = "sa"; // test creds
    private static final String PASS = "reallyStrongPwd123"; // test creds

    @ClassRule
    @SuppressWarnings("deprecation")
    public static final FixedHostPortGenericContainer mssqlServer =
            new FixedHostPortGenericContainer<>("mcr.microsoft.com/mssql/server:2019-latest")
                    .withEnv("ACCEPT_EULA","Y")
                    .withEnv("SA_PASSWORD", PASS)
                    .withFixedExposedPort(1433, 1433);

    @Before
    public void setup() throws Exception {
        //Set up JDBC Driver
        Class.forName("com.microsoft.sqlserver.jdbc.SQLServerDriver");
        connection = DriverManager.getConnection(MSSQL_URL, USER, PASS);
        startConnect();
        jsonConverter =  jsonConverter();
    }

    @After
    public void close() throws SQLException {
        connect.deleteConnector(CONNECTOR_NAME);
        connection.close();
        // Stop all Connect, Kafka and Zk threads.
        stopConnect();
    }

    /**
     * Verify that if a table is specified without a schema/owner
     * and the table schema/owner is not dbo the connector fails
     * with error of form Table is missing and auto-creation is disabled
     */
    @Test
    public void verifyConnectorFailsWhenTableNameS() throws Exception {
        final String table = "example_table";

        // Setup up props for the sink connector
        props = configProperties(table);

        // create table
        String sql = "CREATE TABLE guest." + table
                + " (id int NULL, last_name VARCHAR(50), created_at DATETIME2 NOT NULL);";
        PreparedStatement createStmt = connection.prepareStatement(sql);
        executeSQL(createStmt);

        // Create topic in Kafka
        connect.kafka().createTopic(table, 1);

        // Configure sink connector
        configureAndWaitForConnector(1);

        //create record and produce it
        Timestamp t = Timestamp.from(
                ZonedDateTime.of(2017, 12, 8, 19, 34, 56, 0, ZoneId.of("UTC")).toInstant()
        );
        final Schema schema = SchemaBuilder.struct().name("com.example.Person")
                .field("id", Schema.INT32_SCHEMA)
                .field("last_name", Schema.STRING_SCHEMA)
                .field("created_at", org.apache.kafka.connect.data.Timestamp.SCHEMA)
                .build();
        final Struct struct = new Struct(schema)
                .put("id", 1)
                .put("last_name", "Brams")
                .put("created_at", t);

        String kafkaValue = new String(jsonConverter.fromConnectData(table, schema, struct));
        connect.kafka().produce(table, null, kafkaValue);

        //verify that connector failed because it cannot find the table.
        assertTasksFailedWithTrace(
                CONNECTOR_NAME,
                1,
                "Table \"dbo\".\""
                    + table
                    + "\" is missing and auto-creation is disabled"
        );
    }

    /**
     * Verify that inserting a null BYTES value succeeds.
     */
    @Test
    public void verifyNullBYTESValue() throws Exception {
        final String table = "optional_bytes";

        props = configProperties(table);
        props.put("auto.create", "true");

        connect.kafka().createTopic(table, 1);
        configureAndWaitForConnector(1);

        final Schema schema = SchemaBuilder.struct().name("com.example.OptionalBytes")
                .field("id", Schema.INT32_SCHEMA)
                .field("optional_bytes", Schema.OPTIONAL_BYTES_SCHEMA)
                .build();
        final Struct struct = new Struct(schema)
                .put("id", 1)
                .put("optional_bytes", null);

        String kafkaValue = new String(jsonConverter.fromConnectData(table, schema, struct));
        connect.kafka().produce(table, null, kafkaValue);

        waitForCommittedRecords(CONNECTOR_NAME, Collections.singleton(table), 1, 1, TimeUnit.MINUTES.toMillis(2));
    }

    @Test
    public void verifyUnicodeWorksWhenSendStringParametersAsUnicodeEqualsFalse() throws Exception {
        final String table = "table_with_unicode_fields";

        props = configProperties(table);

        // create table
        String sql = "CREATE TABLE dbo." + table
            + " (id int, unicode_field NVARCHAR(50));";
        PreparedStatement createStmt = connection.prepareStatement(sql);
        executeSQL(createStmt);

        props.put(JdbcSinkConfig.CONNECTION_URL, MSSQL_URL + ";sendStringParametersAsUnicode=false");

        connect.kafka().createTopic(table, 1);
        configureAndWaitForConnector(1);

        final Schema schema = SchemaBuilder.struct().name("com.example.UnicodeField")
            .field("id", Schema.INT32_SCHEMA)
            .field("unicode_field", Schema.OPTIONAL_STRING_SCHEMA)
            .build();
        final Struct struct = new Struct(schema)
            .put("id", 1)
            .put("unicode_field", "एम एस सीक्वल सर्वर");

        String kafkaValue = new String(jsonConverter.fromConnectData(table, schema, struct));
        connect.kafka().produce(table, null, kafkaValue);

        waitForCommittedRecords(CONNECTOR_NAME, Collections.singleton(table), 1, 1, TimeUnit.MINUTES.toMillis(2));

        try (Statement s = connection.createStatement()) {
            try (ResultSet rs = s.executeQuery("SELECT * FROM dbo." + table)) {
                assertTrue(rs.next());
                assertEquals((int)struct.getInt32("id"), rs.getInt("id"));
                assertEquals(struct.getString("unicode_field"), rs.getNString("unicode_field"));
            }
        }

    }

    private Map<String, String> configProperties(String topic) {
        // Create a hashmap to setup sink connector config properties
        Map<String, String> props = new HashMap<>();

        props.put(CONNECTOR_CLASS_CONFIG, "JdbcSinkConnector");
        // converters
        props.put(KEY_CONVERTER_CLASS_CONFIG, StringConverter.class.getName());
        props.put(VALUE_CONVERTER_CLASS_CONFIG, JsonConverter.class.getName());
        // license properties
        props.put("confluent.topic.bootstrap.servers", connect.kafka().bootstrapServers());
        props.put("confluent.topic.replication.factor", "1");

        props.put(JdbcSinkConfig.CONNECTION_URL, MSSQL_URL);
        props.put(JdbcSinkConfig.CONNECTION_USER, USER);
        props.put(JdbcSinkConfig.CONNECTION_PASSWORD, PASS);
        props.put("pk.mode", "none");
        props.put("topics", topic);
        return props;
    }

    private void executeSQL(PreparedStatement stmt) throws Exception {
        try {
            stmt.executeUpdate();
        } catch (Exception ex) {
            log.error("Could not execute SQL: " + stmt.toString());
            throw ex;
        }
    }

    private void configureAndWaitForConnector(int numTasks) throws Exception {
        // start a sink connector
        connect.configureConnector(CONNECTOR_NAME, props);

        // wait for tasks to spin up
        waitForConnectorToStart(CONNECTOR_NAME, numTasks);
    }
}
