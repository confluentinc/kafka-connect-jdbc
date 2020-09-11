/*
 * Copyright [2017 - 2019] Confluent Inc.
 */

package io.confluent.connect.jdbc.source.integration;

import java.sql.SQLException;
import java.time.ZoneId;
import java.time.ZonedDateTime;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.connect.storage.StringConverter;
import org.testcontainers.containers.FixedHostPortGenericContainer;

import org.apache.kafka.test.IntegrationTest;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.ClassRule;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.DriverManager;
import java.sql.Statement;
import java.sql.Connection;
import java.sql.Timestamp;
import java.sql.PreparedStatement;
import java.time.Duration;

import io.confluent.connect.jdbc.integration.BaseConnectorIT;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import java.lang.StringBuilder;

import static org.junit.Assert.assertEquals;
import static io.confluent.connect.jdbc.source.JdbcSourceConnectorConfig.CONNECTION_URL_CONFIG;
import static io.confluent.connect.jdbc.source.JdbcSourceConnectorConfig.CONNECTION_USER_CONFIG;
import static io.confluent.connect.jdbc.source.JdbcSourceConnectorConfig.CONNECTION_PASSWORD_CONFIG;
import static io.confluent.connect.jdbc.source.JdbcSourceConnectorConfig.TABLE_WHITELIST_CONFIG;
import static io.confluent.connect.jdbc.source.JdbcSourceConnectorConfig.MODE_CONFIG;
import static io.confluent.connect.jdbc.source.JdbcSourceConnectorConfig.MODE_TIMESTAMP;
import static io.confluent.connect.jdbc.source.JdbcSourceConnectorConfig.MODE_TIMESTAMP_INCREMENTING;
import static io.confluent.connect.jdbc.source.JdbcSourceConnectorConfig.INCREMENTING_COLUMN_NAME_CONFIG;
import static io.confluent.connect.jdbc.source.JdbcSourceConnectorConfig.TIMESTAMP_COLUMN_NAME_CONFIG;
import static io.confluent.connect.jdbc.source.JdbcSourceConnectorConfig.TOPIC_PREFIX_CONFIG;
import static io.confluent.connect.jdbc.source.JdbcSourceConnectorConfig.POLL_INTERVAL_MS_CONFIG;

import static io.confluent.connect.utils.licensing.LicenseConfigUtil.CONFLUENT_TOPIC_BOOTSTRAP_SERVERS_CONFIG;
import static io.confluent.connect.utils.licensing.LicenseConfigUtil.CONFLUENT_TOPIC_REPLICATION_FACTOR_CONFIG;
import static org.apache.kafka.connect.runtime.ConnectorConfig.CONNECTOR_CLASS_CONFIG;
import static org.apache.kafka.connect.runtime.ConnectorConfig.KEY_CONVERTER_CLASS_CONFIG;
import static org.apache.kafka.connect.runtime.ConnectorConfig.TASKS_MAX_CONFIG;
import static org.apache.kafka.connect.runtime.ConnectorConfig.VALUE_CONVERTER_CLASS_CONFIG;

@Category(IntegrationTest.class)
public class MSSQLDateTimeIT extends BaseConnectorIT {

    private static final Logger log = LoggerFactory.getLogger(MSSQLDateTimeIT.class);
    private static final String CONNECTOR_NAME = "JdbcSourceConnector";
    private static final int NUM_RECORDS_PRODUCED = 1;
    private static final long CONSUME_MAX_DURATION_MS = TimeUnit.MINUTES.toMillis(2);
    private static final int TASKS_MAX = 3;
    private static final String MSSQL_URL = "jdbc:sqlserver://0.0.0.0:1433";
    private static final String MSSQL_Table = "TestTable";
    private static final String TOPIC_PREFIX = "test-";
    private static final List<String> KAFKA_TOPICS = Collections.singletonList(TOPIC_PREFIX + MSSQL_Table );

    private Map<String, String> props;

    private static final String USER = "sa"; // test creds
    private static final String PASS = "reallyStrongPwd123"; // test creds

    private Connection connection;

    @ClassRule
    public static final FixedHostPortGenericContainer mssqlServer =
            new FixedHostPortGenericContainer<>("microsoft/mssql-server-linux:latest")
                .withEnv("ACCEPT_EULA","Y")
                .withEnv("SA_PASSWORD","reallyStrongPwd123")
                .withFixedExposedPort(1433, 1433);

    @Before
    public void setup() throws Exception {
        //Create table
        Class.forName("com.microsoft.sqlserver.jdbc.SQLServerDriver");
        connection = DriverManager.getConnection(MSSQL_URL, USER, PASS);
        startConnect();

    }

    @After
    public void close() throws SQLException {
        connect.deleteConnector(CONNECTOR_NAME);
        connection.close();
        // Stop all Connect, Kafka and Zk threads.
        stopConnect();
    }

    /**
     * Verify that after a number of poll intervals the number of records in topic
     * still equal to the number of records written to the MSSQL Server instance.
     *
     * This test is for a datetime to datetime2 comparison error in MSSQL Server (2016 and after).
     * Datetime is converted recursively to a higher precision (eg- 3.33 MS would be 3.33333 ),
     * while java.sql.Timestamp converts by tacking on 0s (3.33000).
     */
    // Verify connect error thrown
    @Test
    public void verifyDateTimeTSModeKillsTasks() throws Exception {
        // Setup up props for the source connector
        props = configProperties(true);
        createDateTimeTable("DATETIME", "VARCHAR(255)", true);
        ZoneId utc = ZoneId.of("UTC");
        // Create topic in Kafka
        KAFKA_TOPICS.forEach(topic -> connect.kafka().createTopic(topic, 1));

        // Configure source connector
        configureAndWaitForConnector();

        java.sql.Timestamp t = java.sql.Timestamp.from(
                ZonedDateTime.of(2016, 12, 8, 19, 34, 56, 2000000, utc).toInstant()
        );

        writeSingleRowWithTimestamp(t, true, true);

        connect.assertions().assertConnectorIsRunningAndTasksHaveFailed(
                CONNECTOR_NAME,
                Math.min(KAFKA_TOPICS.size(), TASKS_MAX),
                "failed to verify that tasks have failed"
        );

        deleteTable();
    }


    @Test
    public void verifyDateTimeTSAndIncrModeKillsTasks() throws Exception {
        // Setup up props for the source connector
        props = configProperties(false);
        createDateTimeTable("DATETIME", "VARCHAR(255)", false);
        ZoneId utc = ZoneId.of("UTC");
        // Create topic in Kafka
        KAFKA_TOPICS.forEach(topic -> connect.kafka().createTopic(topic, 1));

        // Configure source connector
        configureAndWaitForConnector();

        java.sql.Timestamp t = java.sql.Timestamp.from(
                ZonedDateTime.of(2016, 12, 8, 19, 34, 56, 2000000, utc).toInstant()
        );

        writeSingleRowWithTimestamp(t, true, false);

        connect.assertions().assertConnectorIsRunningAndTasksHaveFailed(
                CONNECTOR_NAME,
                Math.min(KAFKA_TOPICS.size(), TASKS_MAX),
                "failed to verify that tasks have failed"
        );

        deleteTable();
    }


    /**
     * Verify that after a number of poll intervals the number of records in topic
     * still equal to the number of records written to the MSSQL Server instance.
     *
     * This test is for a datetime to datetime2 comparison error in MSSQL Server (2016 and after).
     * Datetime is converted recursively to a higer precision (eg- 3.33 MS would be 3.33333 ),
     * while java.sql.Timestamp converts by tacking on 0s (3.33000).
     */

    @Test
    public void verifyDATETIME2DoesNotKillTask() throws Exception {
        props = configProperties(true);
        // And verify that datetime in a non-timestamp column does not kill task.
        createDateTimeTable("DATETIME2", "DATETIME", true);
        ZoneId utc = ZoneId.of("UTC");
        // Create topic in Kafka
        KAFKA_TOPICS.forEach(topic -> connect.kafka().createTopic(topic, 1));

        // Configure source connector
        configureAndWaitForConnector();

        java.sql.Timestamp t = java.sql.Timestamp.from(
                ZonedDateTime.of(2016, 12, 8, 19, 34, 56, 2000000, utc).toInstant()
        );

        writeSingleRowWithTimestamp(t, false, true);

        Thread.sleep(Duration.ofSeconds(30).toMillis());
        for (String topic: KAFKA_TOPICS) {
            ConsumerRecords<byte[], byte[]> records = connect.kafka().consume(
                    NUM_RECORDS_PRODUCED,
                    CONSUME_MAX_DURATION_MS,
                    topic);
            //Assert that records in topic == NUM_RECORDS_PRODUCED
            assertEquals(NUM_RECORDS_PRODUCED, records.count());

        }
        // clean up table
        deleteTable();
    }

    private Map<String, String> configProperties(boolean timestampModeOnly) {
        // Create a hashmap to setup source connector config properties
        Map<String, String> props = new HashMap<>();
        props.put(CONNECTOR_CLASS_CONFIG, "JdbcSourceConnector");
        props.put(CONNECTION_URL_CONFIG, MSSQL_URL);
        props.put(CONNECTION_USER_CONFIG, "sa");
        props.put(CONNECTION_PASSWORD_CONFIG, "reallyStrongPwd123");
        props.put(TABLE_WHITELIST_CONFIG, MSSQL_Table);

        if (timestampModeOnly) {
            props.put(MODE_CONFIG, MODE_TIMESTAMP);
        } else {
            props.put(MODE_CONFIG, MODE_TIMESTAMP_INCREMENTING);
            props.put(INCREMENTING_COLUMN_NAME_CONFIG, "ict");
        }

        props.put(TIMESTAMP_COLUMN_NAME_CONFIG, "start_time");
        props.put(TOPIC_PREFIX_CONFIG, "test-");
        props.put(POLL_INTERVAL_MS_CONFIG, "30");
        props.put(TASKS_MAX_CONFIG, Integer.toString(TASKS_MAX));
        props.put(KEY_CONVERTER_CLASS_CONFIG, StringConverter.class.getName());
        props.put(VALUE_CONVERTER_CLASS_CONFIG, StringConverter.class.getName());
        props.put(CONFLUENT_TOPIC_BOOTSTRAP_SERVERS_CONFIG, connect.kafka().bootstrapServers());
        props.put(CONFLUENT_TOPIC_REPLICATION_FACTOR_CONFIG, "1");

        return props;
    }

    private void createDateTimeTable(
            String timestampColType,
            String regularColType,
            boolean timestampModeOnly
    ) throws Exception {
        try {
            Statement stmt = connection.createStatement();
            StringBuilder builder = new StringBuilder();
            builder.append("CREATE TABLE " + MSSQL_Table )
                    .append("(start_time " + timestampColType +  " not NULL, " );
            if (!timestampModeOnly) builder.append(" ict  int not NULL, ");
            builder.append(" record " + regularColType +  " )");

            String sql = builder.toString();

            stmt.executeUpdate(sql);
            log.info("Successfully created table");
        } catch (Exception ex) {
            log.error("Could not create table");
            throw ex;
        }
    }

    private void writeSingleRowWithTimestamp(
            Timestamp t,
            boolean stringRecord,
            boolean timestampModeOnly
    ) throws Exception {
        try {
            StringBuilder builder = new StringBuilder();
            builder.append("INSERT INTO " + MSSQL_Table )
                    .append("(start_time, record" );
            if (!timestampModeOnly) builder.append(", ict");
            builder.append(") values (?, ?");
            if (!timestampModeOnly) builder.append(", ?");
            builder.append(")");
            String sql = builder.toString();
            PreparedStatement stmt = connection.prepareStatement(sql);


            stmt.setTimestamp(1, t);
            if (stringRecord) {
                stmt.setString(2, "example record value");
            } else {
                stmt.setTimestamp(2, t);
            }
            if (!timestampModeOnly) stmt.setInt(3, 1);

            stmt.executeUpdate();
        } catch (Exception ex) {
            log.error("Could not write row to MSSQL table");
            throw ex;
        }
    }

    private void deleteTable() throws Exception {
        try {
            PreparedStatement stmt = connection.prepareStatement(
                    "DROP TABLE " + MSSQL_Table
            );
            stmt.executeUpdate();
        } catch (Exception ex) {
            log.error("Could delete all rows in the MSSQL table");
            throw ex;
        }
    }

    private void configureAndWaitForConnector() throws Exception {
        // start a source connector
        connect.configureConnector(CONNECTOR_NAME, props);

        // wait for tasks to spin up
        int minimumNumTasks = Math.min(KAFKA_TOPICS.size(), TASKS_MAX);
        waitForConnectorToStart(CONNECTOR_NAME, minimumNumTasks);
    }
}