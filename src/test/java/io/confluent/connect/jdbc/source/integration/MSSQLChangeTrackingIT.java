/*
 * Copyright [2017 - 2019] Confluent Inc.
 */

package io.confluent.connect.jdbc.source.integration;

import java.sql.SQLException;
import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.connect.storage.StringConverter;
import org.testcontainers.containers.FixedHostPortGenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;

import org.apache.kafka.test.IntegrationTest;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.ClassRule;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.DriverManager;
import java.sql.Connection;
import java.sql.PreparedStatement;

import io.confluent.connect.jdbc.integration.BaseConnectorIT;
import org.apache.kafka.clients.consumer.ConsumerRecords;

import static org.junit.Assert.assertEquals;
import static io.confluent.connect.jdbc.source.JdbcSourceConnectorConfig.CONNECTION_URL_CONFIG;
import static io.confluent.connect.jdbc.source.JdbcSourceConnectorConfig.CONNECTION_USER_CONFIG;
import static io.confluent.connect.jdbc.source.JdbcSourceConnectorConfig.CONNECTION_PASSWORD_CONFIG;
import static io.confluent.connect.jdbc.source.JdbcSourceConnectorConfig.TABLE_WHITELIST_CONFIG;
import static io.confluent.connect.jdbc.source.JdbcSourceConnectorConfig.MODE_CONFIG;
import static io.confluent.connect.jdbc.source.JdbcSourceConnectorConfig.MODE_CHANGE_TRACKING;
import static io.confluent.connect.jdbc.source.JdbcSourceConnectorConfig.TOPIC_PREFIX_CONFIG;
import static io.confluent.connect.jdbc.source.JdbcSourceConnectorConfig.POLL_INTERVAL_MS_CONFIG;

import static org.apache.kafka.connect.runtime.ConnectorConfig.CONNECTOR_CLASS_CONFIG;
import static org.apache.kafka.connect.runtime.ConnectorConfig.KEY_CONVERTER_CLASS_CONFIG;
import static org.apache.kafka.connect.runtime.ConnectorConfig.TASKS_MAX_CONFIG;
import static org.apache.kafka.connect.runtime.ConnectorConfig.VALUE_CONVERTER_CLASS_CONFIG;

/**
 * Integration test for JDBC source connector change tracking mode with MSSQL Server.
 */
@Category(IntegrationTest.class)
public class MSSQLChangeTrackingIT extends BaseConnectorIT {

    private static final Logger log = LoggerFactory.getLogger(MSSQLChangeTrackingIT.class);
    private static final String CONNECTOR_NAME = "JdbcSourceConnector";
    private static final int NUM_RECORDS_PRODUCED = 3;
    private static final long CONSUME_MAX_DURATION_MS = TimeUnit.MINUTES.toMillis(2);
    private static final int TASKS_MAX = 3;
    private static final String MSSQL_URL = "jdbc:sqlserver://0.0.0.0:1433";
    private static final String MSSQL_URL_TESTDB = "jdbc:sqlserver://0.0.0.0:1433;databaseName=testdb";
    private static final String MSSQL_Table = "TestChangeTrackingTable";
    private static final String TOPIC_PREFIX = "test-";
    private static final List<String> KAFKA_TOPICS = Collections.singletonList(TOPIC_PREFIX + MSSQL_Table);

    private Map<String, String> props;
    private static final String USER = "sa";
    private static final String PASS = "reallyStrongPwd123";
    private Connection connection;

    @ClassRule
    @SuppressWarnings("deprecation")
    public static final FixedHostPortGenericContainer mssqlServer =
            new FixedHostPortGenericContainer<>("mcr.microsoft.com/mssql/server:2019-latest")
                .withEnv("ACCEPT_EULA","Y")
                .withEnv("SA_PASSWORD","reallyStrongPwd123")
                .withFixedExposedPort(1433, 1433)
                .waitingFor(Wait.forLogMessage(".*SQL Server is now ready for client connections.*", 1)
                    .withStartupTimeout(Duration.ofMinutes(2)));

    @Before
    public void setup() throws Exception {
        Class.forName("com.microsoft.sqlserver.jdbc.SQLServerDriver");
        Thread.sleep(5000);
        connection = DriverManager.getConnection(MSSQL_URL, USER, PASS);
        startConnect();
    }

    @After
    public void close() throws SQLException {
        deleteTable();
        try {
            connect.deleteConnector(CONNECTOR_NAME);
        } catch (Exception e) {
            log.warn("Failed to delete connector: {}", e.getMessage());
        }
        connection.close();
        stopConnect();
    }

    @Test
    public void verifyChangeTrackingModeWorksWithInserts() throws Exception {
        props = configProperties();
        props.put(MODE_CONFIG, MODE_CHANGE_TRACKING);

        enableChangeTracking();
        createTableWithChangeTracking();

        KAFKA_TOPICS.forEach(topic -> connect.kafka().createTopic(topic, 1));
        configureAndWaitForConnector();

        insertTestRecord(1, "initial record");
        insertTestRecord(2, "second record");

        ConsumerRecords<byte[], byte[]> records = connect.kafka().consume(
            NUM_RECORDS_PRODUCED - 1,
            CONSUME_MAX_DURATION_MS,
            KAFKA_TOPICS.toArray(new String[0])
        );

        assertEquals(2, records.count());
    }

    @Test
    public void verifyChangeTrackingModeWorksWithUpdates() throws Exception {
        props = configProperties();
        props.put(MODE_CONFIG, MODE_CHANGE_TRACKING);

        enableChangeTracking();
        createTableWithChangeTracking();

        insertTestRecord(1, "initial record");

        KAFKA_TOPICS.forEach(topic -> connect.kafka().createTopic(topic, 1));
        configureAndWaitForConnector();

        updateTestRecord(1, "updated record");

        ConsumerRecords<byte[], byte[]> records = connect.kafka().consume(
            1,
            CONSUME_MAX_DURATION_MS,
            KAFKA_TOPICS.toArray(new String[0])
        );

        assertEquals(1, records.count());
    }

    @Test
    public void verifyChangeTrackingModeWorksWithDeletes() throws Exception {
        props = configProperties();
        props.put(MODE_CONFIG, MODE_CHANGE_TRACKING);

        enableChangeTracking();
        createTableWithChangeTracking();

        insertTestRecord(1, "record to delete");

        KAFKA_TOPICS.forEach(topic -> connect.kafka().createTopic(topic, 1));
        configureAndWaitForConnector();

        deleteTestRecord(1);

        ConsumerRecords<byte[], byte[]> records = connect.kafka().consume(
            1,
            CONSUME_MAX_DURATION_MS,
            KAFKA_TOPICS.toArray(new String[0])
        );

        assertEquals(1, records.count());
    }

    private void enableChangeTracking() throws SQLException {
        try {
            String sql = "CREATE DATABASE testdb";
            PreparedStatement stmt = connection.prepareStatement(sql);
            executeSQL(stmt);
        } catch (SQLException e) {
            if (!e.getMessage().contains("already exists")) {
                throw e;
            }
        }
        
        try {
            String sql = "ALTER DATABASE testdb SET CHANGE_TRACKING = ON (CHANGE_RETENTION = 2 DAYS, AUTO_CLEANUP = ON)";
            PreparedStatement stmt = connection.prepareStatement(sql);
            executeSQL(stmt);
        } catch (SQLException e) {
            if (!e.getMessage().contains("already enabled")) {
                throw e;
            }
        }
        
        connection.close();
        connection = DriverManager.getConnection(MSSQL_URL_TESTDB, USER, PASS);
    }

    private void createTableWithChangeTracking() throws SQLException {
        String sql = "CREATE TABLE " + MSSQL_Table + " (id INT PRIMARY KEY, record VARCHAR(255))";
        PreparedStatement createStmt = connection.prepareStatement(sql);
        executeSQL(createStmt);

        sql = "ALTER TABLE " + MSSQL_Table + " ENABLE CHANGE_TRACKING WITH (TRACK_COLUMNS_UPDATED = ON)";
        PreparedStatement enableStmt = connection.prepareStatement(sql);
        executeSQL(enableStmt);
    }

    private void insertTestRecord(int id, String record) throws SQLException {
        String sql = "INSERT INTO " + MSSQL_Table + " (id, record) VALUES (?, ?)";
        PreparedStatement stmt = connection.prepareStatement(sql);
        stmt.setInt(1, id);
        stmt.setString(2, record);
        executeSQL(stmt);
    }

    private void updateTestRecord(int id, String record) throws SQLException {
        String sql = "UPDATE " + MSSQL_Table + " SET record = ? WHERE id = ?";
        PreparedStatement stmt = connection.prepareStatement(sql);
        stmt.setString(1, record);
        stmt.setInt(2, id);
        executeSQL(stmt);
    }

    private void deleteTestRecord(int id) throws SQLException {
        String sql = "DELETE FROM " + MSSQL_Table + " WHERE id = ?";
        PreparedStatement stmt = connection.prepareStatement(sql);
        stmt.setInt(1, id);
        executeSQL(stmt);
    }

    private void deleteTable() throws SQLException {
        try {
            String sql = "DROP TABLE IF EXISTS " + MSSQL_Table;
            PreparedStatement stmt = connection.prepareStatement(sql);
            executeSQL(stmt);
        } catch (SQLException e) {
            log.warn("Failed to delete table: {}", e.getMessage());
        }
    }

    private void executeSQL(PreparedStatement stmt) throws SQLException {
        try {
            stmt.execute();
        } finally {
            stmt.close();
        }
    }

    private Map<String, String> configProperties() {
        Map<String, String> props = new HashMap<>();
        props.put(CONNECTOR_CLASS_CONFIG, "io.confluent.connect.jdbc.JdbcSourceConnector");
        props.put(TASKS_MAX_CONFIG, String.valueOf(TASKS_MAX));
        props.put(CONNECTION_URL_CONFIG, MSSQL_URL_TESTDB);
        props.put(CONNECTION_USER_CONFIG, USER);
        props.put(CONNECTION_PASSWORD_CONFIG, PASS);
        props.put(TABLE_WHITELIST_CONFIG, MSSQL_Table);
        props.put(TOPIC_PREFIX_CONFIG, TOPIC_PREFIX);
        props.put(POLL_INTERVAL_MS_CONFIG, "1000");
        props.put(KEY_CONVERTER_CLASS_CONFIG, StringConverter.class.getName());
        props.put(VALUE_CONVERTER_CLASS_CONFIG, StringConverter.class.getName());
        return props;
    }

    private void configureAndWaitForConnector() throws InterruptedException {
        connect.configureConnector(CONNECTOR_NAME, props);
        connect.assertions().assertConnectorAndAtLeastNumTasksAreRunning(
            CONNECTOR_NAME,
            Math.min(KAFKA_TOPICS.size(), TASKS_MAX),
            "Connector tasks did not start in time."
        );
    }
}