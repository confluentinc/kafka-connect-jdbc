package io.confluent.connect.jdbc.source.integration;

import io.confluent.common.utils.IntegrationTest;
import io.confluent.connect.jdbc.JdbcSourceConnector;
import io.confluent.connect.jdbc.integration.BaseConnectorIT;
import io.confluent.connect.jdbc.source.JdbcSourceConnectorConfig;
import org.apache.kafka.connect.runtime.ConnectorConfig;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.testcontainers.containers.OracleContainer;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;

import static org.apache.kafka.connect.runtime.ConnectorConfig.TASKS_MAX_CONFIG;
import static org.junit.Assert.assertTrue;

@Category(IntegrationTest.class)
public class OracleSynonymIT extends BaseConnectorIT {
    @SuppressWarnings( "deprecation" )
    @Rule
    public OracleContainer oracle = new OracleContainer();

    private Connection connection;
    private final Map<String, String> props = new HashMap<>();
    private final String synonymName = "TEST_SYNONYM";

    @Before
    public void setup() throws SQLException {
        startConnect();

        props.put(ConnectorConfig.CONNECTOR_CLASS_CONFIG, JdbcSourceConnector.class.getName());
        props.put(TASKS_MAX_CONFIG, "1");

        props.put(JdbcSourceConnectorConfig.CONNECTION_URL_CONFIG, oracle.getJdbcUrl());
        props.put(JdbcSourceConnectorConfig.CONNECTION_USER_CONFIG, oracle.getUsername());
        props.put(JdbcSourceConnectorConfig.CONNECTION_PASSWORD_CONFIG, oracle.getPassword());

        props.put(JdbcSourceConnectorConfig.TABLE_TYPE_CONFIG, "SYNONYM");

        props.put(JdbcSourceConnectorConfig.MODE_CONFIG, JdbcSourceConnectorConfig.MODE_TIMESTAMP_INCREMENTING);
        props.put(JdbcSourceConnectorConfig.INCREMENTING_COLUMN_NAME_CONFIG, "ID");
        props.put(JdbcSourceConnectorConfig.TIMESTAMP_COLUMN_NAME_CONFIG, "TSTAMP");

        props.put(JdbcSourceConnectorConfig.TABLE_WHITELIST_CONFIG, synonymName);
        props.put(JdbcSourceConnectorConfig.TOPIC_PREFIX_CONFIG, "topic_");

        connect.kafka().createTopic("topic_" + synonymName, 1);

        connection = DriverManager.getConnection(oracle.getJdbcUrl(),
            oracle.getUsername(), oracle.getPassword());
    }

    @After
    public void tearDown() throws SQLException {
        connection.close();

        stopConnect();
        oracle.stop();
    }

    @Test
    public void testSynonymStartupValidation() throws Exception {
        String tableName = "TEST_TABLE";
        try (Statement s = connection.createStatement()) {
            s.execute("CREATE TABLE " + tableName + "("
                    + "ID NUMBER NOT NULL, PRIMARY KEY (ID), "
                    + "TSTAMP TIMESTAMP NOT NULL"
                    + ")");
        }

        try (Statement s = connection.createStatement()) {
            s.execute("CREATE SYNONYM " + synonymName + " FOR " + tableName);
        }

        String connectorName = "synonymConnector";
        connect.configureConnector(connectorName, props);
        waitForConnectorToStart(connectorName, 1);

        assertTrue(
                "Connector or one of its tasks could not be started with a synonym table-type",
                assertConnectorAndTasksRunning(connectorName, 1).orElse(false));
    }
}
