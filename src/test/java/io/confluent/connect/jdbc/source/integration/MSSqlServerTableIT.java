package io.confluent.connect.jdbc.source.integration;

import io.confluent.common.utils.IntegrationTest;
import io.confluent.connect.jdbc.JdbcSourceConnector;
import io.confluent.connect.jdbc.integration.BaseConnectorIT;
import io.confluent.connect.jdbc.sink.integration.MicrosoftSqlServerSinkIT;
import io.confluent.connect.jdbc.source.JdbcSourceConnectorConfig;
import org.apache.kafka.connect.json.JsonConverter;
import org.apache.kafka.connect.runtime.ConnectorConfig;
import org.junit.*;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.FixedHostPortGenericContainer;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Map;

import static org.apache.kafka.connect.runtime.ConnectorConfig.TASKS_MAX_CONFIG;

@Category(IntegrationTest.class)
public class MSSqlServerTableIT extends BaseConnectorIT {
  private static final Logger log = LoggerFactory.getLogger(MicrosoftSqlServerSinkIT.class);
  private static final String CONNECTOR_NAME = "test-connector";
  private static final String MSSQL_URL = "jdbc:sqlserver://0.0.0.0:1433";
  private static final String USER = "sa";
  private static final String PASS = "reallyStrongPwd123";
  private final String synonymName = "test_synonym";

  @ClassRule
  @SuppressWarnings("deprecation")
  public static final FixedHostPortGenericContainer mssqlServer =
      new FixedHostPortGenericContainer<>("mcr.microsoft.com/mssql/server:2019-latest")
          .withEnv("ACCEPT_EULA", "Y")
          .withEnv("SA_PASSWORD", PASS)
          .withFixedExposedPort(1433, 1433);

  private Map<String, String> props;
  private Connection connection;
  private JsonConverter jsonConverter;

@Before
  public void setup() throws Exception {
    //Set up JDBC Driver
    Class.forName("com.microsoft.sqlserver.jdbc.SQLServerDriver");
    connection = DriverManager.getConnection(MSSQL_URL, USER, PASS);
    startConnect();

    props.put(ConnectorConfig.CONNECTOR_CLASS_CONFIG, JdbcSourceConnector.class.getName());
    props.put(TASKS_MAX_CONFIG, "1");

    props.put(JdbcSourceConnectorConfig.CONNECTION_URL_CONFIG, MSSQL_URL);
    props.put(JdbcSourceConnectorConfig.CONNECTION_USER_CONFIG, USER);
    props.put(JdbcSourceConnectorConfig.CONNECTION_PASSWORD_CONFIG, PASS);

    props.put(JdbcSourceConnectorConfig.TABLE_TYPE_CONFIG, "SYNONYM");

    props.put(
        JdbcSourceConnectorConfig.MODE_CONFIG,
        JdbcSourceConnectorConfig.MODE_TIMESTAMP_INCREMENTING);
    props.put(JdbcSourceConnectorConfig.INCREMENTING_COLUMN_NAME_CONFIG, "ID");
    props.put(JdbcSourceConnectorConfig.TIMESTAMP_COLUMN_NAME_CONFIG, "time");

    props.put(JdbcSourceConnectorConfig.TABLE_WHITELIST_CONFIG, synonymName);
    props.put(JdbcSourceConnectorConfig.TOPIC_PREFIX_CONFIG, "topic_");

    connect.kafka().createTopic("topic_" + synonymName, 1);

    connection =
        DriverManager.getConnection(
         MSSQL_URL, USER, PASS);
  }

  @After
  public void close() throws SQLException {
   connect.deleteConnector(CONNECTOR_NAME);
   connection.close();
   stopConnect();
  }

  @Test
  public void testTaskStartupWithSynonymTable() throws Exception {
    String tableName = "test_table";
    String tableNameWithSchema = "dbo." + tableName;
    try (Statement s = connection.createStatement()) {
      s.execute(
          "CREATE TABLE "
              + tableName
              + "("
              + "ID INT PRIMARY KEY, "
              + "name VARCHAR(50) NOT NULL, "
              + "time DATETIME2 NOT NULL"
              + ")");
    }

    try (Statement s = connection.createStatement()) {
      s.execute("CREATE SYNONYM " + synonymName + " FOR " + tableNameWithSchema);
    }

    connect.configureConnector(CONNECTOR_NAME, props);
    waitForConnectorToStart(CONNECTOR_NAME, 1);

    assertConnectorAndTasksRunning(CONNECTOR_NAME, 1);
  }
}
