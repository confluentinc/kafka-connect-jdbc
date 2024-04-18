package io.confluent.connect.jdbc;

import io.confluent.connect.jdbc.util.ConnectionURLParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.sourcelab.kafka.connect.apiclient.Configuration;
import org.sourcelab.kafka.connect.apiclient.KafkaConnectClient;

import java.sql.*;

public class MonitorThread extends Thread {
    private static final Logger log = LoggerFactory.getLogger(MonitorThread.class);

    private final KafkaConnectClient kafkaClient;
    private final String connectorName;
    private final long monitoringThreadInterval;
    private final long monitoringThreadInitialDelay;
    private boolean running = false;
    private Connection connection;
    private String jdbcUrl;
    private String username;
    private String password;
    private ConnectionLostListener connectionLostListener;
    private ServiceControl serviceControl;

    private final int maxRetryAttempts = 3; // Maximum number of retry attempts


    //


    public MonitorThread(String connectionUrl, String kafkaConnectUrl, String connectorName, long monitoringThreadInitialDelay ,long monitoringThreadInterval) {
        log.info("Creating MonitorThread for connector: {}", connectorName);
        ConnectionURLParser connectionURLParser = new ConnectionURLParser(connectionUrl);
        username = connectionURLParser.getUsername();
        password = connectionURLParser.getPassword();

        jdbcUrl = connectionUrl;
        this.connectorName = connectorName;
        this.monitoringThreadInterval = monitoringThreadInterval;
        this.monitoringThreadInitialDelay = monitoringThreadInitialDelay;
        Configuration configuration = new Configuration(kafkaConnectUrl);
        kafkaClient = new KafkaConnectClient(configuration);
        serviceControl = new ConnectorService(this.kafkaClient, this.connectorName);


    }

    // Method to set connection lost listener
    public void setConnectionLostListener(ConnectionLostListener listener) {
        this.connectionLostListener = listener;
    }

    // Method to set service control
    public void setServiceControl(ServiceControl serviceControl) {
        this.serviceControl = serviceControl;
    }

    // Method to start the monitor thread
    public synchronized void startMonitoring() {
        if (!running) {

            // wait for initial delay before starting the monitor thread
            try {
                Thread.sleep(monitoringThreadInitialDelay);
            } catch (InterruptedException e) {
                log.error("Error while waiting for initial delay: "+e.getMessage());
            }

            running = true;
            start();
        }
    }

    // Method to stop the monitor thread
    public synchronized void stopMonitoring() {
        running = false;
    }

    @Override
    public void run() {
        log.info("Starting MonitorThread for connector: {}", connectorName);
        int retryAttempts = 0;
        try {
            while (running) {
                if (!isConnected()) {
                    if(retryAttempts == 0 && connectionLostListener != null) {
                        connectionLostListener.onConnectionLost();
                    }
                    // Connection lost, retry connecting
                    if (retryAttempts < maxRetryAttempts) {
                        retryAttempts++;
                        log.info("Connection lost. Attempting to reconnect. Retry attempt: {}", retryAttempts);
                        reconnect();
                        Thread.sleep(monitoringThreadInterval);
                        continue; // Retry connecting
                    } else {
                        log.error("Maximum retry attempts reached. Pausing connector: {}", connectorName);
                        // Maximum retry attempts reached, pause the connector
                        serviceControl.pauseServices();

                        retryAttempts = 0; // Reset retry attempts
                    }
                } else {
                    if (!serviceControl.isServicesResumed()) {
                        log.info("Connection restored. Resuming connector: {}", connectorName);
                        // Connection restored, resume connector
                        serviceControl.resumeServices();
                    }
                    retryAttempts = 0; // Reset retry attempts
                }
                // Sleep for some time before checking connection again
                Thread.sleep(5000); // Sleep for 5 seconds
            }
        } catch (InterruptedException e) {
            log.error("MonitorThread interrupted: "+e.getMessage());
            if(running){
                startMonitoring();
            }
        }
    }


    public boolean isConnectionAlive(Connection connection) {
        try {
            Statement stmt = connection.createStatement();
            ResultSet rs = stmt.executeQuery("SELECT 1");
            rs.close();
            stmt.close();
            return true;
        } catch (SQLException e) {
            log.error("Error checking connection status: "+e.getMessage());
            return false;
        }catch (Exception e){
            log.error("Error checking connection status: "+e.getMessage());
            return false;
        }
    }
    private boolean isConnected() {
        try {

            // check if the connection is active


            if (connection != null && !connection.isClosed()) {
                log.info("DB Connection is active");
                return isConnectionAlive(connection);
            }
        } catch (SQLException e) {
           log.error("Error checking connection status: "+e.getMessage());
        }
        log.info("DB Connection is lost");
        return false;
    }

    private boolean reconnect() {
        try {
            log.info("Reconnecting to database...");
            if (connection != null) {
                connection.close();
            }
            connection = DriverManager.getConnection(jdbcUrl, username, password);
            return true;
            // Perform any additional setup for the connection
        } catch (Exception e) {
            log.error("Error reconnecting to database: "+e.getMessage());
        }
        return false;
    }

    public interface ConnectionLostListener {
        void onConnectionLost();
    }

    public interface ServiceControl {
        void pauseServices();

        void resumeServices();

        boolean isServicesResumed();
    }


    enum ConnectStatus {
        STATUS_RUNNING("RUNNING"),
        STATUS_PAUSED("PAUSED");

        private final String status;

        ConnectStatus(String status) {
            this.status = status;
        }

        @Override
        public String toString() {
            return status;
        }
    }

    class ConnectorService implements ServiceControl {


        private final KafkaConnectClient kafkaClient;
        private final String connectorName;

        ConnectorService(KafkaConnectClient kafkaClient, String connectorName) {
            this.kafkaClient = kafkaClient;
            this.connectorName = connectorName;

        }

        @Override
        public void pauseServices() {
            log.info("Pausing connector service...");
            kafkaClient.pauseConnector(connectorName);
        }

        @Override
        public void resumeServices() {
            log.info("Resuming connector service...");
            kafkaClient.resumeConnector(connectorName);
        }

        @Override
        public boolean isServicesResumed() {
            // Check if the connector service is resumed
            boolean resumed = kafkaClient.getConnectorStatus(connectorName).getTasks().stream().reduce(true, (a, b) -> a && b.getState().equals(ConnectStatus.STATUS_RUNNING.status), (a, b) -> a && b);
            log.info("Is connector service running: {}", resumed);
            return resumed;
        }
    }
}