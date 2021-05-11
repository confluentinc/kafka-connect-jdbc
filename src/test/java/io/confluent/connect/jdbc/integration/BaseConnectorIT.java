/*
 * Copyright 2018 Confluent Inc.
 *
 * Licensed under the Confluent Community License (the "License"); you may not use
 * this file except in compliance with the License.  You may obtain a copy of the
 * License at
 *
 * http://www.confluent.io/confluent-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OF ANY KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations under the License.
 */

package io.confluent.connect.jdbc.integration;

import java.util.Collections;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.connect.json.JsonConverter;
import org.apache.kafka.connect.runtime.AbstractStatus;
import org.apache.kafka.connect.runtime.rest.entities.ConnectorStateInfo;
import org.apache.kafka.connect.storage.ConverterConfig;
import org.apache.kafka.connect.storage.ConverterType;
import org.apache.kafka.connect.util.clusters.EmbeddedConnectCluster;
import org.apache.kafka.test.IntegrationTest;
import org.apache.kafka.test.TestUtils;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Category(IntegrationTest.class)
public abstract class BaseConnectorIT {

    private static final Logger log = LoggerFactory.getLogger(BaseConnectorIT.class);

    protected static final long CONNECTOR_STARTUP_DURATION_MS = TimeUnit.SECONDS.toMillis(60);

    protected EmbeddedConnectCluster connect;

    protected void startConnect() {
        connect = new EmbeddedConnectCluster.Builder()
                .name("jdbc-connect-cluster")
                .build();

        // start the clusters
        connect.start();
    }

    protected JsonConverter jsonConverter() {
        JsonConverter jsonConverter = new JsonConverter();
        jsonConverter.configure(Collections.singletonMap(
                ConverterConfig.TYPE_CONFIG,
                ConverterType.VALUE.getName()
        ));

        return jsonConverter;
    }

    protected void stopConnect() {
        // stop all Connect, Kafka and Zk threads.
        if (connect != null) {
            connect.stop();
        }
    }

    /**
     * Wait up to {@link #CONNECTOR_STARTUP_DURATION_MS maximum time limit} for the connector with the given
     * name to start the specified number of tasks.
     *
     * @param name the name of the connector
     * @param numTasks the minimum number of tasks that are expected
     * @return the time this method discovered the connector has started, in milliseconds past epoch
     * @throws InterruptedException if this was interrupted
     */
    protected long waitForConnectorToStart(String name, int numTasks) throws InterruptedException {
        TestUtils.waitForCondition(
                () -> assertConnectorAndTasksRunning(name, numTasks).orElse(false),
                CONNECTOR_STARTUP_DURATION_MS,
                "Connector tasks did not start in time."
        );
        return System.currentTimeMillis();
    }

    /**
     * Confirm that a connector with an exact number of tasks is running.
     *
     * @param connectorName the connector
     * @param numTasks the minimum number of tasks
     * @return true if the connector and tasks are in RUNNING state; false otherwise
     */
    protected Optional<Boolean> assertConnectorAndTasksRunning(String connectorName, int numTasks) {
        try {
            ConnectorStateInfo info = connect.connectorStatus(connectorName);
            boolean result = info != null
                    && info.tasks().size() >= numTasks
                    && info.connector().state().equals(AbstractStatus.State.RUNNING.toString())
                    && info.tasks().stream().allMatch(s -> s.state().equals(AbstractStatus.State.RUNNING.toString()));
            return Optional.of(result);
        } catch (Exception e) {
            log.warn("Could not check connector state info.");
            return Optional.empty();
        }
    }

    protected boolean assertConnectorIsRunningButTasksFailedWith(
            String connectorName,
            int numTasks,
            String error
    ) {
        try {
            ConnectorStateInfo info = connect.connectorStatus(connectorName);
            if (info != null) {
                return info.tasks().size() == numTasks
                        && info.connector().state().equals(
                        AbstractStatus.State.RUNNING.toString()
                )
                        && info.tasks().stream().allMatch(
                        (s) -> s.state().equals(
                                AbstractStatus.State.FAILED.toString()
                        )
                                && s.trace().contains(error)
                );
            } else {
                return false;
            }

        } catch (Exception ex) {
            log.error("Could not check connector state info.", ex);
            return false;
        }
    }

    protected void assertTasksFailedWithTrace(
            String connectorName,
            int numTasks,
            String error
    ) throws InterruptedException {
        try {
            TestUtils.waitForCondition(
                    () -> assertConnectorIsRunningButTasksFailedWith(
                            connectorName,
                            numTasks,
                            error
                    ),
                    CONNECTOR_STARTUP_DURATION_MS,
                    "Either the connector is not running or not all the "
                            + numTasks + " tasks have failed."
            );
        } catch (AssertionError ex) {
            throw new AssertionError("failed to verify that tasks have failed", ex);
        }
    }
}