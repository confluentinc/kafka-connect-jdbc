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

package io.confluent.connect.jdbc.source;

import io.confluent.connect.jdbc.util.TopicNameProvider;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.After;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;

// Tests of polling that return data updates, i.e. verifies the different behaviors with a custom Topic Name Provider
@RunWith(PowerMockRunner.class)
@PrepareForTest({JdbcSourceTask.class})
@PowerMockIgnore("javax.management.*")
public class JdbcSourceTaskTopicNameProviderTest extends JdbcSourceTaskTestBase {

    private static final String SINGLE_TABLE_NAME_REPLACED = "test_replaced";
    private static final String SECOND_TABLE_NAME_REPLACED = "test2_replaced";

    @After
    public void tearDown() throws Exception {
        task.stop();
        super.tearDown();
    }

    @Test
    public void testSingleTableShouldHaveCorrectTopicName() throws Exception {
        db.createTable(SINGLE_TABLE_NAME, "id", "INT");

        Map<String, String> taskConfig = singleTableConfig();
        taskConfig.put(JdbcSourceConnectorConfig.TOPIC_NAME_PROVIDER_CLASS_CONFIG, TestTopicNameProvider.class.getName());
        task.start(taskConfig);

        db.insert(SINGLE_TABLE_NAME, "id", 1);
        db.insert(SINGLE_TABLE_NAME, "id", 2);

        List<SourceRecord> records = task.poll();
        validatePollResultTopic(records, 2, taskConfig.get(JdbcSourceConnectorConfig.TOPIC_PREFIX_CONFIG)+ SINGLE_TABLE_NAME_REPLACED);
    }

    @Test
    public void testMultipleTableShouldHaveCorrectTopicName() throws Exception {
        db.createTable(SINGLE_TABLE_NAME, "id", "INT");
        db.createTable(SECOND_TABLE_NAME, "id", "INT");

        Map<String, String> taskConfig = twoTableConfig();
        taskConfig.put(JdbcSourceConnectorConfig.TOPIC_NAME_PROVIDER_CLASS_CONFIG, TestTopicNameProvider.class.getName());
        task.start(taskConfig);

        db.insert(SINGLE_TABLE_NAME, "id", 1);
        db.insert(SECOND_TABLE_NAME, "id", 2);

        // Both tables should be polled, in order
        List<SourceRecord> records = task.poll();
        validatePollResultTopic(records, 1, taskConfig.get(JdbcSourceConnectorConfig.TOPIC_PREFIX_CONFIG)+ SINGLE_TABLE_NAME_REPLACED);

        records = task.poll();
        validatePollResultTopic(records, 1, taskConfig.get(JdbcSourceConnectorConfig.TOPIC_PREFIX_CONFIG)+ SECOND_TABLE_NAME_REPLACED);
    }

    private static void validatePollResultTopic(List<SourceRecord> records,
        int expected, String topic) {
        assertEquals(expected, records.size());
        for (SourceRecord record : records) {
            assertEquals(topic, record.topic());
        }
    }

    private static class TestTopicNameProvider implements TopicNameProvider {

        public TestTopicNameProvider() {
        }

        @Override
        public String getTopicName(String prefix, String tableName) {
            if (tableName.equals(SINGLE_TABLE_NAME)) {
                return prefix + SINGLE_TABLE_NAME_REPLACED;
            } else if(tableName.equals(SECOND_TABLE_NAME)) {
                return prefix + SECOND_TABLE_NAME_REPLACED;
            } else {
                return prefix + "empty_topic_name";
            }
        }
    }
}
