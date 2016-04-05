/**
 * Copyright 2015 Confluent Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 **/

package io.confluent.connect.jdbc;

import io.confluent.common.utils.MockTime;
import io.confluent.common.utils.Time;
import org.apache.kafka.connect.source.SourceTaskContext;
import org.apache.kafka.connect.storage.OffsetStorageReader;
import org.easymock.EasyMock;
import org.junit.After;
import org.junit.Before;
import org.powermock.api.easymock.PowerMock;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

/**
 * Base class for Oracle specific data type conversion tests.
 */
public class JdbcSourceTaskTestOracleBase
{

  protected static String SINGLE_TABLE_NAME = "test";
  protected static Map<String, Object> SINGLE_TABLE_PARTITION = new HashMap<>();

  static {
    SINGLE_TABLE_PARTITION.put(JdbcSourceConnectorConstants.TABLE_NAME_KEY, SINGLE_TABLE_NAME);
  }

  protected static EmbeddedDerby.TableName SINGLE_TABLE
      = new EmbeddedDerby.TableName(SINGLE_TABLE_NAME);

  protected static final String TOPIC_PREFIX = "test-";

  protected Time time;
  protected SourceTaskContext taskContext;
  protected JdbcSourceTask task;
  protected EmbeddedHSQLDB db;

  @Before
  public void setup() throws Exception {
    time = new MockTime();
    task = new JdbcSourceTask(time);
    taskContext = PowerMock.createMock(SourceTaskContext.class);
    db = new EmbeddedHSQLDB();
  }

  @After
  public void tearDown() throws Exception {
    db.close();
    db.dropDatabase();
  }

  protected Map<String, String> singleTableConfig() {
    Map<String, String> props = new HashMap<>();
    props.put(JdbcSourceConnectorConfig.CONNECTION_URL_CONFIG, db.getUrl());
    props.put(JdbcSourceTaskConfig.TABLES_CONFIG, SINGLE_TABLE_NAME);
    props.put(JdbcSourceConnectorConfig.MODE_CONFIG, JdbcSourceConnectorConfig.MODE_BULK);
    props.put(JdbcSourceTaskConfig.TOPIC_PREFIX_CONFIG, TOPIC_PREFIX);
    return props;
  }
  protected void initializeTask() {
    task.initialize(taskContext);
  }

}
