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

package io.confluent.connect.jdbc;

import io.confluent.connect.jdbc.source.JdbcSchedSourceTask;
import io.confluent.connect.jdbc.source.JdbcSchedSourceTaskConfig;
import io.confluent.connect.jdbc.source.JdbcSourceConnectorConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.Task;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**

 */
public class JdbcSchedSourceConnector extends JdbcSourceConnector {
  private static final Logger log = LoggerFactory.getLogger(JdbcSchedSourceConnector.class);
  public static final String CRON_MODE = JdbcSchedSourceTaskConfig.CRON_MODE;
  public static final String SCHED_GROUP = JdbcSchedSourceTaskConfig.SCHED_GROUP;
  public static final String CRON_EXPRESS = JdbcSchedSourceTaskConfig.CRON_EXPRESS;
  public static final String CRON_CHECK_INTERVAL = JdbcSchedSourceTaskConfig.CRON_CHECK_INTERVAL;

  @Override
  public Class<? extends Task> taskClass() {
    return JdbcSchedSourceTask.class;
  }

  @Override
  public ConfigDef config() {
    ConfigDef configDef = JdbcSourceConnectorConfig.CONFIG_DEF;
    addSchedOptions(configDef);
    return configDef;
  }

  private static final void addSchedOptions(ConfigDef config) {
    int orderInGroup = 0;
    config.define(
            CRON_MODE,
            ConfigDef.Type.STRING,
            "native",
            ConfigDef.Importance.HIGH,
            "可以选择值：native,scheduler。默认native；native:不做任何操作使用原生代码，scheduler 使用quartz指定时开始时间",
            SCHED_GROUP,
            ++orderInGroup,
            ConfigDef.Width.MEDIUM,
            "cron mode"
    ).define(
            CRON_EXPRESS,
            ConfigDef.Type.STRING,
            "0 */2 * * * ?",
            ConfigDef.Importance.HIGH,
            "cron express,默认每2分钟执行一次。支持分钟疾病以上",
            SCHED_GROUP,
            ++orderInGroup,
            ConfigDef.Width.MEDIUM,
            "cron express"
    ).define(
            CRON_CHECK_INTERVAL,
            ConfigDef.Type.LONG,
            1000*10L,
            ConfigDef.Importance.MEDIUM,
            "多久判断cron，单位毫秒",
            SCHED_GROUP,
            ++orderInGroup,
            ConfigDef.Width.LONG,
            "cron check interval"
    );
  }
}
