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

import io.confluent.connect.jdbc.util.CronUtils;
import io.confluent.connect.jdbc.util.TaskScheduler;
import io.confluent.connect.jdbc.util.TaskSchedulerFactory;
import org.apache.kafka.connect.source.SourceRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 *
 */
public class JdbcSchedSourceTask extends JdbcSourceTask {
    private static final Logger log = LoggerFactory.getLogger(JdbcSchedSourceTask.class);
    TaskScheduler taskScheduler;

    @Override
    public void start(Map<String, String> properties) {
        super.start(properties);
        this.taskScheduler = TaskSchedulerFactory.create(config.getString("CRON_MODE"));
        this.taskScheduler.init(config);
    }

    @Override
    public List<SourceRecord> poll() throws InterruptedException {

        log.info("poll taskScheduler.trigger-pre currentTime::"+ CronUtils.nowInMilli());
        if (!taskScheduler.trigger()) {
            taskScheduler.sleep();
            log.info("poll taskScheduler.sleep currentTime::"+ CronUtils.nowInMilli());
            return new ArrayList<>();
        }
        log.info("poll taskScheduler.trigger-post currentTime::"+ CronUtils.nowInMilli());
        return super.poll();

    }
}
