package io.confluent.connect.jdbc.util;

import com.cronutils.model.time.ExecutionTime;
import io.confluent.connect.jdbc.source.JdbcSchedSourceTaskConfig;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.connect.errors.ConnectException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.ZonedDateTime;
import java.util.Optional;

public class PeriodTaskScheduler implements TaskScheduler {
    private static final Logger log = LoggerFactory.getLogger(PeriodTaskScheduler.class);
    private ExecutionTime cronExecutionTime;
    private String cronExpress;
    private long nextExecutionTime = 0L;
    private long cronCheckInterval = 1000L;


    public void init(AbstractConfig config) {
        cronCheckInterval = config.getLong(JdbcSchedSourceTaskConfig.CRON_CHECK_INTERVAL);
        initExecutionTime(config);
        setNextExecutionTime();
        log.info("OnceTaskScheduler init-post: " + this.toString());
    }

    void initExecutionTime(AbstractConfig config) {
        this.cronExpress = config.getString(JdbcSchedSourceTaskConfig.CRON_EXPRESS);
        this.cronExecutionTime = CronUtils.executionTime(cronExpress);
        if (cronExecutionTime == null) {
            throw new ConnectException("cron 初始化失败。 cronExecutionTime 为 null");
        }
    }

    /**
     *
     */
    public boolean trigger() {
        if (nextExecutionTime > 0) {
            if (nextExecutionTime - now().toInstant().toEpochMilli() > 0) {
                return false;
            }
        }
        setNextExecutionTime();
        return true;
    }

    public void sleep() throws InterruptedException {
        Thread.sleep(cronCheckInterval);
    }

    private long nowInMilli() {
        return now().toInstant().toEpochMilli();
    }

    private void setNextExecutionTime() {

        Optional<ZonedDateTime> nextDateTime = this.cronExecutionTime.nextExecution(now());
        if (nextDateTime.isPresent()) {
            this.nextExecutionTime = nextDateTime.get().toInstant().toEpochMilli();
        } else {
            throw new RuntimeException("Cannot decide next execution time.");
        }
    }

    @Override
    public String toString() {
        return "OnceTaskScheduler{" +
                "cronExecutionTime=" + cronExecutionTime +
                ", cronExpress='" + cronExpress + '\'' +
                ", nextExecutionTime=" + nextExecutionTime +
                ", cronCheckInterval=" + cronCheckInterval +
                '}';
    }
}
