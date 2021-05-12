package io.confluent.connect.jdbc.util;

import org.apache.kafka.common.config.AbstractConfig;

import java.time.ZoneId;
import java.time.ZonedDateTime;

public interface TaskScheduler {

    /**
     * 初始化
     * @param config
     */
    void init(AbstractConfig config);

    /**
     * kafka connect 框架：当task 被调度起来后会一直循环调用poll-trigger-send方法。
     * @return
     */
    default boolean trigger() {
        return true;
    }

    default void sleep() throws InterruptedException {

    }


    default ZonedDateTime now() {
        return ZonedDateTime.now(ZoneId.of(ZoneId.SHORT_IDS.get("CTT")));
    }

    enum Mode {
        NATIVE,

        //一次结束
        ONCE,

        SCHEDULER,

    }
}
