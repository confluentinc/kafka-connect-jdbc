package io.confluent.connect.jdbc.util;

import com.cronutils.model.definition.CronDefinitionBuilder;
import com.cronutils.model.time.ExecutionTime;
import com.cronutils.parser.CronParser;
import org.apache.commons.lang3.StringUtils;
import org.junit.Test;

import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.Optional;

import static com.cronutils.model.CronType.QUARTZ;

public class CronTest {

    @Test
    public void test() {
        long nextExecutionTime = -1L;
        final CronParser CRON_PARSER =
                new CronParser(CronDefinitionBuilder.instanceDefinitionFor(QUARTZ));
//        String cronExpression = "*/2 * * * * ?";
        String cronExpression = "0 0 17 * * ?";
        ExecutionTime cronExecutionTime =
                StringUtils.isBlank(cronExpression)
                        ? null
                        : ExecutionTime.forCron(CRON_PARSER.parse(cronExpression));


        ZonedDateTime now = now();
        long current = now.toInstant().toEpochMilli();
        Optional<ZonedDateTime> nextDateTime = cronExecutionTime.nextExecution(now);
        if (nextDateTime.isPresent()) {
            nextExecutionTime = nextDateTime.get().toInstant().toEpochMilli();
        } else {
            throw new RuntimeException("Cannot decide next execution time.");
        }
        System.out.println("current: "+current);
        System.out.println("current second: "+ now.getSecond());
        System.out.println("next: "+nextExecutionTime);
        System.out.println("divide val: "+(nextExecutionTime-current));


//        current: 1620727532791
//        current second: 32
//        next: 1620730800000
//        divide val: 3267209


//        current: 1620727641129
//        current second: 21
//        next: 1620730800000
//        divide val: 3158871

//        current: 1620728003040
//        current second: 23
//        next: 1620810000000
//        divide val: 81996960




        /*

        开始：18  结束：19     ===》 18~19
        1，当前：11-17 ，11-18，11-19  =>skip，sleep
        2，当前：11-18 ，12-18，11-19  =>执行
        3，当前：11-20 ，12-18，11-19  =>skip，sleep





         */


    }

    private ZonedDateTime now() {
        return ZonedDateTime.now(ZoneId.of(ZoneId.SHORT_IDS.get("CTT")));
    }

}
