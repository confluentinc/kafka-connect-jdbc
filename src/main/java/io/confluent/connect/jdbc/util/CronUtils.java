package io.confluent.connect.jdbc.util;

import com.cronutils.model.definition.CronDefinitionBuilder;
import com.cronutils.model.time.ExecutionTime;
import com.cronutils.parser.CronParser;

import java.time.ZoneId;
import java.time.ZonedDateTime;

import static com.cronutils.model.CronType.QUARTZ;

public class CronUtils {

    public static ExecutionTime executionTime(String cronExpression) {
        if (cronExpression == null || cronExpression.isEmpty()) {
            return null;
        }
        final CronParser CRON_PARSER =
                new CronParser(CronDefinitionBuilder.instanceDefinitionFor(QUARTZ));
        ExecutionTime cronExecutionTime = ExecutionTime.forCron(CRON_PARSER.parse(cronExpression));
        return cronExecutionTime;
    }

    public static long nowInMilli() {
        return now().toInstant().toEpochMilli();
    }
    public static ZonedDateTime now() {
        return ZonedDateTime.now(ZoneId.of(ZoneId.SHORT_IDS.get("CTT")));
    }

}
