package io.confluent.connect.jdbc.sink.metadata;

import io.confluent.connect.jdbc.sink.JdbcSinkConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

public class DateTypeConverter {
    private final Logger log = LoggerFactory.getLogger(DateTypeConverter.class);

    private final JdbcSinkConfig config;
    public DateTypeConverter(JdbcSinkConfig config) {
        this.config = config;
    }



        public String convertDate(String dateValue) {
            log.info("Converting date...");
            return convertDateFormat(dateValue, config.dateFromFormat, config.dateToFormat, config.dateFromTimezone, config.dateToTimezone);
        }

        public String convertTime(String dateValue) {
            log.info("Converting time...");
            return convertDateFormat(dateValue, config.timeFromFormat, config.timeToFormat, config.dateFromTimezone, config.dateToTimezone);
        }

        public String convertTimeStamp(String dateValue) {
            log.info("Converting timestamp...");
            return convertDateFormat(dateValue, config.timestampFromFormat, config.timestampToFormat, config.dateFromTimezone, config.dateToTimezone);
        }

    private synchronized String convertDateFormat(String originalDateString, String originalFormat, String targetFormat, java.util.TimeZone fromTimeZone, java.util.TimeZone toTimeZone) {
        if (config.printDebugLogs) {
            log.info("Converting: originalDateString: {} originalFormat: {} targetFormat: {}", originalDateString, originalFormat, targetFormat);
        }
        if (originalDateString == null || originalFormat == null || targetFormat == null || originalDateString.isEmpty() || originalFormat.isEmpty() || targetFormat.isEmpty() || "null".equalsIgnoreCase(originalDateString)) {
            return originalDateString;
        }

        SimpleDateFormat originalFormatter = new SimpleDateFormat(originalFormat);
        if (fromTimeZone != null) originalFormatter.setTimeZone(fromTimeZone);

        SimpleDateFormat targetFormatter = new SimpleDateFormat(targetFormat);
        if (toTimeZone != null) targetFormatter.setTimeZone(toTimeZone);

        try {
            Date parsedDate = originalFormatter.parse(originalDateString);
            return targetFormatter.format(parsedDate);
        } catch (Exception e) {
            log.error("Date conversion failed", e);
            return originalDateString;
        }
    }
}
