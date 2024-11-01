package io.confluent.connect.jdbc.sink.metadata;

import io.confluent.connect.jdbc.sink.JdbcSinkConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

public class DateTypeConverter {

    private static final Logger log = LoggerFactory.getLogger(DateTypeConverter.class);
    private final Map<String, Converter> converters;

    public DateTypeConverter() {
        converters = new HashMap<>();
        converters.put("date", new DateConverter());
        converters.put("time", new TimeConverter());
        converters.put("timestamp", new TimestampConverter());
    }

    public String format(JdbcSinkConfig config, String type, String dateValue) {
        Converter converter = converters.get(type.toLowerCase());
        if (converter != null) {
            return converter.format(config, dateValue);
        }
        return dateValue;
    }

    private interface Converter {
        String format(JdbcSinkConfig config, String dateValue);
    }

    private static class DateConverter implements Converter {
        @Override
        public String format(JdbcSinkConfig config, String dateValue) {
            log.info("Converting date...");
            return convertDateFormat(config, dateValue, config.dateFromFormat, config.dateToFormat, config.dateFromTimezone, config.dateToTimezone);
        }
    }

    private static class TimeConverter implements Converter {
        @Override
        public String format(JdbcSinkConfig config, String dateValue) {
            log.info("Converting time...");
            return convertDateFormat(config, dateValue, config.timeFromFormat, config.timeToFormat, config.dateFromTimezone, config.dateToTimezone);
        }
    }

    private static class TimestampConverter implements Converter {
        @Override
        public String format(JdbcSinkConfig config, String dateValue) {
            log.info("Converting timestamp...");
            return convertDateFormat(config, dateValue, config.timestampFromFormat, config.timestampToFormat, config.dateFromTimezone, config.dateToTimezone);
        }
    }

    private static String convertDateFormat(JdbcSinkConfig config, String originalDateString, String originalFormat, String targetFormat, java.util.TimeZone fromTimeZone, java.util.TimeZone toTimeZone) {
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
