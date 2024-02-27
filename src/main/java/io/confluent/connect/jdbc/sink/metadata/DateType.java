package io.confluent.connect.jdbc.sink.metadata;

import io.confluent.connect.jdbc.gp.GpDataIngestionService;
import io.confluent.connect.jdbc.sink.JdbcSinkConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.SimpleDateFormat;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.Arrays;
import java.util.Date;

public enum DateType {
// Logger

    // add array
    DATE(new String[]{"date", "date without time zone"}){
        @Override
        public String format(JdbcSinkConfig config, String dateValue) {
            log.info("DateType: DATE");
            return convertDateFormat(config, dateValue, config.dateFromFormat, config.dateToFormat);
        }
    },
    DATETIME(new String[]{"time", "time without time zone"}){
        @Override
        public String format(JdbcSinkConfig config, String dateValue) {
            log.info("DateType: DATETIME");
            return convertDateFormat(config, dateValue, config.timeFromFormat, config.timeToFormat);
        }
    },
    TIMESTAMP(new String[]{"timestamp", "timestamp with time zone"}){
        @Override
        public String format(JdbcSinkConfig config, String dateValue) {
            log.info("DateType: TIMESTAMP");
            return convertDateFormat(config, dateValue, config.timestampFromFormat, config.timestampToFormat);
        }
    };
    private static final Logger log = LoggerFactory.getLogger(GpDataIngestionService.class);
    public String format(JdbcSinkConfig config, String dateValue) {
        return dateValue;
    }
    public String convertDateFormat(JdbcSinkConfig config, String originalDateString, String originalFormat, String targetFormat) {
        if(config.deleteEnabled){
            // log all values in one line
            log.info("Converting: originalDateString: " + originalDateString + " originalFormat: " + originalFormat + " targetFormat: " + targetFormat);

        }
        // Define the original and target date formatters
        String[] formats = originalFormat.split("#");

        // for`
        for(String format: formats) {
            SimpleDateFormat originalFormatter = new SimpleDateFormat(format);
            originalFormatter.setTimeZone(config.timeZone);

            SimpleDateFormat targetFormatter = new SimpleDateFormat(targetFormat);
            targetFormatter.setTimeZone(config.timeZone);

            try {
                // Parse the original date string
                Date parsedDate = originalFormatter.parse(originalDateString);
                LocalDate originalDate = parsedDate.toInstant().atZone(config.timeZone.toZoneId()).toLocalDate();

                // Format the parsed date using the target formatter
                return targetFormatter.format(Date.from(originalDate.atStartOfDay(config.timeZone.toZoneId()).toInstant()));

            } catch (Exception e) {
                // If parsing fails with the current formatter, try the next one
                e.printStackTrace();
            }
        }


        return originalDateString;
    }
    DateType(String[] type) {
        this.type = type;
    }
    private String[] type;
    public String[] getType() {
        return type;
    }
    // find enum
    public static DateType fromString(String text) {
        for (DateType b : DateType.values()) {
            if (Arrays.stream(b.type).anyMatch(text::equals)) {
                return b;
            }
        }
        return null;
    }

}
