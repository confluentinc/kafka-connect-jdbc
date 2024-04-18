package io.confluent.connect.jdbc.sink.metadata;

import io.confluent.connect.jdbc.gp.GpDataIngestionService;
import io.confluent.connect.jdbc.sink.JdbcSinkConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.SimpleDateFormat;
import java.util.Date;

public enum DateType {
// Logger

    // add array
    DATE("date"){
        @Override
        public String format(JdbcSinkConfig config, String dateValue) {
            log.info("DateType: DATE");
            return convertDateFormat(config, dateValue, config.dateFromFormat, config.dateToFormat);
        }
    },
    TIME("time"){
        @Override
        public String format(JdbcSinkConfig config, String dateValue) {
            log.info("DateType: TIME");
            return convertDateFormat(config, dateValue, config.timeFromFormat, config.timeToFormat);
        }
    },
    TIMESTAMP("timestamp"){
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
    private SimpleDateFormat originalFormatter;
    private SimpleDateFormat targetFormatter;
    public String convertDateFormat(JdbcSinkConfig config, String originalDateString, String originalFormat, String targetFormat) {
        if (config.printDebugLogs) {
            // log all values in one line
            log.info("Converting: originalDateString: " + originalDateString + " originalFormat: " + originalFormat + " targetFormat: " + targetFormat);
        }
        if (originalDateString == null || originalFormat == null || targetFormat == null || originalDateString.isEmpty() || originalFormat.isEmpty() || targetFormat.isEmpty() ||  "null".equalsIgnoreCase(originalDateString))
            return originalDateString;

        // Define the original and target date formatters
       // String[] formats = originalFormat.split("#");

        // for`
      //  for(String format: formats) {
        if(originalFormatter == null) {
            originalFormatter = new SimpleDateFormat(originalFormat);
            if (config.dateFromTimezone != null)
                originalFormatter.setTimeZone(config.dateFromTimezone);
        }

        if(targetFormatter == null ) {
            targetFormatter = new SimpleDateFormat(targetFormat);
            if (config.dateToTimezone != null)
                targetFormatter.setTimeZone(config.dateToTimezone);
        }
            try {
                // Parse the original date string
                Date parsedDate = originalFormatter.parse(originalDateString);
                // Format the parsed date using the target formatter
                return targetFormatter.format(parsedDate);

            } catch (Exception e) {
                // If parsing fails with the current formatter, try the next one
                e.printStackTrace();
            }
     //   }


        return originalDateString;
    }
    DateType(String type) {
        this.type = type;
    }
    private String type;
    public String getType() {
        return type;
    }
    // find enum
    public static DateType fromString(String text) {
         log.info("Getting DateType from string: "+text);
        if(text != null) {
            text = text.toLowerCase();
            if(text.startsWith(DATE.type)) {
               return DATE;
           }else if(text.startsWith(TIMESTAMP.type)) {
               return TIMESTAMP;
           }else if(text.startsWith(TIME.type)){
                return TIME;
            }
        }

        return null;
    }

}
