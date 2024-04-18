package io.confluent.connect.jdbc;


import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.TimeZone;

public class TestClass {

    public static void main(String[] args) {
        TestClass testClass = new TestClass();

        // Test the convertDateFormat method
        testClass.convertDateFormat(TimeZone.getTimeZone("PST"), TimeZone.getTimeZone("Asia/Karachi"), "2023-06-06 17:50:10", "yyyy-MM-dd HH:mm:ss", "yyyy-MM-dd HH:mm:ss");


    }


    public void convertDateFormat(TimeZone dateFromTimezone, TimeZone dateToTimezone, String originalDateString, String originalFormat, String targetFormat) {

        // Define the original and target date formatters

        // for`
            SimpleDateFormat originalFormatter = new SimpleDateFormat(originalFormat);
            if(dateFromTimezone != null)
                originalFormatter.setTimeZone(dateFromTimezone);

            SimpleDateFormat targetFormatter = new SimpleDateFormat(targetFormat);
            if(dateToTimezone != null)
                targetFormatter.setTimeZone(dateToTimezone);

            try {
                // Parse the original date string
                Date parsedDate = originalFormatter.parse(originalDateString);
                // Format the parsed date using the target formatter
                String targetDate = targetFormatter.format(parsedDate);
                // print all values in one line
                System.out.println("Original Date: " + originalDateString + " Original Format: " + originalFormat + " Target Format: " + targetFormat + " Target Date: " + targetDate);
            } catch (Exception e) {
                // If parsing fails with the current formatter, try the next one
                e.printStackTrace();
            }

    }
}
