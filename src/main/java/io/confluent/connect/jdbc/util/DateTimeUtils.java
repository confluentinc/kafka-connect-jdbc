/*
 *  Copyright 2016 Confluent Inc.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package io.confluent.connect.jdbc.util;

import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.TimeZone;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DateTimeUtils {
  
  private static final Logger log = LoggerFactory.getLogger(DateTimeUtils.class);

  public static final String UTC_TIMEZONE = "utc";
  public static final String JVM_TIMEZONE = "jvm";
  
  public static final TimeZone UTC = TimeZone.getTimeZone("UTC");

  public static final ThreadLocal<Calendar> UTC_CALENDAR = new ThreadLocal<Calendar>() {
    @Override
    protected Calendar initialValue() {
      return new GregorianCalendar(TimeZone.getTimeZone("UTC"));
    }
  };

  private static final ThreadLocal<SimpleDateFormat> UTC_DATE_FORMAT
      = new ThreadLocal<SimpleDateFormat>() {
        protected SimpleDateFormat initialValue() {
          SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
          sdf.setTimeZone(UTC);
          return sdf;
        }
      };   

  private static final ThreadLocal<SimpleDateFormat> UTC_TIME_FORMAT
      = new ThreadLocal<SimpleDateFormat>() {
        protected SimpleDateFormat initialValue() {
          SimpleDateFormat sdf = new SimpleDateFormat("HH:mm:ss.SSS");
          sdf.setTimeZone(UTC);
          return sdf;
        }
      };

  private static final ThreadLocal<SimpleDateFormat> UTC_TIMESTAMP_FORMAT
      = new ThreadLocal<SimpleDateFormat>() {
        protected SimpleDateFormat initialValue() {
          SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
          sdf.setTimeZone(UTC);
          return sdf;
        }
      };
  
  private static final ThreadLocal<SimpleDateFormat> DEFAULT_TIMESTAMP_FORMAT
      = new ThreadLocal<SimpleDateFormat>() {
        protected SimpleDateFormat initialValue() {
          SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
          return sdf;
        }
      };

  public static String formatUtcDate(Date date) {
    return UTC_DATE_FORMAT.get().format(date);
  }

  public static String formatUtcTime(Date date) {
    return UTC_TIME_FORMAT.get().format(date);
  }

  public static String formatUtcTimestamp(Date date) {
    return UTC_TIMESTAMP_FORMAT.get().format(date);
  }
  
  public static String formatDefaultTimestamp(Date date) {
    return DEFAULT_TIMESTAMP_FORMAT.get().format(date);
  }
  
  private static ThreadLocal<Calendar> specificTimezoneCalendar = null;
  
  public static ThreadLocal<Calendar> getSpecificTimezoneCalendarInstance(final String dbTimeZone) {
    if (specificTimezoneCalendar == null) {
      synchronized (ThreadLocal.class) {
        if (specificTimezoneCalendar == null) {
          specificTimezoneCalendar = new ThreadLocal<Calendar>() {
            @Override
            protected Calendar initialValue() {
              return new GregorianCalendar(TimeZone.getTimeZone(dbTimeZone));
            }
          };
        }
      }
    }
    return specificTimezoneCalendar;
  }
  
  public static Calendar getCalendarWithTimeZone(final String dbTimeZone) {
    if (dbTimeZone == null || dbTimeZone.isEmpty()
        || dbTimeZone.equals(DateTimeUtils.UTC_TIMEZONE)) {
      log.info("using using default UTC Calendar");
      return DateTimeUtils.UTC_CALENDAR.get();
    } else if (dbTimeZone.trim().equals(DateTimeUtils.JVM_TIMEZONE)) {
      log.info("using using jvm timezone");
      return null;
    } else {
      log.info("using using " + dbTimeZone + " timezone");
      return DateTimeUtils.getSpecificTimezoneCalendarInstance(dbTimeZone).get();
    }
  }

}
