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
import java.util.HashMap;
import java.util.Map;
import java.util.TimeZone;

public class DateTimeUtils {

  private static final ThreadLocal<Map<TimeZone, Calendar>> TIMEZONE_CALENDARS =
      new ThreadLocal<Map<TimeZone, Calendar>>() {
        @Override
        public Map<TimeZone, Calendar> initialValue() {
          return new HashMap<>();
        }
      };

  private static final ThreadLocal<Map<TimeZone, SimpleDateFormat>> TIMEZONE_DATE_FORMATS =
      new ThreadLocal<Map<TimeZone, SimpleDateFormat>>() {
        @Override
        public Map<TimeZone, SimpleDateFormat> initialValue() {
          return new HashMap<>();
        }
      };

  private static final ThreadLocal<Map<TimeZone, SimpleDateFormat>> TIMEZONE_TIME_FORMATS =
      new ThreadLocal<Map<TimeZone, SimpleDateFormat>>() {
        @Override
        public Map<TimeZone, SimpleDateFormat> initialValue() {
          return new HashMap<>();
        }
      };

  private static final ThreadLocal<Map<TimeZone, SimpleDateFormat>> TIMEZONE_TIMESTAMP_FORMATS =
      new ThreadLocal<Map<TimeZone, SimpleDateFormat>>() {
        @Override
        public Map<TimeZone, SimpleDateFormat> initialValue() {
          return new HashMap<>();
        }
    };

  public static Calendar getTimeZoneCalendar(final TimeZone timeZone) {
    Map<TimeZone, Calendar> map = TIMEZONE_CALENDARS.get();
    if (!map.containsKey(timeZone)) {
      map.put(timeZone, new GregorianCalendar(timeZone));
    }
    return map.get(timeZone);
  }

  public static String formatDate(Date date, TimeZone timeZone) {
    Map<TimeZone, SimpleDateFormat> map = TIMEZONE_DATE_FORMATS.get();
    if (!map.containsKey(timeZone)) {
      SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
      sdf.setTimeZone(timeZone);
      map.put(timeZone, sdf);
    }
    return map.get(timeZone).format(date);
  }

  public static String formatTime(Date date, TimeZone timeZone) {
    Map<TimeZone, SimpleDateFormat> map = TIMEZONE_TIME_FORMATS.get();
    if (!map.containsKey(timeZone)) {
      SimpleDateFormat sdf = new SimpleDateFormat("HH:mm:ss.SSS");
      sdf.setTimeZone(timeZone);
      map.put(timeZone, sdf);
    }
    return map.get(timeZone).format(date);
  }

  public static String formatTimestamp(Date date, TimeZone timeZone) {
    Map<TimeZone, SimpleDateFormat> map = TIMEZONE_TIMESTAMP_FORMATS.get();
    if (!map.containsKey(timeZone)) {
      SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
      sdf.setTimeZone(timeZone);
      map.put(timeZone, sdf);
    }
    return map.get(timeZone).format(date);
  }

  private DateTimeUtils() {
  }
}
