/*
 * Copyright 2018 Confluent Inc.
 *
 * Licensed under the Confluent Community License (the "License"); you may not use
 * this file except in compliance with the License.  You may obtain a copy of the
 * License at
 *
 * http://www.confluent.io/confluent-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OF ANY KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations under the License.
 */

package io.confluent.connect.jdbc.util;

import java.math.BigInteger;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.chrono.ChronoZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.TimeZone;
import java.util.concurrent.TimeUnit;

public class DateTimeUtils {

  static final long MILLISECONDS_PER_SECOND = TimeUnit.SECONDS.toMillis(1);
  static final long MICROSECONDS_PER_MILLISECOND = TimeUnit.MILLISECONDS.toMicros(1);
  static final long MICROSECONDS_PER_SECOND = TimeUnit.SECONDS.toMicros(1);
  static final long NANOSECONDS_PER_MILLISECOND = TimeUnit.MILLISECONDS.toNanos(1);
  static final long NANOSECONDS_PER_SECOND = TimeUnit.SECONDS.toNanos(1);
  static final long NANOSECONDS_PER_MICROSECOND = TimeUnit.MICROSECONDS.toNanos(1);
  static final DateTimeFormatter ISO_DATE_TIME_MICROS_FORMAT =
      DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSSSSS");
  static final DateTimeFormatter ISO_DATE_TIME_NANOS_FORMAT =
      DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSSSSSSSS");

  private static final ThreadLocal<Map<TimeZone, Calendar>> TIMEZONE_CALENDARS =
      ThreadLocal.withInitial(HashMap::new);

  private static final ThreadLocal<Map<TimeZone, SimpleDateFormat>> TIMEZONE_DATE_FORMATS =
      ThreadLocal.withInitial(HashMap::new);

  private static final ThreadLocal<Map<TimeZone, SimpleDateFormat>> TIMEZONE_TIME_FORMATS =
      ThreadLocal.withInitial(HashMap::new);

  private static final ThreadLocal<Map<TimeZone, SimpleDateFormat>> TIMEZONE_TIMESTAMP_FORMATS =
      ThreadLocal.withInitial(HashMap::new);

  public static Calendar getTimeZoneCalendar(final TimeZone timeZone) {
    return TIMEZONE_CALENDARS.get().computeIfAbsent(timeZone, GregorianCalendar::new);
  }

  public static String formatDate(Date date, TimeZone timeZone) {
    return TIMEZONE_DATE_FORMATS.get().computeIfAbsent(timeZone, aTimeZone -> {
      SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
      sdf.setTimeZone(aTimeZone);
      return sdf;
    }).format(date);
  }

  public static String formatTime(Date date, TimeZone timeZone) {
    return TIMEZONE_TIME_FORMATS.get().computeIfAbsent(timeZone, aTimeZone -> {
      SimpleDateFormat sdf = new SimpleDateFormat("HH:mm:ss.SSSSSS");
      sdf.setTimeZone(aTimeZone);
      return sdf;
    }).format(date);
  }

  public static String formatTimestamp(Date date, TimeZone timeZone) {
    return TIMEZONE_TIMESTAMP_FORMATS.get().computeIfAbsent(timeZone, aTimeZone -> {
      SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSSSSS");
      sdf.setTimeZone(aTimeZone);
      return sdf;
    }).format(date);
  }

  public static Timestamp formatSinkMicrosTimestamp(Long epochMicros) {
    return Optional.ofNullable(epochMicros)
            .map(micros -> {
              long epochSeconds = micros / MICROSECONDS_PER_SECOND;
              int nanos = (int) (micros % MICROSECONDS_PER_SECOND) * 1_000;
              return Timestamp.from(Instant.ofEpochSecond(epochSeconds, nanos));
            })
            .orElse(null);
  }

  public static Timestamp formatSinkMicrosTimestamp(String epochMicros) {
    return Optional.ofNullable(epochMicros)
            .map(Long::parseLong)
            .map(DateTimeUtils::formatSinkMicrosTimestamp)
            .orElse(null);
  }

  public static Timestamp formatSinkNanosTimestamp(Long epochNanos) {
    return Optional.ofNullable(epochNanos)
            .map(nanos -> {
              long epochSeconds = nanos / NANOSECONDS_PER_SECOND;
              int nanosComponent = (int) (nanos % NANOSECONDS_PER_SECOND);
              return Timestamp.from(Instant.ofEpochSecond(epochSeconds, nanosComponent));
            })
            .orElse(null);
  }

  public static Timestamp formatSinkNanosTimestamp(String epochNanos) {
    return Optional.ofNullable(epochNanos)
            .map(BigInteger::new)
            .map(nanos -> {
              BigInteger seconds = nanos.divide(BigInteger.valueOf(NANOSECONDS_PER_SECOND));
              BigInteger nanosComponent = nanos.mod(BigInteger.valueOf(NANOSECONDS_PER_SECOND));
              return Timestamp.from(Instant.ofEpochSecond(
               seconds.longValue(),
               nanosComponent.intValue()
              ));
            })
            .orElse(null);
  }

  private static Long convertToEpochMicros(Timestamp t) {
    Long epochMillis = TimeUnit.SECONDS.toMicros(t.getTime() / MILLISECONDS_PER_SECOND);
    Long microsComponent = t.getNanos() / NANOSECONDS_PER_MICROSECOND;
    return epochMillis + microsComponent;
  }

  private static String convertToEpochNanosBigInt(Timestamp t) {
    BigInteger seconds = BigInteger.valueOf(t.getTime() / MILLISECONDS_PER_SECOND);
    BigInteger nanos = BigInteger.valueOf(t.getNanos());
    BigInteger totalNanos = seconds.multiply(BigInteger.valueOf(NANOSECONDS_PER_SECOND)).add(nanos);
    return totalNanos.toString();
  }

  /**
   * Get the number of microseconds past epoch of the given {@link Timestamp}.
   *
   * @param timestamp the Java timestamp value
   * @return the epoch nanoseconds
   */
  public static Long toEpochMicros(Timestamp timestamp) {
    return Optional.ofNullable(timestamp)
            .map(DateTimeUtils::convertToEpochMicros)
            .orElse(null);
  }

  private static Long convertToEpochNanos(Timestamp t) {
    Long epochMillis = TimeUnit.SECONDS.toNanos(t.getTime() / MILLISECONDS_PER_SECOND);
    Long nanosInSecond = TimeUnit.NANOSECONDS.toNanos(t.getNanos());
    return epochMillis + nanosInSecond;
  }

  /**
   * Get the number of nanoseconds past epoch of the given {@link Timestamp}.
   *
   * @param timestamp the Java timestamp value
   * @return the epoch nanoseconds
   */
  public static Long toEpochNanos(Timestamp timestamp) {
    return Optional.ofNullable(timestamp)
        .map(DateTimeUtils::convertToEpochNanos)
        .orElse(null);
  }

  /**
   * Get the number of microseconds past epoch of the given {@link Timestamp}.
   *
   * @param timestamp the Java timestamp value
   * @return the epoch microseconds string
   */
  public static String toEpochMicrosString(Timestamp timestamp) {
    return Optional.ofNullable(timestamp)
            .map(DateTimeUtils::convertToEpochMicros)
            .map(String::valueOf)
            .orElse(null);
  }

  /**
   * Get the number of nanoseconds past epoch of the given {@link Timestamp}.
   *
   * @param timestamp the Java timestamp value
   * @return the epoch nanoseconds string
   */
  public static String toEpochNanosString(Timestamp timestamp) {
    return Optional.ofNullable(timestamp)
        .map(DateTimeUtils::convertToEpochNanosBigInt)
        .map(String::valueOf)
        .orElse(null);
  }

  /**
   * Get the iso date-time string with micro precision for the given {@link Timestamp}.
   *
   * @param timestamp the Java timestamp value
   * @param tz the timezone of the source database
   * @return the string iso date time
   */
  public static String toIsoDateMicrosTimeString(Timestamp timestamp, TimeZone tz) {
    return Optional.ofNullable(timestamp)
        .map(Timestamp::toInstant)
        .map(t -> t.atZone(tz.toZoneId()))
        .map(t -> t.format(ISO_DATE_TIME_MICROS_FORMAT))
        .orElse(null);
  }

  /**
   * Get the iso date-time string with nano precision for the given {@link Timestamp}.
   *
   * @param timestamp the Java timestamp value
   * @param tz the timezone of the source database
   * @return the string iso date time
   */
  public static String toIsoDateTimeString(Timestamp timestamp, TimeZone tz) {
    return Optional.ofNullable(timestamp)
        .map(Timestamp::toInstant)
        .map(t -> t.atZone(tz.toZoneId()))
        .map(t -> t.format(ISO_DATE_TIME_NANOS_FORMAT))
        .orElse(null);
  }

  /**
   * Get {@link Timestamp} from epoch with micro precision
   *
   * @param micros epoch micro in long
   * @return the equivalent java sql Timestamp
   */
  public static Timestamp toMicrosTimestamp(Long micros) {
    return Optional.ofNullable(micros)
        .map(
            m -> {
              Timestamp ts = new Timestamp(micros / MICROSECONDS_PER_MILLISECOND);
              long remainderMicros = micros % MICROSECONDS_PER_MILLISECOND;
              ts.setNanos(ts.getNanos() + (int)(remainderMicros * NANOSECONDS_PER_MICROSECOND));
              return ts;
            })
        .orElse(null);
  }

  /**
   * Get {@link Timestamp} from epoch with micros precision
   *
   * @param micros epoch micros in string
   * @return the equivalent java sql Timestamp
   */
  public static Timestamp toMicrosTimestamp(String micros) throws NumberFormatException {
    return Optional.ofNullable(micros)
            .map(Long::parseLong)
            .map(DateTimeUtils::toMicrosTimestamp)
            .orElse(null);
  }

  /**
   * Get {@link Timestamp} from epoch with nano precision
   *
   * @param nanos epoch nanos in long
   * @return the equivalent java sql Timestamp
   */
  public static Timestamp toTimestamp(Long nanos) {
    return Optional.ofNullable(nanos)
        .map(n -> {
          Timestamp ts = new Timestamp(nanos / NANOSECONDS_PER_MILLISECOND);
          ts.setNanos((int)(nanos % NANOSECONDS_PER_SECOND));
          return ts;
        })
        .orElse(null);
  }

  /**
   * Get {@link Timestamp} from epoch with nano precision
   *
   * @param nanos epoch nanos BigInteger
   * @return the equivalent java sql Timestamp
   */
  public static Timestamp toTimestamp(BigInteger nanos) {
    return Optional.ofNullable(nanos)
        .map(
            n -> {
              long milliseconds =
                  n.divide(BigInteger.valueOf(NANOSECONDS_PER_MILLISECOND)).longValue();
              int nanoseconds = n.mod(BigInteger.valueOf(NANOSECONDS_PER_SECOND)).intValue();
              Timestamp ts = new Timestamp(milliseconds);
              ts.setNanos(nanoseconds);
              return ts;
            })
        .orElse(null);
  }

  /**
   * Get {@link Timestamp} from epoch with nano precision
   *
   * @param nanos epoch nanos in string
   * @return the equivalent java sql Timestamp
   */
  public static Timestamp toTimestamp(String nanos) {
    return Optional.ofNullable(nanos)
            .map(BigInteger::new)
            .map(DateTimeUtils::toTimestamp)
            .orElse(null);
  }

  /**
   * Get {@link Timestamp} from epoch with micro precision
   *
   * @param isoDT iso dateTime format "yyyy-MM-dd'T'HH:mm:ss.SSSSSS"
   * @param tz the timezone of the source database
   * @return the equivalent java sql Timestamp
   */
  public static Timestamp toTimestampFromIsoDateMicrosTime(String isoDT, TimeZone tz) {
    return Optional.ofNullable(isoDT)
        .map(i -> LocalDateTime.parse(isoDT, ISO_DATE_TIME_MICROS_FORMAT))
        .map(t -> t.atZone(tz.toZoneId()))
        .map(ChronoZonedDateTime::toInstant)
        .map(Timestamp::from)
        .orElse(null);
  }

  /**
   * Get {@link Timestamp} from epoch with nano precision
   *
   * @param isoDT iso dateTime format "yyyy-MM-dd'T'HH:mm:ss.SSSSSSSSS"
   * @param tz the timezone of the source database
   * @return the equivalent java sql Timestamp
   */
  public static Timestamp toTimestampFromIsoDateTime(String isoDT, TimeZone tz) {
    return Optional.ofNullable(isoDT)
        .map(i -> LocalDateTime.parse(isoDT, ISO_DATE_TIME_NANOS_FORMAT))
        .map(t -> t.atZone(tz.toZoneId()))
        .map(ChronoZonedDateTime::toInstant)
        .map(Timestamp::from)
        .orElse(null);
  }

  private DateTimeUtils() {
  }
}
