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
import java.time.ZonedDateTime;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.chrono.ChronoZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.HashMap;
import java.util.Locale;
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

  private static final DateTimeFormatter DATE_FORMATTER =
      DateTimeFormatter.ofPattern("yyyy-MM-dd");
  private static final DateTimeFormatter TIME_FORMATTER =
      DateTimeFormatter.ofPattern("HH:mm:ss.SSS");
  private static final DateTimeFormatter TIMESTAMP_FORMATTER =
      DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS");

  public static Calendar getZoneIdCalendar(final ZoneId zoneId) {
    TimeZone timeZone = TimeZone.getTimeZone(zoneId);
    return TIMEZONE_CALENDARS.get().computeIfAbsent(timeZone, GregorianCalendar::new);
  }

  public static String formatDate(Date date, ZoneId zoneId) {
    ZonedDateTime zoned = date.toInstant().atZone(zoneId);
    return DATE_FORMATTER.format(zoned);
  }

  public static String formatTime(Date date, ZoneId zoneId) {
    ZonedDateTime zoned = date.toInstant().atZone(zoneId);
    return TIME_FORMATTER.format(zoned);
  }

  public static String formatTimestamp(Date date, ZoneId zoneId) {
    ZonedDateTime zoned = date.toInstant().atZone(zoneId);
    return TIMESTAMP_FORMATTER.format(zoned);
  }

  public static Timestamp formatSinkMicrosTimestamp(Long epochMicros) {
    return Optional.ofNullable(epochMicros)
            .map(micros -> {
              long epochSeconds = micros / MICROSECONDS_PER_SECOND;
              int nanos = (int) ((micros % MICROSECONDS_PER_SECOND) * NANOSECONDS_PER_MICROSECOND);
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
   * @param zoneId the timezone of the source database
   * @return the string iso date time
   */
  public static String toIsoDateMicrosTimeString(Timestamp timestamp, ZoneId zoneId) {
    return Optional.ofNullable(timestamp)
        .map(Timestamp::toInstant)
        .map(t -> t.atZone(zoneId))
        .map(t -> t.format(ISO_DATE_TIME_MICROS_FORMAT))
        .orElse(null);
  }

  /**
   * Get the iso date-time string with nano precision for the given {@link Timestamp}.
   *
   * @param timestamp the Java timestamp value
   * @param zoneId the timezone of the source database
   * @return the string iso date time
   */
  public static String toIsoDateTimeString(Timestamp timestamp, ZoneId zoneId) {
    return Optional.ofNullable(timestamp)
        .map(Timestamp::toInstant)
        .map(t -> t.atZone(zoneId))
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
              ts.setNanos(ts.getNanos() + (int) (remainderMicros * NANOSECONDS_PER_MICROSECOND));
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
          ts.setNanos((int) (nanos % NANOSECONDS_PER_SECOND));
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
   * @param zoneId the timezone of the source database
   * @return the equivalent java sql Timestamp
   */
  public static Timestamp toTimestampFromIsoDateMicrosTime(String isoDT, ZoneId zoneId) {
    return Optional.ofNullable(isoDT)
        .map(i -> LocalDateTime.parse(isoDT, ISO_DATE_TIME_MICROS_FORMAT))
        .map(t -> t.atZone(zoneId))
        .map(ChronoZonedDateTime::toInstant)
        .map(Timestamp::from)
        .orElse(null);
  }

  /**
   * Get {@link Timestamp} from epoch with nano precision
   *
   * @param isoDT iso dateTime format "yyyy-MM-dd'T'HH:mm:ss.SSSSSSSSS"
   * @param zoneId the timezone of the source database
   * @return the equivalent java sql Timestamp
   */
  public static Timestamp toTimestampFromIsoDateTime(String isoDT, ZoneId zoneId) {
    return Optional.ofNullable(isoDT)
        .map(i -> LocalDateTime.parse(isoDT, ISO_DATE_TIME_NANOS_FORMAT))
        .map(t -> t.atZone(zoneId))
        .map(ChronoZonedDateTime::toInstant)
        .map(Timestamp::from)
        .orElse(null);
  }

  public static java.sql.Date convertToLegacyDate(java.util.Date sqlDate, ZoneId zoneId) {
    if (sqlDate == null) {
      return null;
    }
    java.sql.Timestamp ts =
        convertToLegacyTimestamp(new java.sql.Timestamp(sqlDate.getTime()), zoneId);
    return new java.sql.Date(ts.getTime());
  }

  public static java.sql.Date convertToModernDate(java.sql.Date sqlDate, ZoneId zoneId) {
    if (sqlDate == null) {
      return null;
    }
    java.sql.Timestamp ts =
        convertToModernTimestamp(new java.sql.Timestamp(sqlDate.getTime()), zoneId);
    return new java.sql.Date(ts.getTime());
  }


  public static java.sql.Timestamp convertToModernTimestamp(java.sql.Timestamp ts, ZoneId zoneId) {
    if (ts == null) {
      return null;
    }

    // Use legacy calendar to read fields (hybrid Julian/Gregorian), in the target zone
    java.util.Calendar cal = java.util.Calendar.getInstance(java.util.TimeZone.getTimeZone(zoneId));
    cal.setTimeInMillis(ts.getTime());

    // Map (ERA, YEAR) -> proleptic ISO year for java.time
    int era  = cal.get(java.util.Calendar.ERA);
    int year = cal.get(java.util.Calendar.YEAR);
    int isoYear = (era == java.util.GregorianCalendar.BC) ? 1 - year : year; // 1 BC->0, 2 BC->-1

    java.time.LocalDateTime ldt = java.time.LocalDateTime.of(
        isoYear,
        cal.get(java.util.Calendar.MONTH) + 1,
        cal.get(java.util.Calendar.DAY_OF_MONTH),
        cal.get(java.util.Calendar.HOUR_OF_DAY),
        cal.get(java.util.Calendar.MINUTE),
        cal.get(java.util.Calendar.SECOND),
        ts.getNanos()
    );

    long epochMillis = ldt.atZone(zoneId).toInstant().toEpochMilli();
    return new Timestamp(epochMillis);
  }



  public static Timestamp convertToLegacyTimestamp(java.sql.Timestamp ts, ZoneId zoneId) {
    if (ts == null) {
      return null;
    }
    LocalDateTime ldt = LocalDateTime.ofInstant(ts.toInstant(), zoneId);

    GregorianCalendar cal = new GregorianCalendar(TimeZone.getTimeZone(zoneId), Locale.ROOT);
    cal.setLenient(false);
    cal.clear();

    int y = ldt.getYear();
    if (y <= 0) {
      // proleptic 0 == 1 BC, -1 == 2 BC, etc.
      cal.set(Calendar.ERA, GregorianCalendar.BC);
      cal.set(Calendar.YEAR, 1 - y);   // 0 -> 1, -1 -> 2, …
    } else {
      cal.set(Calendar.ERA, GregorianCalendar.AD);
      cal.set(Calendar.YEAR, y);
    }

    cal.set(Calendar.MONTH, ldt.getMonthValue() - 1);
    cal.set(Calendar.DAY_OF_MONTH, ldt.getDayOfMonth());
    cal.set(Calendar.HOUR_OF_DAY, ldt.getHour());
    cal.set(Calendar.MINUTE, ldt.getMinute());
    cal.set(Calendar.SECOND, ldt.getSecond());
    cal.set(Calendar.MILLISECOND, ldt.getNano() / (int) NANOSECONDS_PER_MILLISECOND);

    Timestamp out = new Timestamp(cal.getTimeInMillis());
    out.setNanos(ldt.getNano());
    return out;
  }

  public static java.sql.Timestamp convertToLegacyTimestamp(java.util.Date ts, ZoneId zoneId) {
    if (ts == null) {
      return null;
    }
    return new java.sql.Timestamp(
        convertToLegacyTimestamp(
            new java.sql.Timestamp(
                ts.getTime()
            ),
            zoneId
        ).getTime()
    );
  }

  private DateTimeUtils() {
  }
}
