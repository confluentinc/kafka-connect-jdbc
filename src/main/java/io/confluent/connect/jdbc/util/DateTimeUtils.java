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

  /**
   * Converts the instant represented by the input java.util.Date's milliseconds value
   * from proleptic Gregorian calendar to the instant representing the same date/time
   * in hybrid Julian/Gregorian calendar.
   * 
   * <p>This method addresses the Julian-Gregorian calendar cutover issue where the same
   * instant (milliseconds since epoch) would provide different day/month/year values
   * depending on whether java.util.Date or LocalDateTime is used for dates before
   * October 15, 1582 (Gregorian cutover date).</p>
   * 
   * <p><strong>Conversion Process:</strong>
   * <ol>
   *   <li>Input java.util.Date uses proleptic Gregorian calendar (java.time semantics)</li>
   *   <li>Extract date/time fields using legacy Calendar (hybrid Julian/Gregorian)</li>
   *   <li>Create new java.sql.Date with same field values but legacy calendar interpretation</li>
   * </ol></p>
   * 
   * <p>The legacy calendar uses a hybrid Julian/Gregorian system where:
   * <ul>
   *   <li>Dates before October 15, 1582 use the Julian calendar</li>
   *   <li>Dates on or after October 15, 1582 use the Gregorian calendar</li>
   *   <li>This matches the behavior of java.util.Calendar and java.util.Date</li>
   * </ul></p>
   * 
   * @param sqlDate the source java.util.Date representing the
   * @param zoneId the timezone to use for the conversion
   * @return the converted java.sql.Date using legacy calendar semantics, or null if input is null
   */
  public static java.sql.Date convertToLegacyDate(java.util.Date sqlDate, ZoneId zoneId) {
    if (sqlDate == null) {
      return null;
    }
    java.sql.Timestamp ts =
        convertToLegacyTimestamp(new java.sql.Timestamp(sqlDate.getTime()), zoneId);
    return new java.sql.Date(ts.getTime());
  }

  /**
   * Converts the instant represented by the input java.sql.Date's milliseconds value
   * from hybrid Julian/Gregorian calendar to the instant representing the same date/time 
   * in proleptic Gregorian calendar.
   * 
   * <p>This method converts timestamps using the proleptic Gregorian calendar system
   * (as used by java.time APIs) rather than the hybrid Julian/Gregorian calendar
   * used by legacy java.util.Date classes.</p>
   * 
   * <p><strong>Conversion Process:</strong>
   * <ol>
   *   <li>Input java.sql.Date uses hybrid Julian/Gregorian calendar (legacy semantics)</li>
   *   <li>Extract date/time fields using legacy Calendar (hybrid Julian/Gregorian)</li>
   *   <li>Convert to proleptic Gregorian using java.time.LocalDateTime</li>
   *   <li>Create new java.sql.Date with proleptic Gregorian interpretation</li>
   * </ol></p>
   * 
   * <p>The modern calendar uses:
   * <ul>
   *   <li>Proleptic Gregorian calendar for all dates (extends Gregorian rules backward)</li>
   *   <li>ISO-8601 year numbering (year 0 = 1 BC, year -1 = 2 BC, etc.)</li>
   *   <li>This matches the behavior of java.time.LocalDateTime and related classes</li>
   * </ul></p>
   * 
   * @param sqlDate the source java.sql.Date to convert (can be null)
   * @param zoneId the timezone to use for the conversion
   * @return the converted java.sql.Date using modern calendar semantics, or null if input is null
   */
  public static java.sql.Date convertToModernDate(java.sql.Date sqlDate, ZoneId zoneId) {
    if (sqlDate == null) {
      return null;
    }
    java.sql.Timestamp ts =
        convertToModernTimestamp(new java.sql.Timestamp(sqlDate.getTime()), zoneId);
    return new java.sql.Date(ts.getTime());
  }


  /**
   * Converts the instant represented by the input java.sql.Date's milliseconds value
   * from hybrid Julian/Gregorian calendar to the instant representing the same date/time
   * in proleptic Gregorian calendar.
   * 
   * <p>This method addresses the fundamental difference between how legacy Java date/time
   * classes (java.util.Date, java.util.Calendar) and modern Java time classes 
   * (java.time.LocalDateTime) interpret the same instant in time, particularly for
   * dates before the Gregorian calendar cutover (October 15, 1582).</p>
   * 
   * <p><strong>The Problem:</strong> For dates before the Julian-Gregorian cutover,
   * the same instant (milliseconds since epoch) produces different day/month/year/hour/
   * minute/second values when interpreted by:
   * <ul>
   *   <li><strong>Legacy classes:</strong> Use hybrid Julian/Gregorian calendar with cutover at
   *   Oct 15, 1582</li>
   *   <li><strong>Modern classes:</strong> Use proleptic Gregorian calendar (Gregorian rules
   *   extended backward)</li>
   * </ul></p>
   * 
   * <p><strong>Conversion Process:</strong>
   * <ol>
   *   <li>Read the timestamp using legacy Calendar (hybrid Julian/Gregorian) in the target
   *   timezone</li>
   *   <li>Extract individual date/time fields (year, month, day, hour, minute, second)</li>
   *   <li>Convert BC/AD era and year to ISO proleptic year numbering (1 BC → year 0, 2 BC →
   *   year -1)</li>
   *   <li>Create a modern LocalDateTime with these field values</li>
   *   <li>Convert back to timestamp using modern semantics</li>
   * </ol></p>
   * 
   * @param ts the source timestamp using legacy calendar semantics (can be null)
   * @param zoneId the timezone to use for field extraction and conversion
   * @return a new timestamp with the same field values but using modern calendar semantics, or
   *     null if input is null
   */
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
    // Convert BC/AD era system to ISO proleptic year numbering
    // In legacy calendar: 1 BC = era BC + year 1, 2 BC = era BC + year 2
    // In ISO system: 1 BC = year 0, 2 BC = year -1, etc.
    int isoYear = (era == java.util.GregorianCalendar.BC) ? 1 - year : year; // 1 BC->0, 2 BC->-1

    // Create modern LocalDateTime using the extracted field values and converted year
    // Note: Calendar.MONTH is 0-based, LocalDateTime month is 1-based
    java.time.LocalDateTime ldt = java.time.LocalDateTime.of(
        isoYear,                                          // ISO proleptic year
        cal.get(java.util.Calendar.MONTH) + 1,          // Convert 0-based to 1-based month
        cal.get(java.util.Calendar.DAY_OF_MONTH),       // Day of month (1-based in both)
        cal.get(java.util.Calendar.HOUR_OF_DAY),        // Hour in 24-hour format
        cal.get(java.util.Calendar.MINUTE),             // Minute
        cal.get(java.util.Calendar.SECOND),             // Second
        ts.getNanos()                                    // Preserve original nanosecond precision
    );

    // Convert the LocalDateTime back to epoch milliseconds using modern calendar semantics
    long epochMillis = ldt.atZone(zoneId).toInstant().toEpochMilli();
    return new Timestamp(epochMillis);
  }



  /**
   * Converts the instant represented by the input java.util.Date's milliseconds value
   * from proleptic Gregorian calendar to the instant representing the same date/time
   * in hybrid Julian/Gregorian calendar.
   * 
   * <p>This method performs the inverse operation of {@link
   * #convertToModernTimestamp(java.sql.Timestamp, ZoneId)},
   * converting from the proleptic Gregorian calendar system used by java.time classes
   * to the hybrid Julian/Gregorian calendar system used by legacy java.util classes.</p>
   * 
   * <p><strong>The Problem:</strong> When modern java.time classes create timestamps,
   * they use a proleptic Gregorian calendar that extends Gregorian calendar rules
   * backward to all historical dates. However, legacy Java classes use a hybrid
   * system that switches from Julian to Gregorian calendar at October 15, 1582.
   * This creates inconsistencies for historical dates.</p>
   * 
   * <p><strong>Conversion Process:</strong>
   * <ol>
   *   <li>Extract date/time fields from the timestamp using modern LocalDateTime
   *   (proleptic Gregorian)</li>
   *   <li>Convert ISO proleptic year numbering to BC/AD era system (year 0 → 1 BC,
   *   year -1 → 2 BC)</li>
   *   <li>Set these field values in a legacy GregorianCalendar</li>
   *   <li>Let the legacy calendar apply its hybrid Julian/Gregorian rules</li>
   *   <li>Create a new timestamp from the legacy calendar's epoch milliseconds</li>
   * </ol></p>
   * 
   * @param ts the source timestamp using modern calendar semantics (can be null)
   * @param zoneId the timezone to use for field extraction and conversion
   * @return a new timestamp with equivalent field values using legacy calendar semantics, or null
   *     if input is null
   */
  public static Timestamp convertToLegacyTimestamp(java.sql.Timestamp ts, ZoneId zoneId) {
    if (ts == null) {
      return null;
    }
    // Extract date/time fields using modern proleptic Gregorian calendar
    LocalDateTime ldt = LocalDateTime.ofInstant(ts.toInstant(), zoneId);

    // Create a legacy GregorianCalendar to apply hybrid Julian/Gregorian rules
    GregorianCalendar cal = new GregorianCalendar(TimeZone.getTimeZone(zoneId), Locale.ROOT);
    cal.setLenient(false);  // Strict date validation
    cal.clear();            // Clear all fields to avoid interference

    // Convert ISO proleptic year to BC/AD era system
    int y = ldt.getYear();
    if (y <= 0) {
      // ISO proleptic: year 0 = 1 BC, year -1 = 2 BC, etc.
      cal.set(Calendar.ERA, GregorianCalendar.BC);
      cal.set(Calendar.YEAR, 1 - y);   // Convert: 0 -> 1, -1 -> 2, -99 -> 100
    } else {
      // Positive years map directly to AD era
      cal.set(Calendar.ERA, GregorianCalendar.AD);
      cal.set(Calendar.YEAR, y);
    }

    // Set the remaining date/time fields
    cal.set(Calendar.MONTH, ldt.getMonthValue() - 1);        // Convert 1-based to 0-based month
    cal.set(Calendar.DAY_OF_MONTH, ldt.getDayOfMonth());     // Day of month (1-based in both)
    cal.set(Calendar.HOUR_OF_DAY, ldt.getHour());            // Hour in 24-hour format
    cal.set(Calendar.MINUTE, ldt.getMinute());               // Minute
    cal.set(Calendar.SECOND, ldt.getSecond());               // Second
    // Set milliseconds component (nanoseconds will be set separately to preserve precision)
    cal.set(Calendar.MILLISECOND, ldt.getNano() / (int) NANOSECONDS_PER_MILLISECOND);

    // Create timestamp from legacy calendar's epoch milliseconds
    Timestamp out = new Timestamp(cal.getTimeInMillis());
    // Preserve full nanosecond precision from the original timestamp
    out.setNanos(ldt.getNano());
    return out;
  }

  /**
   * <p>This is a convenience overload of {@link #convertToLegacyTimestamp(
   * java.sql.Timestamp, ZoneId)} that accepts a java.util.Date as input. The conversion
   * process is identical:</p>
   * <ol>
   *   <li>Convert the java.util.Date to a java.sql.Timestamp</li>
   *   <li>Apply the full legacy timestamp conversion logic</li>
   *   <li>Return the result as a java.sql.Timestamp</li>
   * </ol>
   * 
   * <p>This method ensures that date/time field values remain consistent with legacy
   * java.util.Calendar interpretation, particularly for dates before the Julian-Gregorian
   * calendar cutover (October 15, 1582).</p>
   * 
   * @param ts the source java.util.Date to convert (can be null)
   * @param zoneId the timezone to use for field extraction and conversion
   * @return a new java.sql.Timestamp with equivalent field values using legacy calendar semantics,
   *     or null if input is null @see #convertToLegacyTimestamp(java.sql.Timestamp, ZoneId)
   */
  public static java.sql.Timestamp convertToLegacyTimestamp(java.util.Date ts, ZoneId zoneId) {
    if (ts == null) {
      return null;
    }
    // Delegate to the main conversion method after wrapping in java.sql.Timestamp
    return new java.sql.Timestamp(
        convertToLegacyTimestamp(
            new java.sql.Timestamp(
                ts.getTime()  // Preserve original epoch milliseconds
            ),
            zoneId
        ).getTime()
    );
  }

  private DateTimeUtils() {
  }
}
