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

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.time.LocalDateTime;
import java.time.Instant;
import java.time.Month;
import java.sql.Timestamp;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.Calendar;
import java.util.GregorianCalendar;
import java.util.Locale;
import java.util.TimeZone;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public class DateTimeUtilsTest {

  private ZoneId utcZoneId = ZoneOffset.UTC;
  private TimeZone originalDefaultTimeZone;

  @Before
  public void setUp() {
    // Store original default timezone to restore it after tests
    originalDefaultTimeZone = TimeZone.getDefault();
  }

  @After
  public void tearDown() {
    // Restore original default timezone
    TimeZone.setDefault(originalDefaultTimeZone);
  }

  @Test
  public void testTimestampToNanosLong() {
    Timestamp timestamp = Timestamp.from(Instant.now());
    timestamp.setNanos(141362049);
    long nanos = DateTimeUtils.toEpochNanos(timestamp);
    assertEquals(timestamp, DateTimeUtils.toTimestamp(nanos));
  }

  @Test
  public void testTimestampToNanosLongNull() {
    Long nanos = DateTimeUtils.toEpochNanos(null);
    assertNull(nanos);
    Timestamp timestamp = DateTimeUtils.toTimestamp((Long) null);
    assertNull(timestamp);
  }

  @Test
  public void testTimestampToNanosString() {
    Timestamp timestamp = Timestamp.from(Instant.now());
    timestamp.setNanos(141362049);
    String nanos = DateTimeUtils.toEpochNanosString(timestamp);
    assertEquals(timestamp, DateTimeUtils.toTimestamp(nanos));
  }

  @Test
  public void testTimestampToNanosStringNull() {
    String nanos = DateTimeUtils.toEpochNanosString(null);
    assertNull(nanos);
    Timestamp timestamp = DateTimeUtils.toTimestamp((String) null);
    assertNull(timestamp);
  }

  @Test
  public void testTimestampToNanosStringLargeDate() {
    LocalDateTime localDateTime =
     LocalDateTime.of(9999, Month.DECEMBER, 31, 23, 59, 59);
    Timestamp timestamp = Timestamp.valueOf(localDateTime);
    String nanos = DateTimeUtils.toEpochNanosString(timestamp);
    assertEquals(timestamp, DateTimeUtils.toTimestamp(nanos));
  }

  @Test
  public void testTimestampToIsoDateTime() {
    Timestamp timestamp = Timestamp.from(Instant.now());
    timestamp.setNanos(141362049);
    String isoDateTime = DateTimeUtils.toIsoDateTimeString(timestamp, utcZoneId);
    assertEquals("141362049", isoDateTime.substring(isoDateTime.lastIndexOf('.') + 1));
    assertEquals(timestamp, DateTimeUtils.toTimestampFromIsoDateTime(isoDateTime, utcZoneId));
  }

  @Test
  public void testTimestampToIsoDateTimeNanosLeading0s() {
    Timestamp timestamp = Timestamp.from(Instant.now());
    timestamp.setNanos(1);
    String isoDateTime = DateTimeUtils.toIsoDateTimeString(timestamp, utcZoneId);
    assertEquals("000000001", isoDateTime.substring(isoDateTime.lastIndexOf('.') + 1));
    assertEquals(timestamp, DateTimeUtils.toTimestampFromIsoDateTime(isoDateTime, utcZoneId));
  }

  @Test
  public void testTimestampToIsoDateTimeNanosTrailing0s() {
    Timestamp timestamp = Timestamp.from(Instant.now());
    timestamp.setNanos(100);
    String isoDateTime = DateTimeUtils.toIsoDateTimeString(timestamp, utcZoneId);
    assertEquals("000000100", isoDateTime.substring(isoDateTime.lastIndexOf('.') + 1));
    assertEquals(timestamp, DateTimeUtils.toTimestampFromIsoDateTime(isoDateTime, utcZoneId));
  }

  @Test
  public void testTimestampToIsoDateTimeNanos0s() {
    Timestamp timestamp = Timestamp.from(Instant.now());
    timestamp.setNanos(0);
    String isoDateTime = DateTimeUtils.toIsoDateTimeString(timestamp, utcZoneId);
    assertEquals("000000000", isoDateTime.substring(isoDateTime.lastIndexOf('.') + 1));
    assertEquals(timestamp, DateTimeUtils.toTimestampFromIsoDateTime(isoDateTime, utcZoneId));
  }

  @Test
  public void testTimestampToIsoDateTimeNull() {
    String isoDateTime = DateTimeUtils.toIsoDateTimeString(null, utcZoneId);
    assertNull(isoDateTime);
    Timestamp timestamp = DateTimeUtils.toTimestampFromIsoDateTime(null, utcZoneId);
    assertNull(timestamp);
  }

  @Test
  public void testLegacyModernRoundTripAcrossJvmAndInputTimezones() {
    String[] jvmZones = new String[] {
        "UTC",
        "Etc/GMT+7",
        "UTC+14",
        "Etc/GMT-7",
        "UTC-14",
    };

    String[] inputZones = jvmZones;

    for (String jvmZoneId : jvmZones) {
      TimeZone.setDefault(TimeZone.getTimeZone(jvmZoneId));

      for (String inputZoneId : inputZones) {
        ZoneId zin = ZoneId.of(inputZoneId);

        // 1) Create Timestamp1 representing 0001-01-01 12:34:56.789 in Zin using legacy hybrid calendar
        GregorianCalendar cal = new GregorianCalendar(TimeZone.getTimeZone(zin), Locale.ROOT);
        cal.setLenient(false);
        cal.clear();
        cal.set(Calendar.ERA, GregorianCalendar.AD);
        cal.set(Calendar.YEAR, 1);
        cal.set(Calendar.MONTH, Calendar.JANUARY);
        cal.set(Calendar.DAY_OF_MONTH, 1);
        cal.set(Calendar.HOUR_OF_DAY, 12);
        cal.set(Calendar.MINUTE, 34);
        cal.set(Calendar.SECOND, 56);
        cal.set(Calendar.MILLISECOND, 789);
        Timestamp timestamp1 = new Timestamp(cal.getTimeInMillis());
        timestamp1.setNanos(789_000_000);

        // 2) convertToModernTimestamp(Timestamp1, Zin) -> Timestamp2
        Timestamp timestamp2 = DateTimeUtils.convertToModernTimestamp(timestamp1, zin);

        // 3) Construct LocalDateTime from Timestamp2's instant in Zin; must equal original fields
        LocalDateTime ldtInZin = LocalDateTime.ofInstant(timestamp2.toInstant(), zin);
        assertEquals("Year mismatch for JVM timezone: " + jvmZoneId + ", input timezone: " + inputZoneId, 1, ldtInZin.getYear());
        assertEquals("Month mismatch for JVM timezone: " + jvmZoneId + ", input timezone: " + inputZoneId, 1, ldtInZin.getMonthValue());
        assertEquals("Day mismatch for JVM timezone: " + jvmZoneId + ", input timezone: " + inputZoneId, 1, ldtInZin.getDayOfMonth());
        assertEquals("Hour mismatch for JVM timezone: " + jvmZoneId + ", input timezone: " + inputZoneId, 12, ldtInZin.getHour());
        assertEquals("Minute mismatch for JVM timezone: " + jvmZoneId + ", input timezone: " + inputZoneId, 34, ldtInZin.getMinute());
        assertEquals("Second mismatch for JVM timezone: " + jvmZoneId + ", input timezone: " + inputZoneId, 56, ldtInZin.getSecond());
        assertEquals("Nanos mismatch for JVM timezone: " + jvmZoneId + ", input timezone: " + inputZoneId, 789_000_000, ldtInZin.getNano());

        // 4) convertToLegacyTimestamp(Timestamp2, Zin) -> should equal Timestamp1
        Timestamp backToLegacy = DateTimeUtils.convertToLegacyTimestamp(timestamp2, zin);
        assertEquals("Round-trip legacy mismatch for JVM timezone: " + jvmZoneId + ", input timezone: " + inputZoneId,
            timestamp1.getTime(), backToLegacy.getTime());
        assertEquals("Round-trip nanos mismatch for JVM timezone: " + jvmZoneId + ", input timezone: " + inputZoneId,
            timestamp1.getNanos(), backToLegacy.getNanos());
      }
    }
  }

  @Test
  public void testLegacyModernRoundTripAcrossJvmAndInputTimezones_BC() {
    String[] jvmZones = new String[] {
        "UTC",
        "Etc/GMT+7",
        "UTC+14",
        "Etc/GMT-7",
        "UTC-14",
    };

    String[] inputZones = jvmZones;

    for (String jvmZoneId : jvmZones) {
      TimeZone.setDefault(TimeZone.getTimeZone(jvmZoneId));

      for (String inputZoneId : inputZones) {
        ZoneId zin = ZoneId.of(inputZoneId);

        // 1) Create Timestamp1 representing 0001-12-30 12:34:56.789 BC in Zin using legacy hybrid calendar
        GregorianCalendar cal = new GregorianCalendar(TimeZone.getTimeZone(zin), Locale.ROOT);
        cal.setLenient(false);
        cal.clear();
        cal.set(Calendar.ERA, GregorianCalendar.BC);
        cal.set(Calendar.YEAR, 1);
        cal.set(Calendar.MONTH, Calendar.DECEMBER);
        cal.set(Calendar.DAY_OF_MONTH, 30);
        cal.set(Calendar.HOUR_OF_DAY, 12);
        cal.set(Calendar.MINUTE, 34);
        cal.set(Calendar.SECOND, 56);
        cal.set(Calendar.MILLISECOND, 789);
        Timestamp timestamp1 = new Timestamp(cal.getTimeInMillis());
        timestamp1.setNanos(789_000_000);

        // 2) convertToModernTimestamp(Timestamp1, Zin) -> Timestamp2
        Timestamp timestamp2 = DateTimeUtils.convertToModernTimestamp(timestamp1, zin);

        // 3) Construct LocalDateTime from Timestamp2's instant in Zin; must equal original fields (proleptic)
        LocalDateTime ldtInZin = LocalDateTime.ofInstant(timestamp2.toInstant(), zin);
        // For 1 BC in legacy, proleptic ISO year is 0
        assertEquals("Year mismatch for JVM timezone: " + jvmZoneId + ", input timezone: " + inputZoneId, 0, ldtInZin.getYear());
        assertEquals("Month mismatch for JVM timezone: " + jvmZoneId + ", input timezone: " + inputZoneId, 12, ldtInZin.getMonthValue());
        assertEquals("Day mismatch for JVM timezone: " + jvmZoneId + ", input timezone: " + inputZoneId, 30, ldtInZin.getDayOfMonth());
        assertEquals("Hour mismatch for JVM timezone: " + jvmZoneId + ", input timezone: " + inputZoneId, 12, ldtInZin.getHour());
        assertEquals("Minute mismatch for JVM timezone: " + jvmZoneId + ", input timezone: " + inputZoneId, 34, ldtInZin.getMinute());
        assertEquals("Second mismatch for JVM timezone: " + jvmZoneId + ", input timezone: " + inputZoneId, 56, ldtInZin.getSecond());
        assertEquals("Nanos mismatch for JVM timezone: " + jvmZoneId + ", input timezone: " + inputZoneId, 789_000_000, ldtInZin.getNano());

        // 4) convertToLegacyTimestamp(Timestamp2, Zin) -> should equal Timestamp1
        Timestamp backToLegacy = DateTimeUtils.convertToLegacyTimestamp(timestamp2, zin);
        assertEquals("Round-trip legacy mismatch for JVM timezone: " + jvmZoneId + ", input timezone: " + inputZoneId,
            timestamp1.getTime(), backToLegacy.getTime());
        assertEquals("Round-trip nanos mismatch for JVM timezone: " + jvmZoneId + ", input timezone: " + inputZoneId,
            timestamp1.getNanos(), backToLegacy.getNanos());
      }
    }
  }

  @Test
  public void testConvertToModernDateAcrossJvmAndInputTimezones() {
    String[] jvmZones = new String[] {
        "UTC",
        "Etc/GMT+7",
        "UTC+14",
        "Etc/GMT-7",
        "UTC-14",
    };

    String[] inputZones = jvmZones;

    for (String jvmZoneId : jvmZones) {
      TimeZone.setDefault(TimeZone.getTimeZone(jvmZoneId));

      for (String inputZoneId : inputZones) {
        ZoneId zin = ZoneId.of(inputZoneId);

        // Build a legacy java.sql.Date representing 0001-01-01 in Zin using hybrid calendar
        GregorianCalendar cal = new GregorianCalendar(TimeZone.getTimeZone(zin), Locale.ROOT);
        cal.setLenient(false);
        cal.clear();
        cal.set(Calendar.ERA, GregorianCalendar.AD);
        cal.set(Calendar.YEAR, 1);
        cal.set(Calendar.MONTH, Calendar.JANUARY);
        cal.set(Calendar.DAY_OF_MONTH, 1);
        java.sql.Date legacyDate = new java.sql.Date(cal.getTimeInMillis());

        // Convert to modern date (proleptic behavior)
        java.sql.Date modernDate = DateTimeUtils.convertToModernDate(legacyDate, zin);

        // Validate by interpreting modernDate epoch millis at Zin start-of-day
        LocalDateTime ldtInZin = LocalDateTime.ofInstant(
            java.time.Instant.ofEpochMilli(modernDate.getTime()), zin);
        assertEquals("Year mismatch for JVM timezone: " + jvmZoneId + ", input timezone: " + inputZoneId, 1, ldtInZin.getYear());
        assertEquals("Month mismatch for JVM timezone: " + jvmZoneId + ", input timezone: " + inputZoneId, 1, ldtInZin.getMonthValue());
        assertEquals("Day mismatch for JVM timezone: " + jvmZoneId + ", input timezone: " + inputZoneId, 1, ldtInZin.getDayOfMonth());
        // Time portion should reflect midnight for a Date (00:00:00.000) in that zone
        assertEquals("Hour mismatch for JVM timezone: " + jvmZoneId + ", input timezone: " + inputZoneId, 0, ldtInZin.getHour());
        assertEquals("Minute mismatch for JVM timezone: " + jvmZoneId + ", input timezone: " + inputZoneId, 0, ldtInZin.getMinute());
        assertEquals("Second mismatch for JVM timezone: " + jvmZoneId + ", input timezone: " + inputZoneId, 0, ldtInZin.getSecond());
        assertEquals("Nano mismatch for JVM timezone: " + jvmZoneId + ", input timezone: " + inputZoneId, 0, ldtInZin.getNano());
      }
    }
  }

  @Test
  public void testConvertToModernDateAcrossJvmAndInputTimezones_BC() {
    String[] jvmZones = new String[]{
        "UTC",
        "Etc/GMT+7",
        "UTC+14",
        "Etc/GMT-7",
        "UTC-14",
    };

    String[] inputZones = jvmZones;

    for (String jvmZoneId : jvmZones) {
      TimeZone.setDefault(TimeZone.getTimeZone(jvmZoneId));

      for (String inputZoneId : inputZones) {
        ZoneId zin = ZoneId.of(inputZoneId);

        // Build a legacy java.sql.Date representing 0001-01-01 BC in Zin using hybrid calendar
        GregorianCalendar cal = new GregorianCalendar(TimeZone.getTimeZone(zin), Locale.ROOT);
        cal.setLenient(false);
        cal.clear();
        cal.set(Calendar.ERA, GregorianCalendar.BC);
        cal.set(Calendar.YEAR, 1);
        cal.set(Calendar.MONTH, Calendar.JANUARY);
        cal.set(Calendar.DAY_OF_MONTH, 1);
        java.sql.Date legacyDate = new java.sql.Date(cal.getTimeInMillis());

        // Convert to modern date (proleptic behavior)
        java.sql.Date modernDate = DateTimeUtils.convertToModernDate(legacyDate, zin);

        // Validate by interpreting modernDate epoch millis at Zin start-of-day
        LocalDateTime ldtInZin = LocalDateTime.ofInstant(
            java.time.Instant.ofEpochMilli(modernDate.getTime()), zin);
        // For 1 BC in legacy, proleptic ISO year is 0
        assertEquals("Year mismatch for JVM timezone: " + jvmZoneId + ", input timezone: "
                     + inputZoneId, 0, ldtInZin.getYear());
        assertEquals("Month mismatch for JVM timezone: " + jvmZoneId + ", input timezone: "
                     + inputZoneId, 1, ldtInZin.getMonthValue());
        assertEquals("Day mismatch for JVM timezone: " + jvmZoneId + ", input timezone: "
                     + inputZoneId, 1, ldtInZin.getDayOfMonth());
        // Time portion should reflect midnight for a Date (00:00:00.000) in that zone
        assertEquals("Hour mismatch for JVM timezone: " + jvmZoneId + ", input timezone: "
                     + inputZoneId, 0, ldtInZin.getHour());
        assertEquals("Minute mismatch for JVM timezone: " + jvmZoneId + ", input timezone: "
                     + inputZoneId, 0, ldtInZin.getMinute());
        assertEquals("Second mismatch for JVM timezone: " + jvmZoneId + ", input timezone: "
                     + inputZoneId, 0, ldtInZin.getSecond());
      }
    }
  }
}