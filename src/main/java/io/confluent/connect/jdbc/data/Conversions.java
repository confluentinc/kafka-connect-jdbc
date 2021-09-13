// /*
//  * Copyright 2018 Confluent Inc.
//  *
//  * Licensed under the Confluent Community License (the "License"); you may not use
//  * this file except in compliance with the License.  You may obtain a copy of the
//  * License at
//  *
//  * http://www.confluent.io/confluent-community-license
//  *
//  * Unless required by applicable law or agreed to in writing, software
//  * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
//  * WARRANTIES OF ANY KIND, either express or implied.  See the License for the
//  * specific language governing permissions and limitations under the License.
//  */

// package io.confluent.connect.jdbc.data;

// // import java.time.Duration;
// // import java.time.Instant;
// import java.time.LocalDate;
// import java.time.LocalDateTime;
// // import java.time.LocalTime;
// // import java.time.OffsetDateTime;
// // import java.time.ZoneOffset;
// import java.util.concurrent.TimeUnit;

// // Ref: https://github.com/debezium/debezium/blob/master/debezium-core/src/main/java/io/debezium/time/Conversions.java

// final class Conversions {

//   static final long MILLISECONDS_PER_SECOND = TimeUnit.SECONDS.toMillis(1);
//   static final long MICROSECONDS_PER_MILLISECOND = TimeUnit.MILLISECONDS.toMicros(1);
//   static final long NANOSECONDS_PER_MILLISECOND = TimeUnit.MILLISECONDS.toNanos(1);
//   static final long NANOSECONDS_PER_MICROSECOND = TimeUnit.MICROSECONDS.toNanos(1);
//   static final long NANOSECONDS_PER_SECOND = TimeUnit.SECONDS.toNanos(1);
//   static final long NANOSECONDS_PER_DAY = TimeUnit.DAYS.toNanos(1);
//   static final long SECONDS_PER_DAY = TimeUnit.DAYS.toSeconds(1);
//   static final long MICROSECONDS_PER_DAY = TimeUnit.DAYS.toMicros(1);
//   static final LocalDate EPOCH = LocalDate.ofEpochDay(0);

//   private Conversions() {
//   }

//   @SuppressWarnings("deprecation")
//   // Todo: determine if we need all of this, or just the Timestamp > LocalDateTime conversion
//   // Also todo: see if we can reduce the number of conversions here
//   protected static LocalDateTime toLocalDateTime(Object obj) {
//     if (obj == null) {
//       return null;
//     }
//     // if (obj instanceof OffsetDateTime) {
//     //   return ((OffsetDateTime) obj).toLocalDateTime();
//     // }
//     // if (obj instanceof Instant) {
//     //   return ((Instant) obj).atOffset(ZoneOffset.UTC).toLocalDateTime();
//     // }
//     // if (obj instanceof LocalDateTime) {
//     //   return (LocalDateTime) obj;
//     // }
//     // if (obj instanceof LocalDate) {
//     //   LocalDate date = (LocalDate) obj;
//     //   return LocalDateTime.of(date, LocalTime.MIDNIGHT);
//     // }
//     // if (obj instanceof LocalTime) {
//     //   LocalTime time = (LocalTime) obj;
//     //   return LocalDateTime.of(EPOCH, time);
//     // }
//     // if (obj instanceof java.sql.Date) {
//     //   java.sql.Date sqlDate = (java.sql.Date) obj;
//     //   LocalDate date = sqlDate.toLocalDate();
//     //   return LocalDateTime.of(date, LocalTime.MIDNIGHT);
//     // }
//     // if (obj instanceof java.sql.Time) {
//     //   LocalTime localTime = toLocalTime(obj);
//     //   return LocalDateTime.of(EPOCH, localTime);
//     // }
//     if (obj instanceof java.sql.Timestamp) {
//       java.sql.Timestamp timestamp = (java.sql.Timestamp) obj;
//       return LocalDateTime.of(timestamp.getYear() + 1900,
//                               timestamp.getMonth() + 1,
//                               timestamp.getDate(),
//                               timestamp.getHours(),
//                               timestamp.getMinutes(),
//                               timestamp.getSeconds(),
//                               timestamp.getNanos());
//     }
//     // if (obj instanceof java.util.Date) {
//     //   java.util.Date date = (java.util.Date) obj;
//     //   long millis = (int) (date.getTime() % Conversions.MILLISECONDS_PER_SECOND);
//     //   if (millis < 0) {
//     //     millis = Conversions.MILLISECONDS_PER_SECOND + millis;
//     //   }
//     //   int nanosOfSecond = (int) (millis * Conversions.NANOSECONDS_PER_MILLISECOND);
//     //   return LocalDateTime.of(date.getYear() + 1900,
//     //     date.getMonth() + 1,
//     //     date.getDate(),
//     //     date.getHours(),
//     //     date.getMinutes(),
//     //     date.getSeconds(),
//     //     nanosOfSecond);
//     // }
//     throw new IllegalArgumentException("Unable to convert to LocalTime from unexpected value '" 
//       + obj + "' of type " + obj.getClass().getName());
//   }
  

//   static long toEpochNanos(LocalDateTime timestamp) {
//     long nanoInDay = timestamp.toLocalTime().toNanoOfDay();
//     long nanosOfDay = toEpochNanos(timestamp.toLocalDate());
//     return nanosOfDay + nanoInDay;
//   }

//   /**
//    * Get the number of nanoseconds past epoch of the given {@link LocalDate}.
//    * 
//    * @param date the Java date value
//    * @return the epoch nanoseconds
//    */
//   static long toEpochNanos(LocalDate date) {
//     long epochDay = date.toEpochDay();
//     return epochDay * Conversions.NANOSECONDS_PER_DAY;
//   }

//   // static String toEpochNanosString(LocalDate date) {

//   // }
// }