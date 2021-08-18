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

package io.confluent.connect.jdbc.data;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;


import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;

/**
 * Utility class for managing nano timestamp representations
 * 
 */
public class NanoTimestampString {
  public static final String SCHEMA_NAME = "io.confluent.connect.jdbc.data.NanoTimestampString";
  private static DateTimeFormatter formatter = 
      DateTimeFormatter.ofPattern("uuuu-MM-dd'T'HH:mm:ss.SSSSSSSSS");

  public static SchemaBuilder builder() {
    return SchemaBuilder.string()
                        .name(SCHEMA_NAME)
                        .version(1);
  }

  public static Schema schema() {
    return builder().build();
  }

  // public static long toEpochNanos(Object value) {
  //   LocalDateTime dateTime = Conversions.toLocalDateTime(value);
  //   return Conversions.toEpochNanos(dateTime);
  // }

  public static String toNanoString(Object value) {
    LocalDateTime dateTime = Conversions.toLocalDateTime(value);
    return dateTime.format(formatter);
  } 

  private NanoTimestampString() {
  }
}