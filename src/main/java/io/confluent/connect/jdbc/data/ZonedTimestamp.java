/*
 * Copyright 2026 Confluent Inc.
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

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;

import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.Date;

/**
 * A Kafka Connect logical type that marks a {@code STRING} schema as holding a timezone-aware
 * timestamp, rendered as an ISO-8601 string with offset (for example {@code 2025-06-10T13:00:00Z}).
 *
 * <p>It is the analog of PostgreSQL's {@code timestamp with time zone} (and Debezium's
 * {@code io.debezium.time.ZonedTimestamp}). Unlike the built-in
 * {@link org.apache.kafka.connect.data.Timestamp} logical type — which carries only an instant and
 * no zone, and therefore lands in a plain {@code TIMESTAMP} column on the sink — this type
 * preserves the zone designation end-to-end so the value can be restored to a native
 * {@code TIMESTAMPTZ} column. PostgreSQL stores {@code timestamptz} as a UTC instant, so the
 * rendered string is always normalized to UTC ({@code Z}); the absolute instant is preserved.
 */
public final class ZonedTimestamp {

  public static final String LOGICAL_NAME = "io.confluent.connect.jdbc.data.ZonedTimestamp";
  public static final int SCHEMA_VERSION = 1;

  private static final DateTimeFormatter FORMATTER = DateTimeFormatter.ISO_OFFSET_DATE_TIME;

  private ZonedTimestamp() {
  }

  /**
   * Returns a {@link SchemaBuilder} for a zoned-timestamp STRING field. Callers may further
   * configure it (e.g. {@code optional()}) before building.
   *
   * @return the schema builder; never null
   */
  public static SchemaBuilder builder() {
    return SchemaBuilder.string()
        .name(LOGICAL_NAME)
        .version(SCHEMA_VERSION);
  }

  /**
   * Returns a zoned-timestamp STRING {@link Schema} with all other default settings.
   *
   * @return the schema; never null
   * @see #builder()
   */
  public static Schema schema() {
    return builder().build();
  }

  /**
   * Returns an optional zoned-timestamp STRING {@link Schema} with all other default settings, e.g.
   * for use as an array element type.
   *
   * @return the optional schema; never null
   * @see #builder()
   */
  public static Schema optionalSchema() {
    return builder().optional().build();
  }

  /**
   * Render an instant as an ISO-8601 offset string normalized to UTC, e.g.
   * {@code 2025-06-10T13:00:00Z}. Returns null for a null input.
   *
   * @param instant the instant to render; may be null
   * @return the ISO-8601 UTC string, or null
   */
  public static String toIsoString(Date instant) {
    return instant == null ? null : FORMATTER.format(instant.toInstant().atOffset(ZoneOffset.UTC));
  }
}
