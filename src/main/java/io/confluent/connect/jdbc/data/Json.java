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

/**
 * A Kafka Connect logical type that marks a {@code STRING} schema as holding a JSON document.
 *
 * <p>The physical representation is a plain string carrying the raw JSON text, so no value
 * conversion is required (logical form == physical form == {@code String}). The logical name lets
 * downstream components (sinks, SMTs) recognize the field as JSON — for example, a sink can map it
 * to a native {@code JSONB}/{@code JSON} column instead of {@code TEXT}.
 */
public final class Json {

  public static final String LOGICAL_NAME = "io.confluent.connect.jdbc.data.Json";
  public static final int SCHEMA_VERSION = 1;

  private Json() {
  }

  /**
   * Returns a {@link SchemaBuilder} for a JSON-typed STRING field. Callers may further configure
   * it (e.g. {@code optional()}, a default value, or documentation) before building.
   *
   * @return the schema builder; never null
   */
  public static SchemaBuilder builder() {
    return SchemaBuilder.string()
        .name(LOGICAL_NAME)
        .version(SCHEMA_VERSION);
  }

  /**
   * Returns a JSON-typed STRING {@link Schema} with all other default settings.
   *
   * @return the schema; never null
   * @see #builder()
   */
  public static Schema schema() {
    return builder().build();
  }

  /**
   * Returns an optional JSON-typed STRING {@link Schema} with all other default settings, e.g. for
   * use as an array element type.
   *
   * @return the optional schema; never null
   * @see #builder()
   */
  public static Schema optionalSchema() {
    return builder().optional().build();
  }
}
