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

package io.confluent.connect.jdbc.util;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;

import org.apache.kafka.connect.errors.DataException;

import java.util.Map;

/**
 * Serializes a Connect {@code MAP<STRING, STRING>} into a JSON object string for a JDBC
 * JSON/JSONB column. That is the only complex shape the connector maps to {@code jsonb}: it is
 * what a PostgreSQL {@code hstore} column becomes on the topic, either as a Connect map on the
 * sink or as a JSON string on the source.
 */
public final class JsonConverter {

  /** Typed so a non-String key or value fails rather than being silently coerced. */
  private static final ObjectWriter STRING_MAP_WRITER =
      new ObjectMapper().writerFor(new TypeReference<Map<String, String>>() {});

  private JsonConverter() {
  }

  /**
   * Serialize a map of strings into a JSON object string, writing null values as JSON null.
   *
   * @param value the map to serialize; null yields null
   * @return the JSON object text, or null when the value is null
   * @throws DataException if the value is not a map of strings, or has a null key
   */
  public static String connectMapToJson(Object value) {
    if (value == null) {
      return null;
    }
    if (!(value instanceof Map)) {
      throw new DataException("Expected a Map to serialize to JSON but was " + value.getClass());
    }
    try {
      return STRING_MAP_WRITER.writeValueAsString(value);
    } catch (JsonProcessingException e) {
      throw new DataException("Failed to serialize Connect value to JSON", e);
    }
  }
}
