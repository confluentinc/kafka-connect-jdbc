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
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;

import org.apache.kafka.connect.errors.DataException;

import java.util.Map;

/**
 * Serializes a Connect {@code MAP<STRING, STRING>} into a JSON object string for a JDBC
 * JSON/JSONB column. That is the only complex shape the connector maps to {@code jsonb}: it is
 * what a PostgreSQL {@code hstore} column becomes on the topic, either as a Connect map on the
 * sink or as a JSON string on the source.
 */
public final class JsonConverter {

  private static final JsonNodeFactory JSON_NODE_FACTORY = JsonNodeFactory.instance;

  private static final ObjectMapper MAPPER = new ObjectMapper();

  private JsonConverter() {
  }

  /**
   * Serialize a map of strings into a JSON object string, writing null values as JSON null.
   *
   * @param value the map to serialize; null yields null
   * @return the JSON object text, or null when the value is null
   * @throws DataException if the value is not a map of strings, or has a null key
   */
  public static String connectValueToJson(Object value) {
    if (value == null) {
      return null;
    }
    if (!(value instanceof Map)) {
      throw new DataException("Expected a Map to serialize to JSON but was " + value.getClass());
    }
    try {
      return MAPPER.writeValueAsString(mapToJsonNode((Map<?, ?>) value));
    } catch (JsonProcessingException e) {
      throw new DataException("Failed to serialize Connect value to JSON", e);
    }
  }

  private static ObjectNode mapToJsonNode(Map<?, ?> map) {
    ObjectNode object = JSON_NODE_FACTORY.objectNode();
    for (Map.Entry<?, ?> entry : map.entrySet()) {
      if (entry.getKey() == null) {
        throw new DataException("Cannot serialize a Connect MAP with null keys to JSON");
      }
      Object value = entry.getValue();
      if (value == null) {
        object.set(entry.getKey().toString(), JSON_NODE_FACTORY.nullNode());
      } else if (value instanceof String) {
        object.set(entry.getKey().toString(), JSON_NODE_FACTORY.textNode((String) value));
      } else {
        throw new DataException("Expected a String map value but was " + value.getClass());
      }
    }
    return object;
  }
}
