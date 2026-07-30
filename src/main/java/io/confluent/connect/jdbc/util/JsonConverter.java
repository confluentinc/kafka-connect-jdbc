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

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;

import org.apache.kafka.connect.data.Date;
import org.apache.kafka.connect.data.Decimal;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Time;
import org.apache.kafka.connect.data.Timestamp;
import org.apache.kafka.connect.errors.DataException;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.util.Base64;
import java.util.List;
import java.util.Map;

/**
 * Serializes a Connect Map/List/primitive value into a JSON string for JDBC JSON/JSONB
 * (and {@code jsonb[]}) sink columns.
 */
public final class JsonConverter {

  // Plain decimal notation (1500 not 1.5E+3) and exact scale (1.50 stays 1.50).
  private static final ObjectMapper MAPPER = new ObjectMapper()
      .configure(JsonGenerator.Feature.WRITE_BIGDECIMAL_AS_PLAIN, true)
      .setNodeFactory(JsonNodeFactory.withExactBigDecimals(true));

  private JsonConverter() {
  }

  /**
   * Serialize a Connect value (Map/List/primitive/logical type) into a JSON string for a
   * JSON/JSONB column. Uses the schema to honor logical types when present, else infers from value.
   */
  public static String connectValueToJson(Schema schema, Object value) {
    if (value == null) {
      return null;
    }
    try {
      return MAPPER.writeValueAsString(toJsonNode(schema, value));
    } catch (JsonProcessingException e) {
      throw new DataException("Failed to serialize Connect value to JSON", e);
    }
  }

  private static JsonNode toJsonNode(Schema schema, Object value) {
    if (value == null) {
      return MAPPER.nullNode();
    }
    JsonNode logical = logicalToJsonNode(schema, value);
    if (logical != null) {
      return logical;
    }
    // Dispatch on the runtime type; the (nullable) schema is threaded through so a nested MAP or
    // ARRAY value picks up its element schema.
    if (value instanceof Map) {
      return mapToJsonNode(schema, (Map<?, ?>) value);
    }
    if (value instanceof List) {
      return listToJsonNode(schema, (List<?>) value);
    }
    if (value instanceof byte[] || value instanceof ByteBuffer) {
      return MAPPER.valueToTree(bytesToBase64(value));
    }
    return MAPPER.valueToTree(value);
  }

  private static JsonNode logicalToJsonNode(Schema schema, Object value) {
    if (schema == null || schema.name() == null) {
      return null;
    }
    switch (schema.name()) {
      case Decimal.LOGICAL_NAME:
        // JSON number; jsonb stores it losslessly as numeric.
        return MAPPER.valueToTree((BigDecimal) value);
      case Date.LOGICAL_NAME:
      case Time.LOGICAL_NAME:
      case Timestamp.LOGICAL_NAME:
        return MAPPER.valueToTree(((java.util.Date) value).getTime());
      default:
        return null;
    }
  }

  private static JsonNode mapToJsonNode(Schema schema, Map<?, ?> map) {
    Schema valueSchema = schema != null ? schema.valueSchema() : null;
    ObjectNode obj = MAPPER.createObjectNode();
    for (Map.Entry<?, ?> entry : map.entrySet()) {
      if (entry.getKey() == null) {
        throw new DataException("Cannot serialize a Connect MAP with null keys to JSON");
      }
      obj.set(entry.getKey().toString(), toJsonNode(valueSchema, entry.getValue()));
    }
    return obj;
  }

  private static JsonNode listToJsonNode(Schema schema, List<?> list) {
    Schema valueSchema = schema != null ? schema.valueSchema() : null;
    ArrayNode arr = MAPPER.createArrayNode();
    for (Object element : list) {
      arr.add(toJsonNode(valueSchema, element));
    }
    return arr;
  }

  private static String bytesToBase64(Object value) {
    final byte[] bytes;
    if (value instanceof byte[]) {
      bytes = (byte[]) value;
    } else {
      // slice() leaves the caller's buffer position/limit untouched.
      ByteBuffer buffer = ((ByteBuffer) value).slice();
      bytes = new byte[buffer.remaining()];
      buffer.get(bytes);
    }
    return Base64.getEncoder().encodeToString(bytes);
  }
}
