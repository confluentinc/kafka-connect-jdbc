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
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import org.apache.kafka.connect.data.Date;
import org.apache.kafka.connect.data.Decimal;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.data.Time;
import org.apache.kafka.connect.data.Timestamp;
import org.apache.kafka.connect.errors.DataException;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.util.Base64;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Bridges JDBC JSON/JSONB columns and Kafka Connect schema-bearing values: parses a JSON document
 * into a {@code Map<String,String>} for the source path, and serializes a Connect
 * Struct/Map/List/primitive back into a JSON string for the sink path.
 */
public final class JsonConverter {

  private static final ObjectMapper MAPPER = new ObjectMapper();

  private JsonConverter() {
  }

  /**
   * Parse a JSON object into a {@code Map<String,String>} keyed by top-level field name. Scalars
   * become their decoded text, nested objects/arrays keep their JSON text, and a JSON null becomes
   * a null map value. Returns null for null input, and an empty map for a JSON null document.
   *
   * @throws DataException if the input cannot be parsed or its top level is not a JSON object
   */
  public static Map<String, String> jsonStringToMap(String json) {
    if (json == null) {
      return null;
    }
    final JsonNode root;
    try {
      root = MAPPER.readTree(json);
    } catch (JsonProcessingException e) {
      throw new DataException("Failed to parse JSON content into Connect Map", e);
    }
    if (root == null || root.isNull()) {
      return new LinkedHashMap<>();
    }
    if (!root.isObject()) {
      throw new DataException("Expected a top-level JSON object but found " + root.getNodeType());
    }
    Map<String, String> out = new LinkedHashMap<>();
    Iterator<Map.Entry<String, JsonNode>> fields = root.fields();
    while (fields.hasNext()) {
      Map.Entry<String, JsonNode> entry = fields.next();
      out.put(entry.getKey(), nodeToJsonString(entry.getValue()));
    }
    return out;
  }

  private static String nodeToJsonString(JsonNode node) {
    if (node == null || node.isNull()) {
      return null;
    }
    if (node.isValueNode()) {
      return node.asText();
    }
    try {
      return MAPPER.writeValueAsString(node);
    } catch (JsonProcessingException e) {
      throw new DataException("Failed to encode JSON node", e);
    }
  }

  /**
   * Serialize a Connect value (Struct, Map, List, primitive, or logical type) into a JSON string
   * for binding into a JSON/JSONB column. When present the schema is used to honor logical types
   * (Decimal, Date, Time, Timestamp, bytes); otherwise the type is inferred from the value.
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
    return schema != null ? schemaTypeToJsonNode(schema, value) : inferToJsonNode(value);
  }

  private static JsonNode logicalToJsonNode(Schema schema, Object value) {
    if (schema == null || schema.name() == null) {
      return null;
    }
    switch (schema.name()) {
      case Decimal.LOGICAL_NAME:
        return MAPPER.valueToTree(((BigDecimal) value).toPlainString());
      case Date.LOGICAL_NAME:
      case Time.LOGICAL_NAME:
      case Timestamp.LOGICAL_NAME:
        return MAPPER.valueToTree(((java.util.Date) value).getTime());
      default:
        return null;
    }
  }

  private static JsonNode schemaTypeToJsonNode(Schema schema, Object value) {
    switch (schema.type()) {
      case STRUCT:
        return structToJsonNode((Struct) value);
      case MAP:
        return mapToJsonNode(schema, (Map<?, ?>) value);
      case ARRAY:
        return listToJsonNode(schema, (List<?>) value);
      case BYTES:
        return MAPPER.valueToTree(bytesToBase64(value));
      default:
        return MAPPER.valueToTree(value);
    }
  }

  private static JsonNode inferToJsonNode(Object value) {
    if (value instanceof Struct) {
      return structToJsonNode((Struct) value);
    }
    if (value instanceof Map) {
      return mapToJsonNode(null, (Map<?, ?>) value);
    }
    if (value instanceof List) {
      return listToJsonNode(null, (List<?>) value);
    }
    if (value instanceof byte[] || value instanceof ByteBuffer) {
      return MAPPER.valueToTree(bytesToBase64(value));
    }
    return MAPPER.valueToTree(value);
  }

  private static JsonNode structToJsonNode(Struct struct) {
    ObjectNode obj = MAPPER.createObjectNode();
    for (Field field : struct.schema().fields()) {
      obj.set(field.name(), toJsonNode(field.schema(), struct.get(field)));
    }
    return obj;
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
    if (value instanceof byte[]) {
      return Base64.getEncoder().encodeToString((byte[]) value);
    }
    ByteBuffer buf = ((ByteBuffer) value).slice();
    byte[] bytes = new byte[buf.remaining()];
    buf.get(bytes);
    return Base64.getEncoder().encodeToString(bytes);
  }
}
