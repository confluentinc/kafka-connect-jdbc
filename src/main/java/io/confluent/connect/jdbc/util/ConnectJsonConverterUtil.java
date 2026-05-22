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
import com.fasterxml.jackson.databind.node.ObjectNode;

import org.apache.kafka.connect.data.Decimal;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.DataException;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.util.Base64;
import java.util.Date;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Helpers that bridge JDBC JSON / JSONB columns and Kafka Connect schema-bearing values.
 *
 * <p>Used by the JDBC source path (parsing a JSON String into a Connect-friendly
 * {@code Map<String,String>}) and by the JDBC sink path (serializing a Connect
 * Struct/Map/List back into a JSON String suitable for binding into a JSON or JSONB column).
 *
 * <p>The map representation is intentionally shallow: each top-level entry maps the JSON
 * field name to the JSON-encoded representation of its value. This keeps the schema fixed
 * (Map&lt;STRING,STRING&gt;) while still letting downstream consumers (ksqlDB, SMTs) walk into
 * individual top-level fields without re-parsing the whole document.
 */
public final class ConnectJsonConverterUtil {

  private static final ObjectMapper MAPPER = new ObjectMapper();

  private ConnectJsonConverterUtil() {
  }

  /**
   * Parse a JSON string into a {@code Map<String,String>} where each top-level value is
   * projected to a String according to its JSON type:
   *
   * <ul>
   *   <li>JSON string → decoded text (no surrounding quotes), so a JSONB→MAP→JSONB
   *       round-trip preserves the original literal.
   *   <li>JSON number / boolean → its textual form ({@code "1"}, {@code "true"}).
   *   <li>JSON object / array → its JSON-encoded text, so structure is preserved as a
   *       sub-document inside the MAP value.
   *   <li>JSON null → Java {@code null} map value.
   * </ul>
   *
   * <p>Examples:
   * <ul>
   *   <li>{@code {"a":1,"b":"x"}} → {@code {"a":"1", "b":"x"}}
   *   <li>{@code {"sensor":{"temp":50}}} → {@code {"sensor":"{\"temp\":50}"}}
   * </ul>
   *
   * <p>Returns null if the input is null. Returns an empty map if the input represents
   * a JSON null. Throws if the JSON top-level is not an object.
   */
  public static Map<String, String> jsonStringToShallowMap(String json) {
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
      throw new DataException(
          "Expected JSON object at top-level for shallow Map conversion but found "
              + root.getNodeType());
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
      // Scalars (string/number/boolean) → decoded text so a JSONB round-trip stays clean.
      return node.asText();
    }
    try {
      return MAPPER.writeValueAsString(node);
    } catch (JsonProcessingException e) {
      throw new DataException("Failed to encode JSON node", e);
    }
  }

  /**
   * Serialize a Kafka Connect value (Struct, Map, List, primitive, logical type) into a
   * JSON string suitable for binding into a database JSON / JSONB column.
   *
   * <p>The schema is used when present to honor logical types (Decimal, Date, Time,
   * Timestamp, byte arrays). When the schema is null, types are inferred from the value.
   */
  public static String connectValueToJson(Schema schema, Object value) {
    if (value == null) {
      return null;
    }
    JsonNode node = toJsonNode(schema, value);
    try {
      return MAPPER.writeValueAsString(node);
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
    if (schema != null) {
      return schemaTypeToJsonNode(schema, value);
    }
    return inferToJsonNode(value);
  }

  private static JsonNode logicalToJsonNode(Schema schema, Object value) {
    if (schema == null || schema.name() == null) {
      return null;
    }
    switch (schema.name()) {
      case Decimal.LOGICAL_NAME:
        return MAPPER.valueToTree(((BigDecimal) value).toPlainString());
      case org.apache.kafka.connect.data.Date.LOGICAL_NAME:
      case org.apache.kafka.connect.data.Time.LOGICAL_NAME:
      case org.apache.kafka.connect.data.Timestamp.LOGICAL_NAME:
        return MAPPER.valueToTree(((Date) value).getTime());
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
    Schema schema = struct.schema();
    for (Field field : schema.fields()) {
      Object fieldValue = struct.get(field);
      obj.set(field.name(), toJsonNode(field.schema(), fieldValue));
    }
    return obj;
  }

  private static JsonNode mapToJsonNode(Schema schema, Map<?, ?> map) {
    Schema valueSchema = (schema != null) ? schema.valueSchema() : null;
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
    Schema valueSchema = (schema != null) ? schema.valueSchema() : null;
    com.fasterxml.jackson.databind.node.ArrayNode arr = MAPPER.createArrayNode();
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
