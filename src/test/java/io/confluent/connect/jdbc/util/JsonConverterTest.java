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

import org.apache.kafka.connect.data.Date;
import org.apache.kafka.connect.data.Decimal;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.data.Time;
import org.apache.kafka.connect.data.Timestamp;
import org.apache.kafka.connect.errors.DataException;
import org.junit.Test;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

public class JsonConverterTest {

  @Test
  public void mapNullStaysNull() {
    assertNull(JsonConverter.jsonStringToMap(null));
  }

  @Test
  public void mapJsonNullBecomesEmpty() {
    Map<String, String> result = JsonConverter.jsonStringToMap("null");
    assertTrue(result.isEmpty());
  }

  @Test
  public void mapDecodesScalarsAndKeepsNestedAsJsonStrings() {
    String json = "{\"name\":\"sensor-1\",\"temp\":42,\"on\":true,\"miss\":null,"
        + "\"meta\":{\"loc\":\"x\"},\"tags\":[1,2]}";
    Map<String, String> result = JsonConverter.jsonStringToMap(json);
    assertEquals("sensor-1", result.get("name"));
    assertEquals("42", result.get("temp"));
    assertEquals("true", result.get("on"));
    assertNull(result.get("miss"));
    assertEquals("{\"loc\":\"x\"}", result.get("meta"));
    assertEquals("[1,2]", result.get("tags"));
  }

  @Test
  public void mapRejectsTopLevelArray() {
    assertThrows(DataException.class,
        () -> JsonConverter.jsonStringToMap("[1,2,3]"));
  }

  @Test
  public void mapRejectsTopLevelScalar() {
    // map mode requires a top-level object; a bare number or string cannot become a Map.
    assertThrows(DataException.class, () -> JsonConverter.jsonStringToMap("42"));
    assertThrows(DataException.class, () -> JsonConverter.jsonStringToMap("\"hello\""));
  }

  @Test
  public void mapRejectsMalformedJson() {
    assertThrows(DataException.class, () -> JsonConverter.jsonStringToMap("{not valid"));
  }

  @Test
  public void serializeNullValueReturnsNull() {
    assertNull(JsonConverter.connectValueToJson(Schema.OPTIONAL_STRING_SCHEMA, null));
  }

  @Test
  public void serializeStruct() {
    Schema schema = SchemaBuilder.struct()
        .field("id", Schema.INT32_SCHEMA)
        .field("name", Schema.STRING_SCHEMA)
        .build();
    Struct struct = new Struct(schema).put("id", 7).put("name", "alice");
    String json = JsonConverter.connectValueToJson(schema, struct);
    assertEquals("{\"id\":7,\"name\":\"alice\"}", json);
  }

  @Test
  public void serializeMap() {
    Schema schema = SchemaBuilder.map(Schema.STRING_SCHEMA, Schema.INT32_SCHEMA).build();
    Map<String, Integer> input = new LinkedHashMap<>();
    input.put("a", 1);
    input.put("b", 2);
    String json = JsonConverter.connectValueToJson(schema, input);
    assertEquals("{\"a\":1,\"b\":2}", json);
  }

  @Test
  public void serializeArray() {
    Schema schema = SchemaBuilder.array(Schema.INT32_SCHEMA).build();
    String json = JsonConverter.connectValueToJson(schema, Arrays.asList(1, 2, 3));
    assertEquals("[1,2,3]", json);
  }

  @Test
  public void serializeNestedStructWithMapAndArray() {
    Schema inner = SchemaBuilder.struct()
        .field("loc", Schema.STRING_SCHEMA)
        .build();
    Schema schema = SchemaBuilder.struct()
        .field("inner", inner)
        .field("tags", SchemaBuilder.array(Schema.STRING_SCHEMA).build())
        .field("attrs", SchemaBuilder.map(Schema.STRING_SCHEMA, Schema.INT32_SCHEMA).build())
        .build();
    Map<String, Integer> attrs = new LinkedHashMap<>();
    attrs.put("temp", 42);
    Struct struct = new Struct(schema)
        .put("inner", new Struct(inner).put("loc", "lab"))
        .put("tags", Arrays.asList("a", "b"))
        .put("attrs", attrs);
    String json = JsonConverter.connectValueToJson(schema, struct);
    assertEquals("{\"inner\":{\"loc\":\"lab\"},\"tags\":[\"a\",\"b\"],\"attrs\":{\"temp\":42}}",
        json);
  }

  @Test
  public void serializeDecimalAsJsonNumber() {
    // Decimal logical type -> JSON number (not a quoted string), scale preserved.
    assertEquals("1.50",
        JsonConverter.connectValueToJson(Decimal.schema(2), new BigDecimal("1.50")));
  }

  @Test
  public void serializeDecimalStaysInPlainNotation() {
    // WRITE_BIGDECIMAL_AS_PLAIN: a high-scale small magnitude must not become 1E-7.
    assertEquals("0.0000001",
        JsonConverter.connectValueToJson(Decimal.schema(7), new BigDecimal("0.0000001")));
  }

  @Test
  public void serializeTemporalLogicalTypesAsEpochMillis() {
    assertEquals("0", JsonConverter.connectValueToJson(Date.SCHEMA, new java.util.Date(0L)));
    assertEquals("0", JsonConverter.connectValueToJson(Time.SCHEMA, new java.util.Date(0L)));
    assertEquals("1749560400000",
        JsonConverter.connectValueToJson(Timestamp.SCHEMA, new java.util.Date(1749560400000L)));
  }

  @Test
  public void serializeBytesAsBase64() {
    // JSON has no binary type; bytes are Base64-encoded. {1,2,3} -> "AQID".
    assertEquals("\"AQID\"",
        JsonConverter.connectValueToJson(Schema.BYTES_SCHEMA, new byte[]{1, 2, 3}));
    // ByteBuffer is handled the same way (and its position is left intact via slice()).
    ByteBuffer buffer = ByteBuffer.wrap(new byte[]{1, 2, 3});
    assertEquals("\"AQID\"",
        JsonConverter.connectValueToJson(Schema.BYTES_SCHEMA, buffer));
    assertEquals("buffer position must not be consumed", 0, buffer.position());
  }

  @Test
  public void serializeMapWithNullKeyThrows() {
    Schema schema = SchemaBuilder.map(Schema.OPTIONAL_STRING_SCHEMA, Schema.INT32_SCHEMA).build();
    Map<String, Integer> input = new HashMap<>();
    input.put(null, 1);
    assertThrows(DataException.class, () -> JsonConverter.connectValueToJson(schema, input));
  }

  @Test
  public void serializeInfersStructureWhenSchemaIsNull() {
    // Schemaless path: inferToJsonNode dispatches on the Java runtime type.
    assertEquals("[1,2,3]",
        JsonConverter.connectValueToJson(null, Arrays.asList(1, 2, 3)));

    Map<String, Integer> map = new LinkedHashMap<>();
    map.put("a", 1);
    assertEquals("{\"a\":1}", JsonConverter.connectValueToJson(null, map));

    assertEquals("\"hi\"", JsonConverter.connectValueToJson(null, "hi"));
    assertEquals("\"AQID\"",
        JsonConverter.connectValueToJson(null, new byte[]{1, 2, 3}));
  }
}
