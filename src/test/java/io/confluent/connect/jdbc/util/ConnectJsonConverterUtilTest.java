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

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.DataException;
import org.junit.Test;

import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

public class ConnectJsonConverterUtilTest {

  @Test
  public void shallowMapNullStaysNull() {
    assertNull(ConnectJsonConverterUtil.jsonStringToShallowMap(null));
  }

  @Test
  public void shallowMapJsonNullBecomesEmpty() {
    Map<String, String> result = ConnectJsonConverterUtil.jsonStringToShallowMap("null");
    assertTrue(result.isEmpty());
  }

  @Test
  public void shallowMapPreservesScalarsAndNestedAsJsonStrings() {
    String json = "{\"name\":\"sensor-1\",\"temp\":42,\"meta\":{\"loc\":\"x\"},\"tags\":[1,2]}";
    Map<String, String> result = ConnectJsonConverterUtil.jsonStringToShallowMap(json);
    assertEquals("\"sensor-1\"", result.get("name"));
    assertEquals("42", result.get("temp"));
    assertEquals("{\"loc\":\"x\"}", result.get("meta"));
    assertEquals("[1,2]", result.get("tags"));
  }

  @Test
  public void shallowMapRejectsTopLevelArray() {
    assertThrows(DataException.class,
        () -> ConnectJsonConverterUtil.jsonStringToShallowMap("[1,2,3]"));
  }

  @Test
  public void serializeNullValueReturnsNull() {
    assertNull(ConnectJsonConverterUtil.connectValueToJson(Schema.OPTIONAL_STRING_SCHEMA, null));
  }

  @Test
  public void serializeStruct() {
    Schema schema = SchemaBuilder.struct()
        .field("id", Schema.INT32_SCHEMA)
        .field("name", Schema.STRING_SCHEMA)
        .build();
    Struct struct = new Struct(schema).put("id", 7).put("name", "alice");
    String json = ConnectJsonConverterUtil.connectValueToJson(schema, struct);
    assertEquals("{\"id\":7,\"name\":\"alice\"}", json);
  }

  @Test
  public void serializeMap() {
    Schema schema = SchemaBuilder.map(Schema.STRING_SCHEMA, Schema.INT32_SCHEMA).build();
    Map<String, Integer> input = new LinkedHashMap<>();
    input.put("a", 1);
    input.put("b", 2);
    String json = ConnectJsonConverterUtil.connectValueToJson(schema, input);
    assertEquals("{\"a\":1,\"b\":2}", json);
  }

  @Test
  public void serializeArray() {
    Schema schema = SchemaBuilder.array(Schema.INT32_SCHEMA).build();
    String json = ConnectJsonConverterUtil.connectValueToJson(schema, Arrays.asList(1, 2, 3));
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
    String json = ConnectJsonConverterUtil.connectValueToJson(schema, struct);
    assertEquals("{\"inner\":{\"loc\":\"lab\"},\"tags\":[\"a\",\"b\"],\"attrs\":{\"temp\":42}}",
        json);
  }
}
