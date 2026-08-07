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

import org.apache.kafka.connect.errors.DataException;
import org.junit.Test;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;

public class JsonConverterTest {

  @Test
  public void serializeNullValueReturnsNull() {
    assertNull(JsonConverter.connectMapToJson(null));
  }

  @Test
  public void serializeStringMapAsJsonObject() {
    Map<String, String> input = new LinkedHashMap<>();
    input.put("env", "prod");
    input.put("tier", "gold");

    assertEquals("{\"env\":\"prod\",\"tier\":\"gold\"}", JsonConverter.connectMapToJson(input));
  }

  @Test
  public void serializeEmptyMapAsEmptyJsonObject() {
    assertEquals("{}", JsonConverter.connectMapToJson(new LinkedHashMap<>()));
  }

  @Test
  public void serializeNullMapValueAsJsonNull() {
    Map<String, String> input = new LinkedHashMap<>();
    input.put("present", "v");
    input.put("absent", null);

    assertEquals("{\"present\":\"v\",\"absent\":null}", JsonConverter.connectMapToJson(input));
  }

  @Test
  public void serializeEscapesKeysAndValues() {
    Map<String, String> input = new LinkedHashMap<>();
    input.put("quote\"key", "line\nbreak");

    assertEquals("{\"quote\\\"key\":\"line\\nbreak\"}", JsonConverter.connectMapToJson(input));
  }

  @Test
  public void serializeMapWithNullKeyThrows() {
    Map<String, String> input = new HashMap<>();
    input.put(null, "v");

    assertThrows(DataException.class, () -> JsonConverter.connectMapToJson(input));
  }

  @Test
  public void serializeNonStringMapValueThrows() {
    Map<String, Integer> input = new LinkedHashMap<>();
    input.put("n", 1);

    assertThrows(DataException.class, () -> JsonConverter.connectMapToJson(input));
  }

  @Test
  public void serializeNonStringMapKeyThrows() {
    // A key outside the MAP<STRING,STRING> contract fails rather than being coerced via toString().
    Map<Integer, String> input = new LinkedHashMap<>();
    input.put(7, "v");

    assertThrows(DataException.class, () -> JsonConverter.connectMapToJson(input));
  }

  @Test
  public void serializeNonMapValueThrows() {
    assertThrows(DataException.class, () -> JsonConverter.connectMapToJson("not-a-map"));
  }
}
