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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

public class HstoreConverterTest {

  /**
   * The hstore text form the bind produces. Everything is quoted, so a delimiter inside a key or
   * value is inert and the string {@code NULL} stays distinct from a NULL value.
   */
  @Test
  public void shouldSerializeMapsToHstoreText() {
    Map<String, String> map = new LinkedHashMap<>();
    map.put("env", "prod");
    map.put("absent", null);
    map.put("literal", "NULL");
    map.put("a=>b", "c,d");
    map.put("say \"hi\"", "back\\slash");

    assertEquals("\"env\"=>\"prod\",\"absent\"=>NULL,\"literal\"=>\"NULL\","
            + "\"a=>b\"=>\"c,d\",\"say \\\"hi\\\"\"=>\"back\\\\slash\"",
        HstoreConverter.connectMapToHstore(map));

    assertEquals("", HstoreConverter.connectMapToHstore(Collections.emptyMap()));
    assertNull(HstoreConverter.connectMapToHstore(null));
  }

  /**
   * The text form the driver hands back for an hstore it could not resolve. Quoting makes the
   * delimiters inert, and a bare NULL is a null value while a quoted one is the string.
   */
  @Test
  public void shouldParseHstoreText() {
    Map<String, String> expected = new LinkedHashMap<>();
    expected.put("env", "prod");
    expected.put("absent", null);
    expected.put("literal", "NULL");
    expected.put("a=>b", "c,d");
    expected.put("say \"hi\"", "back\\slash");

    // PostgreSQL renders pairs separated by ", " and escapes quotes and backslashes.
    assertEquals(expected, HstoreConverter.hstoreToConnectMap(
        "\"env\"=>\"prod\", \"absent\"=>NULL, \"literal\"=>\"NULL\", "
            + "\"a=>b\"=>\"c,d\", \"say \\\"hi\\\"\"=>\"back\\\\slash\""));

    assertEquals(Collections.emptyMap(), HstoreConverter.hstoreToConnectMap(""));
    assertEquals(Collections.emptyMap(), HstoreConverter.hstoreToConnectMap("   "));
    assertNull(HstoreConverter.hstoreToConnectMap(null));
  }

  /**
   * Insertion order is preserved, which {@link Map#equals} cannot show: the parser returns a
   * {@link LinkedHashMap} so a round trip through Kafka keeps the column's own pair order.
   */
  @Test
  public void shouldPreserveKeyOrderWhenParsing() {
    Map<String, String> parsed = HstoreConverter.hstoreToConnectMap(
        "\"z\"=>\"1\", \"a\"=>\"2\", \"m\"=>\"3\"");

    assertEquals(Arrays.asList("z", "a", "m"), new ArrayList<>(parsed.keySet()));
  }

  /**
   * {@code hstore_in} tolerates padding and a trailing comma even though {@code hstore_out} never
   * emits them, so text assembled by hand rather than by the driver still parses.
   */
  @Test
  public void shouldAcceptWhitespaceAndATrailingComma() {
    Map<String, String> expected = new LinkedHashMap<>();
    expected.put("env", "prod");
    expected.put("tier", "web");

    assertEquals(expected, HstoreConverter.hstoreToConnectMap(
        "  \"env\" => \"prod\" ,  \"tier\" => \"web\" ,  "));
  }

  /** Whatever the serializer writes, the parser must read back identically. */
  @Test
  public void shouldRoundTripHstoreTextThroughBothDirections() {
    Map<String, String> original = new LinkedHashMap<>();
    original.put("env", "prod");
    original.put("absent", null);
    original.put("literal", "NULL");
    original.put("a=>b", "c,d");
    original.put("say \"hi\"", "back\\slash");
    original.put("key 2", " ##123 78");
    original.put("empty", "");

    assertEquals(original, HstoreConverter.hstoreToConnectMap(
        HstoreConverter.connectMapToHstore(original)));
    assertEquals(Collections.emptyMap(), HstoreConverter.hstoreToConnectMap(
        HstoreConverter.connectMapToHstore(Collections.emptyMap())));
  }

  @Test
  public void shouldRejectMalformedHstoreText() {
    for (String malformed : new String[]{
        "not hstore at all",          // no => at all
        "\"k\"",                      // key with no value
        "\"k\"=>",                    // separator with no value
        "\"k\"=>\"unterminated",      // unclosed quote
        "\"a\"=>\"1\" \"b\"=>\"2\"",     // missing comma between pairs
        "a=>1",                       // unquoted: hstore_out always quotes, so this is not ours
        "\"a\"=>1",                   // unquoted value, likewise
        "\"a\"=>null"                 // lowercase null is the string, never the NULL literal
    }) {
      assertThrows("should reject: " + malformed, DataException.class,
          () -> HstoreConverter.hstoreToConnectMap(malformed));
    }
  }

  /**
   * The refusal locates the offending text. A malformed value is a data problem an operator has to
   * find in a column that may hold many pairs, so the offset earns its place in the message.
   */
  @Test
  public void shouldNameThePositionOfMalformedText() {
    DataException e = assertThrows(DataException.class,
        () -> HstoreConverter.hstoreToConnectMap("\"a\"=>\"1\", oops"));

    assertTrue("should locate the failure, but was: " + e.getMessage(),
        e.getMessage().contains("position"));
  }

  @Test
  public void shouldRejectValuesThatAreNotStringMaps() {
    assertThrows(DataException.class, () -> HstoreConverter.connectMapToHstore("not a map"));
    assertThrows(DataException.class,
        () -> HstoreConverter.connectMapToHstore(Collections.singletonMap(null, "v")));
    // A non-String key or value fails rather than being coerced through toString, so a schema and
    // value that disagree surface here instead of silently reaching the database.
    assertThrows(DataException.class,
        () -> HstoreConverter.connectMapToHstore(Collections.singletonMap("k", 1)));
    assertThrows(DataException.class,
        () -> HstoreConverter.connectMapToHstore(Collections.singletonMap(1, "v")));
  }
}
