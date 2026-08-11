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

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Converts a Connect {@code MAP<STRING, STRING>} to and from PostgreSQL {@code hstore} text,
 * {@code "key"=>"value"}. pgjdbc's own converter is unusable here: the driver is runtime-scope, so
 * no dialect compiles against it.
 */
public final class HstoreConverter {

  private static final String NULL_VALUE = "NULL";

  /** A quoted token: any run of characters that are neither a quote nor an escape lead-in. */
  private static final String QUOTED = "\"((?:[^\"\\\\]|\\\\.)*)\"";

  /** One "key"=>"value" or "key"=>NULL pair, and the separator that follows it. */
  private static final Pattern HSTORE_PAIR =
      Pattern.compile(QUOTED + "\\s*=>\\s*(?:" + NULL_VALUE + "|" + QUOTED + ")\\s*(?:,\\s*|$)");

  private static final Pattern ESCAPED_CHARACTER = Pattern.compile("\\\\(.)");

  private HstoreConverter() {
  }

  /**
   * Parse {@code hstore} text into a map, preserving order; null and empty text yield null and an
   * empty map. The driver returns text when it cannot resolve the type OID. Accepts quoted pairs —
   * what {@code hstore_out} emits, plus the surrounding whitespace and trailing comma
   * {@code hstore_in} also allows; anything else throws rather than being guessed at.
   */
  public static Map<String, String> hstoreToConnectMap(String text) {
    if (text == null) {
      return null;
    }
    String remaining = text.trim();
    Map<String, String> out = new LinkedHashMap<>();
    Matcher pair = HSTORE_PAIR.matcher(remaining);
    int at = 0;
    while (at < remaining.length()) {
      if (!pair.find(at) || pair.start() != at) {
        throw new DataException(
            "Malformed hstore text at position " + at + " of " + remaining.length());
      }
      out.put(unescape(pair.group(1)), pair.group(2) == null ? null : unescape(pair.group(2)));
      at = pair.end();
    }
    return out;
  }

  private static String unescape(String token) {
    return ESCAPED_CHARACTER.matcher(token).replaceAll("$1");
  }

  /**
   * Serialize a map of strings into hstore text; null yields null. Keys and values are always
   * quoted, so none reads back as the unquoted {@code NULL} that a null value is written as.
   * Throws unless the value is a map of strings with no null key.
   */
  public static String connectMapToHstore(Object value) {
    if (value == null) {
      return null;
    }
    if (!(value instanceof Map)) {
      throw new DataException("Expected a Map to serialize to hstore but was " + value.getClass());
    }
    StringBuilder out = new StringBuilder();
    for (Map.Entry<?, ?> entry : ((Map<?, ?>) value).entrySet()) {
      if (entry.getKey() == null) {
        throw new DataException("An hstore key may not be null");
      }
      if (out.length() > 0) {
        out.append(',');
      }
      appendQuoted(out, asString(entry.getKey(), "key"));
      out.append("=>");
      if (entry.getValue() == null) {
        out.append(NULL_VALUE);
      } else {
        appendQuoted(out, asString(entry.getValue(), "value"));
      }
    }
    return out.toString();
  }

  /** Rejects a non-String rather than coercing it, as JsonConverter does for jsonb. */
  private static String asString(Object entry, String part) {
    if (!(entry instanceof String)) {
      throw new DataException(
          "An hstore " + part + " must be a string but was " + entry.getClass());
    }
    return (String) entry;
  }

  private static void appendQuoted(StringBuilder out, String text) {
    out.append('"');
    for (int i = 0; i < text.length(); i++) {
      char c = text.charAt(i);
      if (c == '"' || c == '\\') {
        out.append('\\');
      }
      out.append(c);
    }
    out.append('"');
  }
}
