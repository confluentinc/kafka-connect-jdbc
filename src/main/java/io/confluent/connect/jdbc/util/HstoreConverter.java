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
 * Converts between a Connect {@code MAP<STRING, STRING>} and the PostgreSQL {@code hstore} text
 * form, {@code "key"=>"value"} — writing it for a bind through a cast, and reading it back for the
 * columns the driver hands over as text rather than as a decoded map.
 *
 * <p>pgjdbc's own {@code org.postgresql.util.HStoreConverter} is not usable here: the driver is a
 * runtime-scope dependency so that it ships with the connector without any dialect compiling
 * against it, which keeps the dialects on JDBC APIs alone.
 */
public final class HstoreConverter {

  private static final String NULL_VALUE = "NULL";

  /** A quoted token: any run of characters that are neither a quote nor an escape lead-in. */
  private static final String QUOTED = "\"((?:[^\"\\\\]|\\\\.)*)\"";

  /**
   * One {@code "key"=>"value"} or {@code "key"=>NULL} pair, and the separator that follows it.
   */
  private static final Pattern HSTORE_PAIR =
      Pattern.compile(QUOTED + "\\s*=>\\s*(?:" + NULL_VALUE + "|" + QUOTED + ")\\s*(?:,\\s*|$)");

  private static final Pattern ESCAPED_CHARACTER = Pattern.compile("\\\\(.)");

  private HstoreConverter() {
  }

  /**
   * Parse PostgreSQL {@code hstore} text into a map. The driver hands back this form instead of a
   * decoded map whenever it cannot resolve the extension's type OID, which is the case for any
   * hstore that is not on the connection's {@code search_path}.
   *
   * <p>Only what {@code hstore_out} emits is accepted: {@code "key"=>"value"} pairs separated by
   * {@code ", "}, with both sides always quoted apart from a bare {@code NULL} value. Anything else
   * did not come from PostgreSQL and is rejected rather than guessed at.
   *
   * @param text the hstore text; null yields null
   * @return the parsed map, preserving pair order; empty text yields an empty map
   * @throws DataException if the text is not well-formed hstore
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
   * Serialize a map of strings into hstore text. Every key and value is quoted, so no value can be
   * read back as the unquoted {@code NULL} literal; a null value is written as that literal.
   *
   * @param value the map to serialize; null yields null
   * @return the hstore text, or null when the value is null
   * @throws DataException if the value is not a map of strings, or has a null key
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

  /**
   * Rejects a non-String rather than coercing it through {@code toString}, so a schema and value
   * that disagree fail here instead of reaching the database. {@link JsonConverter} makes the same
   * choice for jsonb, through a typed writer.
   */
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
