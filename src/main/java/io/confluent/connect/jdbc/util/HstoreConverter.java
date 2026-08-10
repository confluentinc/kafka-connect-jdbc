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

import java.util.Map;

/**
 * Serializes a Connect {@code MAP<STRING, STRING>} into the PostgreSQL {@code hstore} text form,
 * {@code "key"=>"value"}, for binding into an {@code hstore} column through a cast.
 *
 * <p>The driver's own converter is not usable here: pgjdbc is a runtime-scope dependency, so no
 * dialect compiles against it.
 */
public final class HstoreConverter {

  private static final String NULL_VALUE = "NULL";

  private HstoreConverter() {
  }

  /**
   * Serialize a map of strings into hstore text. Every key and value is quoted, so no value can be
   * read back as the unquoted {@code NULL} literal; a null value is written as that literal.
   *
   * @param value the map to serialize; null yields null
   * @return the hstore text, or null when the value is null
   * @throws DataException if the value is not a map, or has a null key
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
      appendQuoted(out, entry.getKey().toString());
      out.append("=>");
      if (entry.getValue() == null) {
        out.append(NULL_VALUE);
      } else {
        appendQuoted(out, entry.getValue().toString());
      }
    }
    return out.toString();
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
