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

public final class SafeText {

  public static final int MAX_CHARS = 4000;

  private SafeText() {
  }

  public static boolean isSafe(String s) {
    if (s == null) {
      return false;
    }
    if (s.length() > MAX_CHARS) {
      return false;
    }

    for (int index = 0; index < s.length(); ) {
      char current = s.charAt(index);
      if (!isValidSurrogate(s, index, current)) {
        return false;
      }

      int codePoint = Character.codePointAt(s, index);
      if (isForbidden(codePoint)) {
        return false;
      }
      index += Character.charCount(codePoint);
    }
    return true;
  }

  private static boolean isValidSurrogate(String s, int index, char current) {
    if (Character.isHighSurrogate(current)) {
      return index + 1 < s.length() && Character.isLowSurrogate(s.charAt(index + 1));
    }
    return !Character.isLowSurrogate(current);
  }

  private static boolean isForbidden(int codePoint) {
    if (codePoint < 0x20) {
      return true;
    }
    if (codePoint >= 0x7F && codePoint <= 0x9F) {
      return true;
    }
    if (codePoint == 0x2028 || codePoint == 0x2029) {
      return true;
    }
    return Character.getType(codePoint) == Character.FORMAT;
  }
}
