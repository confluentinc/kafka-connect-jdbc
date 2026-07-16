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
import org.junit.Assert; import org.junit.Test;
public class SafeTextTest {
  @Test public void plainLineIsSafe() {
    Assert.assertTrue(SafeText.isSafe("INSERT INTO \"t\" (\"id\") VALUES (<redacted>): ERROR: sql_error"));
  }
  @Test public void nullIsUnsafe() { Assert.assertFalse(SafeText.isSafe(null)); }
  @Test public void newlinesAndCrUnsafe() {
    Assert.assertFalse(SafeText.isSafe("a\nb"));
    Assert.assertFalse(SafeText.isSafe("a\rb"));
  }
  @Test public void lineSeparatorUnsafe() { Assert.assertFalse(SafeText.isSafe("a b")); }
  @Test public void bidiControlUnsafe() { Assert.assertFalse(SafeText.isSafe("a‮b")); }
  @Test public void zeroWidthUnsafe() { Assert.assertFalse(SafeText.isSafe("a​b")); }
  @Test public void unpairedSurrogateUnsafe() { Assert.assertFalse(SafeText.isSafe("a\uD800b")); }
  @Test public void overCapUnsafe() {
    StringBuilder sb = new StringBuilder();
    for (int i = 0; i <= SafeText.MAX_CHARS; i++) sb.append('x');
    Assert.assertFalse(SafeText.isSafe(sb.toString()));
  }
  @Test public void atCapSafe() {
    StringBuilder sb = new StringBuilder();
    for (int i = 0; i < SafeText.MAX_CHARS; i++) sb.append('x');
    Assert.assertTrue(SafeText.isSafe(sb.toString()));
  }
}
