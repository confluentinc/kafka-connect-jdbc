/*
 * Copyright 2024 Confluent Inc.
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

import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class TableIdTest {

  @Test
  public void testToStringWithQuotes() {
    TableId tableId = new TableId("mydb", "myschema", "mytable");
    String result = tableId.toString();
    assertEquals("\"mydb\".\"myschema\".\"mytable\"", result);
  }

  @Test
  public void testToUnquotedString() {
    TableId tableId = new TableId("mydb", "myschema", "mytable");
    String result = tableId.toUnquotedString();
    assertEquals("mydb.myschema.mytable", result);
  }

  @Test
  public void testToUnquotedStringWithNullSchema() {
    TableId tableId = new TableId("mydb", null, "mytable");
    String result = tableId.toUnquotedString();
    assertEquals("mydb.mytable", result);
  }

  @Test
  public void testToUnquotedStringWithNullCatalog() {
    TableId tableId = new TableId(null, "myschema", "mytable");
    String result = tableId.toUnquotedString();
    assertEquals("myschema.mytable", result);
  }

  @Test
  public void testToUnquotedStringWithOnlyTable() {
    TableId tableId = new TableId(null, null, "mytable");
    String result = tableId.toUnquotedString();
    assertEquals("mytable", result);
  }
}
