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

package io.confluent.connect.jdbc.data;

import org.apache.kafka.connect.data.Schema;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class JsonTest {

  @Test
  public void schemaShouldBeNamedStringType() {
    Schema schema = Json.schema();
    assertEquals(Schema.Type.STRING, schema.type());
    assertEquals(Json.LOGICAL_NAME, schema.name());
    assertEquals(Integer.valueOf(Json.SCHEMA_VERSION), schema.version());
    assertFalse(schema.isOptional());
  }

  @Test
  public void optionalSchemaShouldBeOptional() {
    Schema schema = Json.optionalSchema();
    assertEquals(Schema.Type.STRING, schema.type());
    assertEquals(Json.LOGICAL_NAME, schema.name());
    assertTrue(schema.isOptional());
  }
}
