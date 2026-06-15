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

import java.util.Date;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class ZonedTimestampTest {

  @Test
  public void schemaShouldBeNamedStringType() {
    Schema schema = ZonedTimestamp.schema();
    assertEquals(Schema.Type.STRING, schema.type());
    assertEquals(ZonedTimestamp.LOGICAL_NAME, schema.name());
    assertEquals(Integer.valueOf(ZonedTimestamp.SCHEMA_VERSION), schema.version());
    assertFalse(schema.isOptional());
  }

  @Test
  public void optionalSchemaShouldBeOptional() {
    assertTrue(ZonedTimestamp.optionalSchema().isOptional());
  }

  @Test
  public void shouldRenderInstantAsUtcIsoOffsetString() {
    assertEquals("1970-01-01T00:00:00Z", ZonedTimestamp.toIsoString(new Date(0L)));
    // 2025-06-10 13:00:00 UTC
    assertEquals("2025-06-10T13:00:00Z", ZonedTimestamp.toIsoString(new Date(1749560400000L)));
  }

  @Test
  public void shouldReturnNullForNullInstant() {
    assertNull(ZonedTimestamp.toIsoString(null));
  }
}
