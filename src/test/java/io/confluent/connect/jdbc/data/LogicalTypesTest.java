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
import org.apache.kafka.connect.data.Struct;
import org.junit.Test;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Date;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

/**
 * Unit tests for the custom Connect logical types in this package: {@link Json},
 * {@link VariableScaleDecimal}, and {@link ZonedTimestamp}.
 */
public class LogicalTypesTest {

  // ----- Json -----

  @Test
  public void jsonSchemaIsNamedStringType() {
    Schema schema = Json.schema();
    assertEquals(Schema.Type.STRING, schema.type());
    assertEquals(Json.LOGICAL_NAME, schema.name());
    assertEquals(Integer.valueOf(Json.SCHEMA_VERSION), schema.version());
    assertFalse(schema.isOptional());
  }

  @Test
  public void jsonOptionalSchemaIsOptional() {
    assertTrue(Json.optionalSchema().isOptional());
  }

  // ----- VariableScaleDecimal -----

  @Test
  public void variableScaleDecimalSchemaHasScaleAndValueFields() {
    Schema schema = VariableScaleDecimal.schema();
    assertEquals(Schema.Type.STRUCT, schema.type());
    assertEquals(VariableScaleDecimal.LOGICAL_NAME, schema.name());
    assertEquals(Integer.valueOf(VariableScaleDecimal.SCHEMA_VERSION), schema.version());
    assertEquals(Schema.Type.INT32, schema.field(VariableScaleDecimal.SCALE_FIELD).schema().type());
    assertEquals(Schema.Type.BYTES, schema.field(VariableScaleDecimal.VALUE_FIELD).schema().type());
    assertFalse(schema.isOptional());
  }

  @Test
  public void variableScaleDecimalOptionalSchemaIsOptional() {
    assertTrue(VariableScaleDecimal.optionalSchema().isOptional());
  }

  @Test
  public void variableScaleDecimalRoundTripsAcrossPositiveZeroAndNegativeScales() {
    // Positive scale (the common case), including trailing zeros and unconstrained precision.
    assertRoundTrip(new BigDecimal("1.50"));                              // scale 2
    assertRoundTrip(new BigDecimal("3.14159"));                          // scale 5
    assertRoundTrip(new BigDecimal("-3.14159"));                         // negative value, scale 5
    assertRoundTrip(new BigDecimal("0.0001"));                           // scale 4
    assertRoundTrip(new BigDecimal("123456789012345.678901234567890")); // large, scale 15

    // Zero scale.
    assertRoundTrip(new BigDecimal("100"));                              // scale 0
    assertRoundTrip(BigDecimal.ZERO);                                    // 0, scale 0

    // Negative scale, e.g. PostgreSQL numeric(p,-s); BigDecimal supports this directly.
    assertRoundTrip(new BigDecimal(BigInteger.valueOf(15), -2));         // 1.5E+3  == 1500
    assertRoundTrip(new BigDecimal(BigInteger.valueOf(-7), -3));         // -7E+3   == -7000
  }

  private static void assertRoundTrip(BigDecimal value) {
    Struct encoded = VariableScaleDecimal.fromLogical(VariableScaleDecimal.schema(), value);
    BigDecimal decoded = VariableScaleDecimal.toLogical(encoded);
    // Equality (not compareTo) so the scale, e.g. the trailing zeros of "1.50", is preserved.
    assertEquals(value, decoded);
    assertEquals(value.scale(), decoded.scale());
  }

  // ----- ZonedTimestamp -----

  @Test
  public void zonedTimestampSchemaIsNamedStringType() {
    Schema schema = ZonedTimestamp.schema();
    assertEquals(Schema.Type.STRING, schema.type());
    assertEquals(ZonedTimestamp.LOGICAL_NAME, schema.name());
    assertEquals(Integer.valueOf(ZonedTimestamp.SCHEMA_VERSION), schema.version());
    assertFalse(schema.isOptional());
  }

  @Test
  public void zonedTimestampOptionalSchemaIsOptional() {
    assertTrue(ZonedTimestamp.optionalSchema().isOptional());
  }

  @Test
  public void zonedTimestampRendersInstantAsUtcIsoOffsetString() {
    assertEquals("1970-01-01T00:00:00Z", ZonedTimestamp.toIsoString(new Date(0L)));
    // 2025-06-10 13:00:00 UTC
    assertEquals("2025-06-10T13:00:00Z", ZonedTimestamp.toIsoString(new Date(1749560400000L)));
  }

  @Test
  public void zonedTimestampReturnsNullForNullInstant() {
    assertNull(ZonedTimestamp.toIsoString(null));
  }
}
