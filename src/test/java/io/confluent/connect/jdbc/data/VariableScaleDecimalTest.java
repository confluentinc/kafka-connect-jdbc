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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class VariableScaleDecimalTest {

  @Test
  public void schemaShouldBeStructWithScaleAndValueFields() {
    Schema schema = VariableScaleDecimal.schema();
    assertEquals(Schema.Type.STRUCT, schema.type());
    assertEquals(VariableScaleDecimal.LOGICAL_NAME, schema.name());
    assertEquals(Integer.valueOf(VariableScaleDecimal.SCHEMA_VERSION), schema.version());
    assertEquals(Schema.Type.INT32, schema.field(VariableScaleDecimal.SCALE_FIELD).schema().type());
    assertEquals(Schema.Type.BYTES, schema.field(VariableScaleDecimal.VALUE_FIELD).schema().type());
    assertFalse(schema.isOptional());
  }

  @Test
  public void optionalSchemaShouldBeOptional() {
    assertTrue(VariableScaleDecimal.optionalSchema().isOptional());
  }

  @Test
  public void shouldRoundTripAcrossPositiveZeroAndNegativeScales() {
    // Positive scale (the common case), including trailing zeros and unconstrained precision.
    assertRoundTrip(new BigDecimal("1.50"));                              // scale 2
    assertRoundTrip(new BigDecimal("3.14159"));                          // scale 5
    assertRoundTrip(new BigDecimal("-3.14159"));                         // negative value, scale 5
    assertRoundTrip(new BigDecimal("0.0001"));                           // scale 4
    assertRoundTrip(new BigDecimal("123456789012345.678901234567890")); // large, scale 15

    // Zero scale.
    assertRoundTrip(new BigDecimal("100"));                              // scale 0
    assertRoundTrip(BigDecimal.ZERO);                                    // 0, scale 0

    // Negative scale, e.g. PostgreSQL numeric(p,-s): the value rounds to tens/hundreds and the
    // unscaled magnitude carries a negative exponent. BigDecimal supports this directly.
    assertRoundTrip(new BigDecimal(BigInteger.valueOf(15), -2));         // 1.5E+3  == 1500
    assertRoundTrip(new BigDecimal(BigInteger.valueOf(-7), -3));         // -7E+3   == -7000
  }

  private static void assertRoundTrip(BigDecimal value) {
    Struct encoded = VariableScaleDecimal.fromLogical(VariableScaleDecimal.schema(), value);
    BigDecimal decoded = VariableScaleDecimal.toLogical(encoded);
    // Equality (not compareTo) so that the scale, e.g. the trailing zeros of "1.50", is preserved.
    assertEquals(value, decoded);
    assertEquals(value.scale(), decoded.scale());
  }
}
