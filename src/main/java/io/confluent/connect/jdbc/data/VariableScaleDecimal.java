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
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;

import java.math.BigDecimal;
import java.math.BigInteger;

/**
 * A Kafka Connect logical type for an arbitrary-precision decimal whose scale is not fixed at the
 * column level — for example a PostgreSQL unconstrained {@code numeric}/{@code numeric[]} where
 * each value may carry a different scale.
 *
 * <p>Connect's built-in {@link org.apache.kafka.connect.data.Decimal} requires a single fixed
 * scale shared by every value, which cannot represent such columns. This type instead encodes the
 * scale alongside the value as a STRUCT of {@code {scale INT32, value BYTES}}, where {@code value}
 * is the two's-complement big-endian unscaled magnitude. Together they reconstruct the original
 * {@link BigDecimal} losslessly with its own scale.
 */
public final class VariableScaleDecimal {

  public static final String LOGICAL_NAME = "io.confluent.connect.jdbc.data.VariableScaleDecimal";
  public static final String SCALE_FIELD = "scale";
  public static final String VALUE_FIELD = "value";
  public static final int SCHEMA_VERSION = 1;

  private VariableScaleDecimal() {
  }

  /**
   * Returns a {@link SchemaBuilder} for the variable-scale decimal STRUCT. Callers may further
   * configure it (e.g. {@code optional()}) before building.
   *
   * @return the schema builder; never null
   */
  public static SchemaBuilder builder() {
    return SchemaBuilder.struct()
        .name(LOGICAL_NAME)
        .version(SCHEMA_VERSION)
        .doc("Variable scaled decimal")
        .field(SCALE_FIELD, Schema.INT32_SCHEMA)
        .field(VALUE_FIELD, Schema.BYTES_SCHEMA);
  }

  /**
   * Returns a variable-scale decimal {@link Schema} with all other default settings.
   *
   * @return the schema; never null
   * @see #builder()
   */
  public static Schema schema() {
    return builder().build();
  }

  /**
   * Returns an optional variable-scale decimal {@link Schema} with all other default settings, e.g.
   * for use as an array element type.
   *
   * @return the optional schema; never null
   * @see #builder()
   */
  public static Schema optionalSchema() {
    return builder().optional().build();
  }

  /**
   * Encode a {@link BigDecimal} into a {@link Struct} carrying its scale and unscaled bytes.
   *
   * @param schema the struct schema to build against; may not be null
   * @param value  the decimal to encode; may not be null
   * @return the populated struct; never null
   */
  public static Struct fromLogical(Schema schema, BigDecimal value) {
    Struct struct = new Struct(schema);
    struct.put(SCALE_FIELD, value.scale());
    struct.put(VALUE_FIELD, value.unscaledValue().toByteArray());
    return struct;
  }

  /**
   * Decode a {@link Struct} produced by {@link #fromLogical(Schema, BigDecimal)} back into a
   * {@link BigDecimal}.
   *
   * @param struct the struct to decode; may not be null
   * @return the decoded decimal; never null
   */
  public static BigDecimal toLogical(Struct struct) {
    return new BigDecimal(
        new BigInteger(struct.getBytes(VALUE_FIELD)), struct.getInt32(SCALE_FIELD));
  }
}
