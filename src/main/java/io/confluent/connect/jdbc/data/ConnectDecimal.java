/*
 * Copyright 2020 Confluent Inc.
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

import org.apache.kafka.connect.data.Decimal;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;


/**
 * Wrapper around Kafka {@link Decimal} to include the additional precision parameter required by
 * the Connect Avro Decimal schema.
 */
public class ConnectDecimal extends Decimal {
  /**
   * Used by AvroData serializer for creating an Avro schema with the correct precision.
   * @see "io.confluent.connect.avro.AvroData.CONNECT_AVRO_DECIMAL_PRECISION_PROP"
   */
  public static final String CONNECT_AVRO_PRECISION_FIELD = "connect.decimal.precision";

  /**
   * Returns a {@link SchemaBuilder} for a Decimal number with a given precision and scale factor.
   */
  public static SchemaBuilder builder(int precision, int scale) {
    return Decimal.builder(scale)
        .parameter(CONNECT_AVRO_PRECISION_FIELD, String.valueOf(precision));
  }

  public static Schema schema(int precision, int scale) {
    return builder(precision, scale).build();
  }
}
