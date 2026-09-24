/*
 * Copyright 2018 Confluent Inc.
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

package io.confluent.connect.jdbc.dialect;

import org.apache.kafka.connect.data.Schema.Type;
import org.junit.Test;

import static org.hamcrest.CoreMatchers.instanceOf;
import static org.junit.Assert.assertThat;

public class RedshiftDatabaseDialectTest extends BaseDialectTest<RedshiftDatabaseDialect> {

  @Override
  protected RedshiftDatabaseDialect createDialect() {
    return new RedshiftDatabaseDialect(sourceConfigWithUrl("jdbc:redshift://something"));
  }

  @Test
  public void shouldInheritFromPostgreSqlDatabaseDialect() {
    assertThat(createDialect(), instanceOf(PostgreSqlDatabaseDialect.class));
  }

  @Test
  public void shouldMapPrimitiveSchemaTypeToSqlTypes() {
    assertPrimitiveMapping(Type.INT8, "SMALLINT");
    assertPrimitiveMapping(Type.INT16, "SMALLINT");
    assertPrimitiveMapping(Type.INT32, "INT");
    assertPrimitiveMapping(Type.INT64, "BIGINT");
    assertPrimitiveMapping(Type.FLOAT32, "REAL");
    assertPrimitiveMapping(Type.FLOAT64, "DOUBLE PRECISION");
    assertPrimitiveMapping(Type.BOOLEAN, "BOOLEAN");
    assertPrimitiveMapping(Type.STRING, "VARCHAR(MAX)");
  }
}