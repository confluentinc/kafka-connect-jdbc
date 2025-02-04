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


import io.confluent.connect.jdbc.util.TableDefinition;
import io.confluent.connect.jdbc.util.TableId;
import org.junit.Test;
import static org.junit.Assert.assertEquals;


public class RedshiftDatabaseDialectTest extends BaseDialectTest<RedshiftDatabaseDialect> {

  @Override
  protected RedshiftDatabaseDialect createDialect() {
    return new RedshiftDatabaseDialect(sourceConfigWithUrl("jdbc:redshift://localhost:9000/dw_test"));
  }

  @Test
  public void shouldReturnOne() {
    final int expected = 1;
    assertEquals(expected, 1);
  }

  @Test
  public void shouldBuildUpsertStatement() {
    String expected = "MERGE INTO \"myTable\" USING (SELECT ? \"id1\", ? \"id2\", ? \"columnA\", " +
            "? \"columnB\", ? \"columnC\", ? \"columnD\") AS source ON" +
            "(\"myTable\".\"id1\"=source.\"id1\" AND \"myTable\".\"id2\"=source" +
            ".\"id2\") WHEN MATCHED THEN UPDATE SET \"myTable\".\"columnA\"=source" +
            ".\"columnA\",\"myTable\".\"columnB\"=source.\"columnB\",\"myTable\"" +
            ".\"columnC\"=source.\"columnC\",\"myTable\".\"columnD\"=source" +
            ".\"columnD\" WHEN NOT MATCHED THEN INSERT(\"myTable\".\"columnA\"," +
            "\"myTable\".\"columnB\",\"myTable\".\"columnC\",\"myTable\".\"columnD\"," +
            "\"myTable\".\"id1\",\"myTable\".\"id2\") VALUES(source.\"columnA\"," +
            "source.\"columnB\",source.\"columnC\",source.\"columnD\",source" +
            ".\"id1\",source.\"id2\")";
    String sql = dialect.buildUpsertQueryStatement(tableId, pkColumns, columnsAtoD,null);
    assertEquals(expected, sql);
  }
}

