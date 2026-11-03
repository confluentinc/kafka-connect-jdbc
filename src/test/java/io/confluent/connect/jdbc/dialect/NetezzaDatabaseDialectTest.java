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

import org.junit.Test;

import java.util.Arrays;

import static org.junit.Assert.assertEquals;

public class NetezzaDatabaseDialectTest extends BaseDialectTest<NetezzaDatabaseDialect> {

  @Override
  protected NetezzaDatabaseDialect createDialect() {
    return new NetezzaDatabaseDialect(sourceConfigWithUrl("jdbc:netezza:thin://something"));
  }

  @Test
  public void shouldBuildUpsertStatement() {
    String expected = "merge into \"myTable\" AS target using (select ? \"id1\", ? \"columnA\", " +
                      "? \"columnB\", ? \"columnC\", ? \"columnD\") AS incoming on" +
                      "(target.\"id1\"=incoming.\"id1\") when matched then update set target.\"columnA\"=incoming" +
                      ".\"columnA\",target.\"columnB\"=incoming.\"columnB\",target" +
                      ".\"columnC\"=incoming.\"columnC\",target.\"columnD\"=incoming" +
                      ".\"columnD\" when not matched then insert(\"columnA\"," +
                      "\"columnB\",\"columnC\",\"columnD\"," +
                      "\"id1\") values(incoming.\"columnA\"," +
                      "incoming.\"columnB\",incoming.\"columnC\",incoming.\"columnD\",incoming" +
                      ".\"id1\")";
    String sql = dialect.buildUpsertQueryStatement(tableId, Arrays.asList(columnPK1), columnsAtoD);
    assertEquals(expected, sql);
  }

  @Test
  public void shouldBuildUpsertStatementWith2Pk() {
    String expected = "merge into \"myTable\" AS target using (select ? \"id1\", ? \"id2\", ? \"columnA\", " +
            "? \"columnB\", ? \"columnC\", ? \"columnD\") AS incoming on" +
            "(target.\"id1\"=incoming.\"id1\" and target.\"id2\"=incoming.\"id2\") when matched then update set target.\"columnA\"=incoming" +
            ".\"columnA\",target.\"columnB\"=incoming.\"columnB\",target" +
            ".\"columnC\"=incoming.\"columnC\",target.\"columnD\"=incoming" +
            ".\"columnD\" when not matched then insert(\"columnA\"," +
            "\"columnB\",\"columnC\",\"columnD\"," +
            "\"id1\",\"id2\") values(incoming.\"columnA\"," +
            "incoming.\"columnB\",incoming.\"columnC\",incoming.\"columnD\",incoming" +
            ".\"id1\",incoming.\"id2\")";
    String sql = dialect.buildUpsertQueryStatement(tableId, pkColumns, columnsAtoD);
    assertEquals(expected, sql);
  }

}