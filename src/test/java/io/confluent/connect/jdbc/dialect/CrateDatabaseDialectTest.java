/**
 * Copyright 2017 Confluent Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 **/

package io.confluent.connect.jdbc.dialect;

import io.confluent.connect.jdbc.util.TableId;
import org.apache.kafka.connect.data.*;
import org.apache.kafka.connect.data.Schema.Type;
import org.junit.Test;

import java.util.List;

import static org.junit.Assert.assertEquals;

public class CrateDatabaseDialectTest extends BaseDialectTest<CrateDatabaseDialect> {

  @Override
  protected CrateDatabaseDialect createDialect() {
    return new CrateDatabaseDialect(sourceConfigWithUrl("jdbc:crate://something/"));
  }

  @Test
  public void shouldMapPrimitiveSchemaTypeToSqlTypes() {
    assertPrimitiveMapping(Type.INT8, "INTEGER");
    assertPrimitiveMapping(Type.INT16, "INTEGER");
    assertPrimitiveMapping(Type.INT32, "INTEGER");
    assertPrimitiveMapping(Type.INT64, "INTEGER");
    assertPrimitiveMapping(Type.FLOAT32, "FLOAT");
    assertPrimitiveMapping(Type.FLOAT64, "FLOAT");
    assertPrimitiveMapping(Type.BOOLEAN, "BOOLEAN");
    assertPrimitiveMapping(Type.BYTES, "BYTE");
    assertPrimitiveMapping(Type.STRING, "STRING");
  }

  @Test
  public void shouldMapDecimalSchemaTypeToDecimalSqlType() {
    assertDecimalMapping(0, "DOUBLE");
    assertDecimalMapping(3, "DOUBLE");
    assertDecimalMapping(4, "DOUBLE");
    assertDecimalMapping(5, "DOUBLE");
  }

  @Test
  public void shouldMapDataTypes() {
    verifyDataTypeMapping("INTEGER", Schema.INT8_SCHEMA);
    verifyDataTypeMapping("INTEGER", Schema.INT16_SCHEMA);
    verifyDataTypeMapping("INTEGER", Schema.INT32_SCHEMA);
    verifyDataTypeMapping("INTEGER", Schema.INT64_SCHEMA);
    verifyDataTypeMapping("FLOAT", Schema.FLOAT32_SCHEMA);
    verifyDataTypeMapping("FLOAT", Schema.FLOAT64_SCHEMA);
    verifyDataTypeMapping("BOOLEAN", Schema.BOOLEAN_SCHEMA);
    verifyDataTypeMapping("STRING", Schema.STRING_SCHEMA);
    verifyDataTypeMapping("BYTE", Schema.BYTES_SCHEMA);
    verifyDataTypeMapping("DOUBLE", Decimal.schema(0));
    verifyDataTypeMapping("TIMESTAMP", Date.SCHEMA);
    verifyDataTypeMapping("TIMESTAMP", Time.SCHEMA);
    verifyDataTypeMapping("TIMESTAMP", Timestamp.SCHEMA);
  }

  @Test
  public void shouldMapDateSchemaTypeToDateSqlType() {
    assertDateMapping("TIMESTAMP");
  }

  @Test
  public void shouldMapTimeSchemaTypeToTimeSqlType() {
    assertTimeMapping("TIMESTAMP");
  }

  @Test
  public void shouldMapTimestampSchemaTypeToTimestampSqlType() {
    assertTimestampMapping("TIMESTAMP");
  }

  @Test
  public void shouldBuildCreateQueryStatement() {
    String expected =
        "CREATE TABLE \"myTable\" (\n" + "\"c1\" INTEGER NOT NULL,\n" + "\"c2\" INTEGER NOT NULL,\n" +
        "\"c3\" STRING NOT NULL,\n" + "\"c4\" STRING,\n" + "\"c5\" TIMESTAMP DEFAULT '2001-03-15',\n" +
        "\"c6\" TIMESTAMP DEFAULT '00:00:00.000',\n" +
        "\"c7\" TIMESTAMP DEFAULT '2001-03-15 00:00:00.000',\n" + "\"c8\" DOUBLE,\n" +
        "PRIMARY KEY(\"c1\"))";
    String sql = dialect.buildCreateTableStatement(tableId, sinkRecordFields);
    System.out.print(sql);
    assertEquals(expected, sql);
  }

  @Test
  public void shouldBuildAlterTableStatement() {
    List<String> statements = dialect.buildAlterTable(tableId, sinkRecordFields);
    String[] sql = {"ALTER TABLE \"myTable\" \n" + "ADD \"c1\" INTEGER NOT NULL,\n" +
                    "ADD \"c2\" INTEGER NOT NULL,\n" + "ADD \"c3\" STRING NOT NULL,\n" +
                    "ADD \"c4\" STRING,\n" + "ADD \"c5\" TIMESTAMP DEFAULT '2001-03-15',\n" +
                    "ADD \"c6\" TIMESTAMP DEFAULT '00:00:00.000',\n" +
                    "ADD \"c7\" TIMESTAMP DEFAULT '2001-03-15 00:00:00.000',\n" +
                    "ADD \"c8\" DOUBLE"};
    assertStatements(sql, statements);
  }

  @Test
  public void shouldBuildUpsertStatement() {
    String expected = "INSERT INTO \"myTable\" (\"id1\",\"id2\",\"columnA\",\"columnB\"," +
                      "\"columnC\",\"columnD\") VALUES (?,?,?,?,?,?) ON CONFLICT (\"id1\"," +
                      "\"id2\") DO UPDATE SET \"columnA\"=EXCLUDED" +
                      ".\"columnA\",\"columnB\"=EXCLUDED.\"columnB\",\"columnC\"=EXCLUDED" +
                      ".\"columnC\",\"columnD\"=EXCLUDED.\"columnD\"";
    String sql = dialect.buildUpsertQueryStatement(tableId, pkColumns, columnsAtoD);
    assertEquals(expected, sql);
  }

  @Test
  public void createOneColNoPk() {
    verifyCreateOneColNoPk(
        "CREATE TABLE \"myTable\" (" + System.lineSeparator() + "\"col1\" INTEGER NOT NULL)");
  }

  @Test
  public void createOneColOnePk() {
    verifyCreateOneColOnePk(
        "CREATE TABLE \"myTable\" (" + System.lineSeparator() + "\"pk1\" INTEGER NOT NULL," +
        System.lineSeparator() + "PRIMARY KEY(\"pk1\"))");
  }

  @Test
  public void createThreeColTwoPk() {
    verifyCreateThreeColTwoPk(
        "CREATE TABLE \"myTable\" (" + System.lineSeparator() + "\"pk1\" INTEGER NOT NULL," +
        System.lineSeparator() + "\"pk2\" INTEGER NOT NULL," + System.lineSeparator() +
        "\"col1\" INTEGER NOT NULL," + System.lineSeparator() + "PRIMARY KEY(\"pk1\",\"pk2\"))");
  }

  @Test
  public void alterAddOneCol() {
    verifyAlterAddOneCol("ALTER TABLE \"myTable\" ADD \"newcol1\" INTEGER");
  }

  @Test
  public void alterAddTwoCol() {
    verifyAlterAddTwoCols(
        "ALTER TABLE \"myTable\" " + System.lineSeparator() + "ADD \"newcol1\" INTEGER," +
        System.lineSeparator() + "ADD \"newcol2\" INTEGER DEFAULT 42");
  }

  @Test
  public void upsert() {
    TableId customer = tableId("Customer");
    assertEquals("INSERT INTO \"Customer\" (\"id\",\"name\",\"salary\",\"address\") " +
                 "VALUES (?,?,?,?) ON CONFLICT (\"id\") DO UPDATE SET \"name\"=EXCLUDED.\"name\"," +
                 "\"salary\"=EXCLUDED.\"salary\",\"address\"=EXCLUDED.\"address\"", dialect
                     .buildUpsertQueryStatement(customer, columns(customer, "id"),
                                                columns(customer, "name", "salary", "address")));
  }

}