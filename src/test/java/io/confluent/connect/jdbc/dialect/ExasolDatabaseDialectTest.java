/*
 * Copyright 2018 Confluent Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.confluent.connect.jdbc.dialect;

import org.apache.kafka.connect.data.Date;
import org.apache.kafka.connect.data.Decimal;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Schema.Type;
import org.apache.kafka.connect.data.Time;
import org.apache.kafka.connect.data.Timestamp;
import org.apache.kafka.connect.errors.ConnectException;

import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.List;

import io.confluent.connect.jdbc.dialect.BaseDialectTest;
import io.confluent.connect.jdbc.util.TableId;

import static org.junit.Assert.assertEquals;

public class ExasolDatabaseDialectTest extends BaseDialectTest<ExasolDatabaseDialect> {

  @Override
  protected ExasolDatabaseDialect createDialect() {
    return new ExasolDatabaseDialect(sourceConfigWithUrl("jdbc:exa://something"));
  }

  @Rule
  public final ExpectedException exception = ExpectedException.none();

  @Test
  public void shouldMapPrimitiveSchemaTypeToSqlTypes() {
    assertPrimitiveMapping(Type.INT8, "DECIMAL(3,0)");
    assertPrimitiveMapping(Type.INT16, "DECIMAL(5,0)");
    assertPrimitiveMapping(Type.INT32, "DECIMAL(10,0)");
    assertPrimitiveMapping(Type.INT64, "DECIMAL(19,0)");
    assertPrimitiveMapping(Type.FLOAT32, "FLOAT");
    assertPrimitiveMapping(Type.FLOAT64, "DOUBLE");
    assertPrimitiveMapping(Type.BOOLEAN, "BOOLEAN");
    assertPrimitiveMapping(Type.STRING, "CLOB");
    // BLOB is not supported
    exception.expect(ConnectException.class);
    assertPrimitiveMapping(Type.BYTES, "BLOB");
  }

  @Test
  public void shouldMapDecimalSchemaTypeToDecimalSqlType() {
    assertDecimalMapping(0, "DECIMAL(36,0)");
    assertDecimalMapping(3, "DECIMAL(36,3)");
    assertDecimalMapping(4, "DECIMAL(36,4)");
    assertDecimalMapping(5, "DECIMAL(36,5)");
  }

  @Test
  public void shouldMapDataTypes() {
    verifyDataTypeMapping("DECIMAL(3,0)", Schema.INT8_SCHEMA);
    verifyDataTypeMapping("DECIMAL(5,0)", Schema.INT16_SCHEMA);
    verifyDataTypeMapping("DECIMAL(10,0)", Schema.INT32_SCHEMA);
    verifyDataTypeMapping("DECIMAL(19,0)", Schema.INT64_SCHEMA);
    verifyDataTypeMapping("FLOAT", Schema.FLOAT32_SCHEMA);
    verifyDataTypeMapping("DOUBLE", Schema.FLOAT64_SCHEMA);
    verifyDataTypeMapping("BOOLEAN", Schema.BOOLEAN_SCHEMA);
    verifyDataTypeMapping("CLOB", Schema.STRING_SCHEMA);
    verifyDataTypeMapping("DECIMAL(36,0)", Decimal.schema(0));
    verifyDataTypeMapping("DECIMAL(36,4)", Decimal.schema(4));
    verifyDataTypeMapping("DATE", Date.SCHEMA);
    verifyDataTypeMapping("DECIMAL(10,0)", Time.SCHEMA);
    verifyDataTypeMapping("TIMESTAMP", Timestamp.SCHEMA);
    // BLOB is not supported
    exception.expect(ConnectException.class);
    verifyDataTypeMapping("BLOB", Schema.BYTES_SCHEMA);
  }

  @Test
  public void shouldMapDateSchemaTypeToDateSqlType() {
    assertDateMapping("DATE");
  }

  @Test
  public void shouldMapTimeSchemaTypeToTimeSqlType() {
    assertTimeMapping("DECIMAL(10,0)");
  }

  @Test
  public void shouldMapTimestampSchemaTypeToTimestampSqlType() {
    assertTimestampMapping("TIMESTAMP");
  }

  @Test
  public void shouldBuildCreateQueryStatement() {
    String expected = "CREATE TABLE \"myTable\" (\n" +
                      "\"c1\" DECIMAL(10,0) NOT NULL,\n" +
                      "\"c2\" DECIMAL(19,0) NOT NULL,\n" +
                      "\"c3\" CLOB NOT NULL,\n" +
                      "\"c4\" CLOB NULL,\n" +
                      "\"c5\" DATE DEFAULT '2001-03-15',\n" +
                      "\"c6\" DECIMAL(10,0) DEFAULT '00:00:00.000',\n" +
                      "\"c7\" TIMESTAMP DEFAULT '2001-03-15 00:00:00.000',\n" +
                      "\"c8\" DECIMAL(36,4) NULL,\n" +
                      "PRIMARY KEY(\"c1\"))";
    String sql = dialect.buildCreateTableStatement(tableId, sinkRecordFields);
    assertEquals(expected, sql);
  }

  @Test
  public void shouldBuildAlterTableStatement() {
    List<String> statements = dialect.buildAlterTable(tableId, sinkRecordFields);
    String[] sql = {"ALTER TABLE \"myTable\" ADD \"c1\" DECIMAL(10,0) NOT NULL",
                    "ALTER TABLE \"myTable\" ADD \"c2\" DECIMAL(19,0) NOT NULL",
                    "ALTER TABLE \"myTable\" ADD \"c3\" CLOB NOT NULL",
                    "ALTER TABLE \"myTable\" ADD \"c4\" CLOB NULL",
                    "ALTER TABLE \"myTable\" ADD \"c5\" DATE DEFAULT '2001-03-15'",
                    "ALTER TABLE \"myTable\" ADD \"c6\" DECIMAL(10,0) DEFAULT '00:00:00.000'",
                    "ALTER TABLE \"myTable\" ADD \"c7\" TIMESTAMP DEFAULT '2001-03-15 00:00:00.000'",
                    "ALTER TABLE \"myTable\" ADD \"c8\" DECIMAL(36,4) NULL"};
    assertStatements(sql, statements);
  }

  @Test
  public void shouldBuildUpsertStatement() {
    String expected = "MERGE INTO \"myTable\" AS target USING " +
                      "(SELECT ? AS \"id1\", ? AS \"id2\", ? AS \"columnA\", " +
                      "? AS \"columnB\", ? AS \"columnC\", ? AS \"columnD\") AS incoming ON " +
                      "(target.\"id1\"=incoming.\"id1\" AND target.\"id2\"=incoming.\"id2\") " +
                      "WHEN MATCHED THEN UPDATE SET " +
                      "\"columnA\"=incoming.\"columnA\",\"columnB\"=incoming.\"columnB\"," +
                      "\"columnC\"=incoming.\"columnC\",\"columnD\"=incoming.\"columnD\" " +
                      "WHEN NOT MATCHED THEN INSERT " +
                      "(\"columnA\",\"columnB\",\"columnC\",\"columnD\",\"id1\",\"id2\") " +
                      "VALUES " +
                      "(incoming.\"columnA\",incoming.\"columnB\",incoming.\"columnC\"," +
                      "incoming.\"columnD\",incoming.\"id1\",incoming.\"id2\")";
    String sql = dialect.buildUpsertQueryStatement(tableId, pkColumns, columnsAtoD);
    assertEquals(expected, sql);
  }

  @Test
  public void createOneColNoPk() {
    verifyCreateOneColNoPk(
        "CREATE TABLE \"myTable\" (" + System.lineSeparator() + "\"col1\" DECIMAL(10,0) NOT NULL)");
  }

  @Test
  public void createOneColOnePk() {
    verifyCreateOneColOnePk(
        "CREATE TABLE \"myTable\" (" + System.lineSeparator() + "\"pk1\" DECIMAL(10,0) NOT NULL," +
        System.lineSeparator() + "PRIMARY KEY(\"pk1\"))");
  }

  @Test
  public void createThreeColTwoPk() {
    verifyCreateThreeColTwoPk(
        "CREATE TABLE \"myTable\" (" + System.lineSeparator() + "\"pk1\" DECIMAL(10,0) NOT NULL," +
        System.lineSeparator() + "\"pk2\" DECIMAL(10,0) NOT NULL," + System.lineSeparator() +
        "\"col1\" DECIMAL(10,0) NOT NULL," + System.lineSeparator() +
        "PRIMARY KEY(\"pk1\",\"pk2\"))");
  }

  @Test
  public void alterAddOneCol() {
    verifyAlterAddOneCol("ALTER TABLE \"myTable\" ADD \"newcol1\" DECIMAL(10,0) NULL");
  }

  @Test
  public void alterAddTwoCol() {
    verifyAlterAddTwoCols("ALTER TABLE \"myTable\" ADD \"newcol1\" DECIMAL(10,0) NULL",
                          "ALTER TABLE \"myTable\" ADD \"newcol2\" DECIMAL(10,0) DEFAULT 42");
  }

  @Test
  public void upsert1() {
    TableId customer = tableId("Customer");
    String expected = "MERGE INTO \"Customer\" AS target " +
                      "USING (SELECT ? AS \"id\", ? AS \"name\", ? AS \"salary\", ? AS \"address\") AS incoming " +
                      "ON (target.\"id\"=incoming.\"id\") " +
                      "WHEN MATCHED THEN UPDATE SET " +
                      "\"name\"=incoming.\"name\",\"salary\"=incoming.\"salary\",\"address\"=incoming.\"address\" "+
                      "WHEN NOT MATCHED THEN INSERT (\"name\",\"salary\",\"address\",\"id\") " +
                      "VALUES (incoming.\"name\",incoming.\"salary\",incoming.\"address\",incoming.\"id\")";
    String sql = dialect.buildUpsertQueryStatement(customer, columns(customer, "id"), columns(customer, "name", "salary", "address"));
    assertEquals(expected, sql);
  }

  @Test
  public void upsert2() {
    TableId book = new TableId(null, null, "Book");
    String expected = "MERGE INTO \"Book\" AS target " +
                      "USING (SELECT ? AS \"author\", ? AS \"title\", ? AS \"ISBN\", ? AS \"year\", ? AS \"pages\") AS incoming " +
                      "ON (target.\"author\"=incoming.\"author\" AND target.\"title\"=incoming.\"title\") " +
                      "WHEN MATCHED THEN UPDATE SET " +
                      "\"ISBN\"=incoming.\"ISBN\",\"year\"=incoming.\"year\",\"pages\"=incoming.\"pages\" " +
                      "WHEN NOT MATCHED THEN INSERT (\"ISBN\",\"year\",\"pages\",\"author\",\"title\") " +
                      "VALUES (incoming.\"ISBN\",incoming.\"year\",incoming.\"pages\",incoming.\"author\",incoming.\"title\")";
    String sql = dialect.buildUpsertQueryStatement(book, columns(book, "author", "title"),
                                                   columns(book, "ISBN", "year", "pages"));
    assertEquals(expected, sql);
  }

}
