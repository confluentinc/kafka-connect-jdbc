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

import org.apache.kafka.connect.data.Date;
import org.apache.kafka.connect.data.Decimal;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Schema.Type;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Time;
import org.apache.kafka.connect.data.Timestamp;
import org.junit.Test;

import java.util.Collections;
import java.util.List;

import io.confluent.connect.jdbc.sink.metadata.SinkRecordField;
import io.confluent.connect.jdbc.util.ExpressionBuilder;
import io.confluent.connect.jdbc.util.QuoteMethod;

import static org.junit.Assert.assertEquals;

public class VerticaDatabaseDialectTest extends BaseDialectTest<VerticaDatabaseDialect> {

  @Override
  protected VerticaDatabaseDialect createDialect() {
    return new VerticaDatabaseDialect(sourceConfigWithUrl("jdbc:vertica://something"));
  }


  @Test
  public void shouldMapPrimitiveSchemaTypeToSqlTypes() {
    assertPrimitiveMapping(Type.INT8, "INT");
    assertPrimitiveMapping(Type.INT16, "INT");
    assertPrimitiveMapping(Type.INT32, "INT");
    assertPrimitiveMapping(Type.INT64, "INT");
    assertPrimitiveMapping(Type.FLOAT32, "FLOAT");
    assertPrimitiveMapping(Type.FLOAT64, "FLOAT");
    assertPrimitiveMapping(Type.BOOLEAN, "BOOLEAN");
    assertPrimitiveMapping(Type.BYTES, "VARBINARY(1024)");
    assertPrimitiveMapping(Type.STRING, "VARCHAR(1024)");
  }

  @Test
  public void shouldParseDecimalScaleAsInt() {
    assertDecimalScaleParsedAsInt("DECIMAL(18,2)");
  }

  @Test
  public void shouldRejectNonNumericDecimalScale() {
    assertNonNumericDecimalScaleRejected();
  }

  @Test
  public void shouldMapDecimalSchemaTypeToDecimalSqlType() {
    assertDecimalMapping(0, "DECIMAL(18,0)");
    assertDecimalMapping(3, "DECIMAL(18,3)");
    assertDecimalMapping(4, "DECIMAL(18,4)");
    assertDecimalMapping(5, "DECIMAL(18,5)");
  }

  @Test
  public void shouldMapDataTypes() {
    verifyDataTypeMapping("INT", Schema.INT8_SCHEMA);
    verifyDataTypeMapping("INT", Schema.INT16_SCHEMA);
    verifyDataTypeMapping("INT", Schema.INT32_SCHEMA);
    verifyDataTypeMapping("INT", Schema.INT64_SCHEMA);
    verifyDataTypeMapping("FLOAT", Schema.FLOAT32_SCHEMA);
    verifyDataTypeMapping("FLOAT", Schema.FLOAT64_SCHEMA);
    verifyDataTypeMapping("BOOLEAN", Schema.BOOLEAN_SCHEMA);
    verifyDataTypeMapping("VARCHAR(1024)", Schema.STRING_SCHEMA);
    verifyDataTypeMapping("VARBINARY(1024)", Schema.BYTES_SCHEMA);
    verifyDataTypeMapping("DECIMAL(18,0)", Decimal.schema(0));
    verifyDataTypeMapping("DECIMAL(18,4)", Decimal.schema(4));
    verifyDataTypeMapping("DATE", Date.SCHEMA);
    verifyDataTypeMapping("TIME", Time.SCHEMA);
    verifyDataTypeMapping("TIMESTAMP", Timestamp.SCHEMA);
  }

  @Test
  public void shouldMapDateSchemaTypeToDateSqlType() {
    assertDateMapping("DATE");
  }

  @Test
  public void shouldMapTimeSchemaTypeToTimeSqlType() {
    assertTimeMapping("TIME");
  }

  @Test
  public void shouldMapTimestampSchemaTypeToTimestampSqlType() {
    assertTimestampMapping("TIMESTAMP");
  }

  @Test
  public void shouldBuildCreateQueryStatement() {
    assertEquals(
        "CREATE TABLE \"myTable\" (\n"
        + "\"c1\" INT NOT NULL,\n"
        + "\"c2\" INT NOT NULL,\n"
        + "\"c3\" VARCHAR(1024) NOT NULL,\n"
        + "\"c4\" VARCHAR(1024) NULL,\n"
        + "\"c5\" DATE DEFAULT '2001-03-15',\n"
        + "\"c6\" TIME DEFAULT '00:00:00.000',\n"
        + "\"c7\" TIMESTAMP DEFAULT '2001-03-15 00:00:00.000',\n"
        + "\"c8\" DECIMAL(18,4) NULL,\n"
        + "\"c9\" BOOLEAN DEFAULT 1,\n"
        + "PRIMARY KEY(\"c1\"))",
        dialect.buildCreateTableStatement(tableId, sinkRecordFields)
    );

    quoteIdentfiiers = QuoteMethod.NEVER;
    dialect = createDialect();
    assertEquals(
        "CREATE TABLE myTable (\n"
        + "c1 INT NOT NULL,\n"
        + "c2 INT NOT NULL,\n"
        + "c3 VARCHAR(1024) NOT NULL,\n"
        + "c4 VARCHAR(1024) NULL,\n"
        + "c5 DATE DEFAULT '2001-03-15',\n"
        + "c6 TIME DEFAULT '00:00:00.000',\n"
        + "c7 TIMESTAMP DEFAULT '2001-03-15 00:00:00.000',\n"
        + "c8 DECIMAL(18,4) NULL,\n"
        + "c9 BOOLEAN DEFAULT 1,\n"
        + "PRIMARY KEY(c1))",
        dialect.buildCreateTableStatement(tableId, sinkRecordFields)
    );
  }

  @Test
  public void shouldBuildAlterTableStatement() {
    assertStatements(
        new String[]{
            "ALTER TABLE \"myTable\" ADD \"c1\" INT NOT NULL",
            "ALTER TABLE \"myTable\" ADD \"c2\" INT NOT NULL",
            "ALTER TABLE \"myTable\" ADD \"c3\" VARCHAR(1024) NOT NULL",
            "ALTER TABLE \"myTable\" ADD \"c4\" VARCHAR(1024) NULL",
            "ALTER TABLE \"myTable\" ADD \"c5\" DATE DEFAULT '2001-03-15'",
            "ALTER TABLE \"myTable\" ADD \"c6\" TIME DEFAULT '00:00:00.000'",
            "ALTER TABLE \"myTable\" ADD \"c7\" TIMESTAMP DEFAULT '2001-03-15 00:00:00.000'",
            "ALTER TABLE \"myTable\" ADD \"c8\" DECIMAL(18,4) NULL",
            "ALTER TABLE \"myTable\" ADD \"c9\" BOOLEAN DEFAULT 1"
        },
        dialect.buildAlterTable(tableId, sinkRecordFields)
    );

    quoteIdentfiiers = QuoteMethod.NEVER;
    dialect = createDialect();
    assertStatements(
        new String[]{
            "ALTER TABLE myTable ADD c1 INT NOT NULL",
            "ALTER TABLE myTable ADD c2 INT NOT NULL",
            "ALTER TABLE myTable ADD c3 VARCHAR(1024) NOT NULL",
            "ALTER TABLE myTable ADD c4 VARCHAR(1024) NULL",
            "ALTER TABLE myTable ADD c5 DATE DEFAULT '2001-03-15'",
            "ALTER TABLE myTable ADD c6 TIME DEFAULT '00:00:00.000'",
            "ALTER TABLE myTable ADD c7 TIMESTAMP DEFAULT '2001-03-15 00:00:00.000'",
            "ALTER TABLE myTable ADD c8 DECIMAL(18,4) NULL",
            "ALTER TABLE myTable ADD c9 BOOLEAN DEFAULT 1"
        },
        dialect.buildAlterTable(tableId, sinkRecordFields)
    );
  }

  @Test(expected = UnsupportedOperationException.class)
  public void shouldBuildUpsertStatement() {
    dialect.buildUpsertQueryStatement(tableId, pkColumns, columnsAtoD);
  }


  @Test
  public void createOneColNoPk() {
    verifyCreateOneColNoPk(
        "CREATE TABLE \"myTable\" (" + System.lineSeparator() + "\"col1\" INT NOT NULL)");
  }

  @Test
  public void createOneColOnePk() {
    verifyCreateOneColOnePk(
        "CREATE TABLE \"myTable\" (" + System.lineSeparator() + "\"pk1\" INT NOT NULL," +
        System.lineSeparator() + "PRIMARY KEY(\"pk1\"))");
  }

  @Test
  public void createThreeColTwoPk() {
    verifyCreateThreeColTwoPk(
        "CREATE TABLE \"myTable\" (" + System.lineSeparator() + "\"pk1\" INT NOT NULL," +
        System.lineSeparator() + "\"pk2\" INT NOT NULL," + System.lineSeparator() +
        "\"col1\" INT NOT NULL," + System.lineSeparator() + "PRIMARY KEY(\"pk1\",\"pk2\"))");

    quoteIdentfiiers = QuoteMethod.NEVER;
    dialect = createDialect();
    verifyCreateThreeColTwoPk(
        "CREATE TABLE myTable (" + System.lineSeparator() + "pk1 INT NOT NULL," +
        System.lineSeparator() + "pk2 INT NOT NULL," + System.lineSeparator() +
        "col1 INT NOT NULL," + System.lineSeparator() + "PRIMARY KEY(pk1,pk2))");
  }

  @Test
  public void alterAddOneCol() {
    verifyAlterAddOneCol("ALTER TABLE \"myTable\" ADD \"newcol1\" INT NULL");
  }

  @Test
  public void alterAddTwoCol() {
    verifyAlterAddTwoCols("ALTER TABLE \"myTable\" ADD \"newcol1\" INT NULL",
                          "ALTER TABLE \"myTable\" ADD \"newcol2\" INT DEFAULT 42");
  }

  @Test
  public void shouldSanitizeUrlWithoutCredentialsInProperties() {
    assertSanitizedUrl(
        "jdbc:vertica://something?key1=value1&key2=value2&key3=value3&&other=value",
        "jdbc:vertica://something?key1=value1&key2=value2&key3=value3&&other=value"

    );
  }

  @Test
  public void shouldSanitizeUrlWithCredentialsInUrlProperties() {
    assertSanitizedUrl(
        "jdbc:vertica://something?password=secret&key1=value1&key2=value2&key3=value3&"
        + "user=smith&password=secret&other=value",
        "jdbc:vertica://something?password=****&key1=value1&key2=value2&key3=value3&"
        + "user=smith&password=****&other=value"
    );
  }

  private static final String BACKSLASH_PAYLOAD = "a\\'); DROP TABLE t; --";

  @Test
  public void stringDefaultWithBackslashUsesEscapeStringSyntax() {
    // With StandardConformingStrings off (its legacy setting) Vertica treats backslash as an
    // escape inside an ordinary '...' literal, so "a\'" would close the literal early. An E'...'
    // literal always treats backslash as an escape, so doubling both backslash and quote inside
    // E'...' yields the same value whichever way StandardConformingStrings is set.
    ExpressionBuilder builder = dialect.expressionBuilder();
    dialect.formatColumnValue(builder, null, null, Type.STRING, BACKSLASH_PAYLOAD);
    String literal = builder.toString();
    assertEquals("E'a\\\\''); DROP TABLE t; --'", literal);
    assertEquals(literal.length(), endOfEscapeStringLiteral(literal, 1));
  }

  @Test
  public void ordinaryStringDefaultsKeepStandardLiteralSyntax() {
    ExpressionBuilder builder = dialect.expressionBuilder();
    dialect.formatColumnValue(builder, null, null, Type.STRING, "n/a");
    assertEquals("'n/a'", builder.toString());
    builder = dialect.expressionBuilder();
    dialect.formatColumnValue(builder, null, null, Type.STRING, "O'Neill");
    assertEquals("'O''Neill'", builder.toString());
  }

  @Test
  public void createTableBackslashStringDefaultCannotCloseLiteral() {
    String ddl = dialect.buildCreateTableStatement(tableId, Collections.singletonList(
        new SinkRecordField(
            SchemaBuilder.string().optional().defaultValue(BACKSLASH_PAYLOAD).build(),
            "ref", false)));
    assertEquals("CREATE TABLE \"myTable\" (" + System.lineSeparator()
        + "\"ref\" VARCHAR(1024) DEFAULT E'a\\\\''); DROP TABLE t; --')", ddl);
    // The literal must run all the way to the closing quote before the final ")".
    int open = ddl.indexOf(" DEFAULT E'") + " DEFAULT E".length();
    assertEquals(ddl.length() - 1, endOfEscapeStringLiteral(ddl, open));
  }

  @Test
  public void alterTableBackslashStringDefaultCannotCloseLiteral() {
    List<String> ddl = dialect.buildAlterTable(tableId, Collections.singletonList(
        new SinkRecordField(
            SchemaBuilder.string().optional().defaultValue(BACKSLASH_PAYLOAD).build(),
            "ref", false)));
    assertEquals(Collections.singletonList(
        "ALTER TABLE \"myTable\" ADD \"ref\" VARCHAR(1024) DEFAULT E'a\\\\''); DROP TABLE t; --'"),
        ddl);
    int open = ddl.get(0).indexOf(" DEFAULT E'") + " DEFAULT E".length();
    assertEquals(ddl.get(0).length(), endOfEscapeStringLiteral(ddl.get(0), open));
  }

  /**
   * Lex a single-quoted literal starting at {@code open} with backslash escapes honored (as in an
   * E'...' literal, or an ordinary literal with StandardConformingStrings off) and return the
   * index just past its closing quote.
   */
  private static int endOfEscapeStringLiteral(String sql, int open) {
    assertEquals('\'', sql.charAt(open));
    int i = open + 1;
    while (i < sql.length()) {
      char c = sql.charAt(i);
      if (c == '\\') {
        i += 2;
      } else if (c == '\'') {
        if (i + 1 < sql.length() && sql.charAt(i + 1) == '\'') {
          i += 2;
        } else {
          return i + 1;
        }
      } else {
        i++;
      }
    }
    throw new AssertionError("Unterminated string literal in: " + sql);
  }
}