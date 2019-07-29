/*
 * Copyright 2018 Confluent Inc.
 * Copyright 2019 Nike, Inc.
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

import io.confluent.connect.jdbc.util.QuoteMethod;
import io.confluent.connect.jdbc.util.TableId;
import org.apache.kafka.connect.data.Date;
import org.apache.kafka.connect.data.Decimal;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Schema.Type;
import org.apache.kafka.connect.data.Time;
import org.apache.kafka.connect.data.Timestamp;
import org.junit.Test;

import java.util.Arrays;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class SnowflakeDatabaseDialectTest extends BaseDialectTest<SnowflakeDatabaseDialect> {
    @Override
    protected SnowflakeDatabaseDialect createDialect() {
        return new SnowflakeDatabaseDialect(sourceConfigWithUrl("jdbc:snowflake://something"));
    }

    @Test
    public void shouldUseTempTableMergeTechnique() {
        assertTrue("Snowflake performs very poorly when using individual jdbc calls to upsert; " +
                "so it should instead use the temp table merge technique", dialect.shouldLoadMergeOnUpsert());
    }

    @Test
    public void shouldMapPrimitiveSchemaTypeToSqlTypes() {
        assertPrimitiveMapping(Type.INT8, "TINYINT");
        assertPrimitiveMapping(Type.INT16, "SMALLINT");
        assertPrimitiveMapping(Type.INT32, "INTEGER");
        assertPrimitiveMapping(Type.INT64, "BIGINT");
        assertPrimitiveMapping(Type.FLOAT32, "REAL");
        assertPrimitiveMapping(Type.FLOAT64, "DOUBLE PRECISION");
        assertPrimitiveMapping(Type.BOOLEAN, "BOOLEAN");
        assertPrimitiveMapping(Type.BYTES, "BINARY");
        assertPrimitiveMapping(Type.STRING, "TEXT");
    }

    @Test
    public void shouldMapDecimalSchemaTypeToDecimalSqlType() {
        assertDecimalMapping(0, "DECIMAL");
        assertDecimalMapping(3, "DECIMAL");
        assertDecimalMapping(4, "DECIMAL");
        assertDecimalMapping(5, "DECIMAL");
    }

    @Test
    public void shouldMapDataTypes() {
        verifyDataTypeMapping("TINYINT", Schema.INT8_SCHEMA);
        verifyDataTypeMapping("SMALLINT", Schema.INT16_SCHEMA);
        verifyDataTypeMapping("INTEGER", Schema.INT32_SCHEMA);
        verifyDataTypeMapping("BIGINT", Schema.INT64_SCHEMA);
        verifyDataTypeMapping("REAL", Schema.FLOAT32_SCHEMA);
        verifyDataTypeMapping("DOUBLE PRECISION", Schema.FLOAT64_SCHEMA);
        verifyDataTypeMapping("BOOLEAN", Schema.BOOLEAN_SCHEMA);
        verifyDataTypeMapping("TEXT", Schema.STRING_SCHEMA);
        verifyDataTypeMapping("BINARY", Schema.BYTES_SCHEMA);
        verifyDataTypeMapping("DECIMAL", Decimal.schema(0));
        verifyDataTypeMapping("DATE", Date.SCHEMA);
        verifyDataTypeMapping("TIME", Time.SCHEMA);
        verifyDataTypeMapping("TIMESTAMP_LTZ", Timestamp.SCHEMA);
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
        assertTimestampMapping("TIMESTAMP_LTZ");
    }

    @Test
    public void shouldBuildCreateQueryStatement() {
        assertEquals(
                "CREATE TABLE \"myTable\" (\n"
                        + "\"c1\" INTEGER NOT NULL,\n"
                        + "\"c2\" BIGINT NOT NULL,\n"
                        + "\"c3\" TEXT NOT NULL,\n"
                        + "\"c4\" TEXT NULL,\n"
                        + "\"c5\" DATE DEFAULT '2001-03-15',\n"
                        + "\"c6\" TIME DEFAULT '00:00:00.000',\n"
                        + "\"c7\" TIMESTAMP_LTZ DEFAULT '2001-03-15 00:00:00.000',\n"
                        + "\"c8\" DECIMAL NULL,\n"
                        + "PRIMARY KEY(\"c1\"))",
                dialect.buildCreateTableStatement(tableId, sinkRecordFields)
        );

        quoteIdentfiiers = QuoteMethod.NEVER;
        dialect = createDialect();

        assertEquals(
                "CREATE TABLE myTable (\n"
                        + "c1 INTEGER NOT NULL,\n"
                        + "c2 BIGINT NOT NULL,\n"
                        + "c3 TEXT NOT NULL,\n"
                        + "c4 TEXT NULL,\n"
                        + "c5 DATE DEFAULT '2001-03-15',\n"
                        + "c6 TIME DEFAULT '00:00:00.000',\n"
                        + "c7 TIMESTAMP_LTZ DEFAULT '2001-03-15 00:00:00.000',\n"
                        + "c8 DECIMAL NULL,\n"
                        + "PRIMARY KEY(c1))",
                dialect.buildCreateTableStatement(tableId, sinkRecordFields)
        );
    }

    @Test
    public void shouldBuildAlterTableStatement() {
        assertEquals(
                Arrays.asList(
                        "ALTER TABLE \"myTable\" ADD " + System.lineSeparator()
                                + "\"c1\" INTEGER NOT NULL," + System.lineSeparator()
                                + "\"c2\" BIGINT NOT NULL," + System.lineSeparator()
                                + "\"c3\" TEXT NOT NULL," + System.lineSeparator()
                                + "\"c4\" TEXT NULL," + System.lineSeparator()
                                + "\"c5\" DATE DEFAULT '2001-03-15'," + System.lineSeparator()
                                + "\"c6\" TIME DEFAULT '00:00:00.000'," + System.lineSeparator()
                                + "\"c7\" TIMESTAMP_LTZ DEFAULT '2001-03-15 00:00:00.000'," + System.lineSeparator()
                                + "\"c8\" DECIMAL NULL"
                ),
                dialect.buildAlterTable(tableId, sinkRecordFields)
        );

        quoteIdentfiiers = QuoteMethod.NEVER;
        dialect = createDialect();

        assertEquals(
                Arrays.asList(
                        "ALTER TABLE myTable ADD " + System.lineSeparator()
                        + "c1 INTEGER NOT NULL," + System.lineSeparator()
                        + "c2 BIGINT NOT NULL," + System.lineSeparator()
                        + "c3 TEXT NOT NULL," + System.lineSeparator()
                        + "c4 TEXT NULL," + System.lineSeparator()
                        + "c5 DATE DEFAULT '2001-03-15'," + System.lineSeparator()
                        + "c6 TIME DEFAULT '00:00:00.000'," + System.lineSeparator()
                        + "c7 TIMESTAMP_LTZ DEFAULT '2001-03-15 00:00:00.000'," + System.lineSeparator()
                        + "c8 DECIMAL NULL"
                ),
                dialect.buildAlterTable(tableId, sinkRecordFields)
        );
    }

    @Test
    public void shouldBuildAlterStatementWithAddOneCol() {
        verifyAlterAddOneCol("ALTER TABLE \"myTable\" ADD \"newcol1\" INTEGER NULL");
    }

    @Test
    public void shouldBuildAlterStatementWithAddTwoCol() {
        verifyAlterAddTwoCols(
                "ALTER TABLE \"myTable\" ADD " + System.lineSeparator()
                        + "\"newcol1\" INTEGER NULL," + System.lineSeparator()
                        + "\"newcol2\" INTEGER DEFAULT 42");
    }

    @Test
    public void shouldBuildCreateStatementWithOneColNoPk() {
        verifyCreateOneColNoPk(
                "CREATE TABLE \"myTable\" (" + System.lineSeparator() + "\"col1\" INTEGER NOT NULL)");
    }

    @Test
    public void shouldBuildCreateStatementWithOneColOnePk() {
        verifyCreateOneColOnePk(
                "CREATE TABLE \"myTable\" (" + System.lineSeparator() + "\"pk1\" INTEGER NOT NULL," +
                        System.lineSeparator() + "PRIMARY KEY(\"pk1\"))");
    }

    @Test
    public void shouldBuildCreateStatementWithThreeColTwoPk() {
        verifyCreateThreeColTwoPk(
                "CREATE TABLE \"myTable\" (" + System.lineSeparator() + "\"pk1\" INTEGER NOT NULL," +
                        System.lineSeparator() + "\"pk2\" INTEGER NOT NULL," + System.lineSeparator() +
                        "\"col1\" INTEGER NOT NULL," + System.lineSeparator() + "PRIMARY KEY(\"pk1\",\"pk2\"))");

        quoteIdentfiiers = QuoteMethod.NEVER;
        dialect = createDialect();

        verifyCreateThreeColTwoPk(
                "CREATE TABLE myTable (" + System.lineSeparator() + "pk1 INTEGER NOT NULL," +
                        System.lineSeparator() + "pk2 INTEGER NOT NULL," + System.lineSeparator() +
                        "col1 INTEGER NOT NULL," + System.lineSeparator() + "PRIMARY KEY(pk1,pk2))");
    }

    @Test
    public void shouldBuildCreateTempTableStatementWithOneColNoPk() {
        verifyCreateTempTableOneColNoPk(
                "CREATE TEMPORARY TABLE \"myTable\" (" + System.lineSeparator() + "\"col1\" INTEGER NOT NULL)");
    }

    @Test
    public void shouldBuildCreateTempTableStatementWithOneColOnePk() {
        verifyCreateTempTableOneColOnePk(
                "CREATE TEMPORARY TABLE \"myTable\" (" + System.lineSeparator() + "\"pk1\" INTEGER NOT NULL)");
    }

    @Test
    public void shouldBuildCreateTempTableStatementWithThreeColTwoPk() {
        verifyCreateTempTableThreeColTwoPk(
                "CREATE TEMPORARY TABLE \"myTable\" (" + System.lineSeparator() + "\"pk1\" INTEGER NOT NULL," +
                        System.lineSeparator() + "\"pk2\" INTEGER NOT NULL," + System.lineSeparator() +
                        "\"col1\" INTEGER NOT NULL)");

        quoteIdentfiiers = QuoteMethod.NEVER;
        dialect = createDialect();

        verifyCreateTempTableThreeColTwoPk(
                "CREATE TEMPORARY TABLE myTable (" + System.lineSeparator() + "pk1 INTEGER NOT NULL," +
                        System.lineSeparator() + "pk2 INTEGER NOT NULL," + System.lineSeparator() +
                        "col1 INTEGER NOT NULL)");
    }

    @Test
    public void shouldBuildDropTableStatement() {
        String expected = "DROP TABLE \"myTable\"";
        String sql = dialect.buildDropTableStatement(tableId, new DropOptions().setIfExists(false));
        assertEquals(expected, sql);
    }

    @Test
    public void shouldBuildDropTableStatementWithIfExistsClause() {
        String expected = "DROP TABLE IF EXISTS \"myTable\"";
        String sql = dialect.buildDropTableStatement(tableId, new DropOptions().setIfExists(true));
        assertEquals(expected, sql);
    }

    @Test
    public void shouldBuildDropTableStatementWithIfExistsClauseAndSchemaNameInTableId() {
        tableId = new TableId("dbName","dbo", "myTable");
        String expected = "DROP TABLE IF EXISTS \"dbName\".\"dbo\".\"myTable\"";
        String sql = dialect.buildDropTableStatement(tableId, new DropOptions().setIfExists(true));
        assertEquals(expected, sql);

        quoteIdentfiiers = QuoteMethod.NEVER;
        dialect = createDialect();
        expected = "DROP TABLE IF EXISTS dbName.dbo.myTable";
        sql = dialect.buildDropTableStatement(tableId, new DropOptions().setIfExists(true));
        assertEquals(expected, sql);
    }

    @Test
    public void shouldBuildInsertStatement() {
        String expected = "INSERT INTO \"myTable\"(\"pk1\",\"columnA\",\"columnB\") VALUES(?,?,?)";
        String sql = dialect.buildInsertStatement(tableId, columns(tableId),
                columns(tableId, "pk1", "columnA", "columnB"));
        assertEquals(expected, sql);

        quoteIdentfiiers = QuoteMethod.NEVER;
        dialect = createDialect();

        expected = "INSERT INTO myTable(pk1,columnA,columnB) VALUES(?,?,?)";
        sql = dialect.buildInsertStatement(tableId, columns(tableId),
                columns(tableId, "pk1", "columnA", "columnB"));
        assertEquals(expected, sql);
    }

    @Test
    public void shouldBuildUpdateStatement() {
        String expected = "UPDATE \"myTable\" SET \"columnA\" = ?, \"columnB\" = ? WHERE \"pk1\" = ? AND \"pk2\" = ?";
        String sql = dialect.buildUpdateStatement(tableId, columns(tableId, "pk1", "pk2"),
                columns(tableId, "columnA", "columnB"));
        assertEquals(expected, sql);

        quoteIdentfiiers = QuoteMethod.NEVER;
        dialect = createDialect();

        expected = "UPDATE myTable SET columnA = ?, columnB = ? WHERE pk1 = ? AND pk2 = ?";
        sql = dialect.buildUpdateStatement(tableId, columns(tableId, "pk1", "pk2"),
                columns(tableId, "columnA", "columnB"));
        assertEquals(expected, sql);
    }

    @Test
    public void shouldBuildTempTableMergeStatement() {

    }

    @Test
    public void shouldSanitizeUrlWithoutCredentialsInProperties() {
        assertSanitizedUrl(
                "jdbc:snowflake://localhost/test?user=fred&ssl=true",
                "jdbc:snowflake://localhost/test?user=fred&ssl=true"
        );
    }

    @Test
    public void shouldSanitizeUrlWithCredentialsInUrlProperties() {
        assertSanitizedUrl(
                "jdbc:snowflake://localhost/test?user=fred&password=secret&ssl=true",
                "jdbc:snowflake://localhost/test?user=fred&password=****&ssl=true"
        );
    }
}
