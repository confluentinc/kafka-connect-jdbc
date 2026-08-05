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

import io.confluent.connect.jdbc.data.Json;
import io.confluent.connect.jdbc.sink.JdbcSinkConfig;
import io.confluent.connect.jdbc.sink.metadata.SinkRecordField;
import io.confluent.connect.jdbc.source.ColumnMapping;
import io.confluent.connect.jdbc.source.JdbcSourceConnectorConfig;
import io.confluent.connect.jdbc.util.ColumnDefinition;
import io.confluent.connect.jdbc.util.ColumnId;
import io.confluent.connect.jdbc.util.QuoteMethod;
import io.confluent.connect.jdbc.util.TableDefinition;
import io.confluent.connect.jdbc.util.TableDefinitionBuilder;
import io.confluent.connect.jdbc.util.TableId;
import io.confluent.connect.jdbc.util.ExpressionBuilder;

import org.apache.kafka.connect.data.Date;
import org.apache.kafka.connect.data.Decimal;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Schema.Type;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Time;
import org.apache.kafka.connect.data.Timestamp;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.errors.DataException;
import org.junit.Test;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.JDBCType;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;


import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class PostgreSqlDatabaseDialectTest extends BaseDialectTest<PostgreSqlDatabaseDialect> {

  private static final String METADATA_CATALOG = "postgres";
  private static final String METADATA_SCHEMA = "public";
  private static final String CUSTOMERS_TABLE = "customers";
  private static final String ORDERS_TABLE = "orders";
  private static final String VARCHAR_TYPE = "varchar";

  @Override
  protected PostgreSqlDatabaseDialect createDialect() {
    return new PostgreSqlDatabaseDialect(sourceConfigWithUrl("jdbc:postgresql://something"));
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
    assertPrimitiveMapping(Type.BYTES, "BYTEA");
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
  public void testCustomColumnConverters() {
    assertColumnConverter(Types.OTHER, PostgreSqlDatabaseDialect.JSON_TYPE_NAME, Schema.STRING_SCHEMA, String.class);
    assertColumnConverter(Types.OTHER, PostgreSqlDatabaseDialect.JSONB_TYPE_NAME, Schema.STRING_SCHEMA, String.class);
    assertColumnConverter(Types.OTHER, PostgreSqlDatabaseDialect.UUID_TYPE_NAME, Schema.STRING_SCHEMA, UUID.class);
  }

  @Test
  public void logicalJsonMapsToJsonbOnlyWhenComplexTypesEnabled() {
    SinkRecordField field = new SinkRecordField(Json.optionalSchema(), "col", false);

    PostgreSqlDatabaseDialect enabled = new PostgreSqlDatabaseDialect(sinkConfigWithUrl(
        "jdbc:postgresql://something", JdbcSinkConfig.SQL_COMPLEX_TYPES_ENABLE, "true"));
    assertEquals("JSONB", enabled.getSqlType(field));

    PostgreSqlDatabaseDialect disabled =
        new PostgreSqlDatabaseDialect(sinkConfigWithUrl("jdbc:postgresql://something"));
    assertEquals("TEXT", disabled.getSqlType(field));
  }

  @Test
  public void shouldMapDataTypesForAddingColumnToTable() {
    verifyDataTypeMapping("SMALLINT", Schema.INT8_SCHEMA);
    verifyDataTypeMapping("SMALLINT", Schema.INT16_SCHEMA);
    verifyDataTypeMapping("INT", Schema.INT32_SCHEMA);
    verifyDataTypeMapping("BIGINT", Schema.INT64_SCHEMA);
    verifyDataTypeMapping("REAL", Schema.FLOAT32_SCHEMA);
    verifyDataTypeMapping("DOUBLE PRECISION", Schema.FLOAT64_SCHEMA);
    verifyDataTypeMapping("BOOLEAN", Schema.BOOLEAN_SCHEMA);
    verifyDataTypeMapping("TEXT", Schema.STRING_SCHEMA);
    verifyDataTypeMapping("BYTEA", Schema.BYTES_SCHEMA);
    verifyDataTypeMapping("DECIMAL", Decimal.schema(0));
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
        + "\"c2\" BIGINT NOT NULL,\n"
        + "\"c3\" TEXT NOT NULL,\n"
        + "\"c4\" TEXT NULL,\n"
        + "\"c5\" DATE DEFAULT '2001-03-15',\n"
        + "\"c6\" TIME DEFAULT '00:00:00.000',\n"
        + "\"c7\" TIMESTAMP DEFAULT '2001-03-15 00:00:00.000',\n"
        + "\"c8\" DECIMAL NULL,\n"
        + "\"c9\" BOOLEAN DEFAULT TRUE,\n"
        + "PRIMARY KEY(\"c1\"))",
        dialect.buildCreateTableStatement(tableId, sinkRecordFields)
    );

    quoteIdentfiiers = QuoteMethod.NEVER;
    dialect = createDialect();

    assertEquals(
        "CREATE TABLE myTable (\n"
        + "c1 INT NOT NULL,\n"
        + "c2 BIGINT NOT NULL,\n"
        + "c3 TEXT NOT NULL,\n"
        + "c4 TEXT NULL,\n"
        + "c5 DATE DEFAULT '2001-03-15',\n"
        + "c6 TIME DEFAULT '00:00:00.000',\n"
        + "c7 TIMESTAMP DEFAULT '2001-03-15 00:00:00.000',\n"
        + "c8 DECIMAL NULL,\n"
        + "c9 BOOLEAN DEFAULT TRUE,\n"
        + "PRIMARY KEY(c1))",
        dialect.buildCreateTableStatement(tableId, sinkRecordFields)
    );
  }

  @Test
  public void shouldBuildAlterTableStatement() {
    assertEquals(
        Arrays.asList(
            "ALTER TABLE \"myTable\" \n"
            + "ADD \"c1\" INT NOT NULL,\n"
            + "ADD \"c2\" BIGINT NOT NULL,\n"
            + "ADD \"c3\" TEXT NOT NULL,\n"
            + "ADD \"c4\" TEXT NULL,\n"
            + "ADD \"c5\" DATE DEFAULT '2001-03-15',\n"
            + "ADD \"c6\" TIME DEFAULT '00:00:00.000',\n"
            + "ADD \"c7\" TIMESTAMP DEFAULT '2001-03-15 00:00:00.000',\n"
            + "ADD \"c8\" DECIMAL NULL,\n"
            + "ADD \"c9\" BOOLEAN DEFAULT TRUE"
        ),
        dialect.buildAlterTable(tableId, sinkRecordFields)
    );

    quoteIdentfiiers = QuoteMethod.NEVER;
    dialect = createDialect();

    assertEquals(
        Arrays.asList(
            "ALTER TABLE myTable \n"
            + "ADD c1 INT NOT NULL,\n"
            + "ADD c2 BIGINT NOT NULL,\n"
            + "ADD c3 TEXT NOT NULL,\n"
            + "ADD c4 TEXT NULL,\n"
            + "ADD c5 DATE DEFAULT '2001-03-15',\n"
            + "ADD c6 TIME DEFAULT '00:00:00.000',\n"
            + "ADD c7 TIMESTAMP DEFAULT '2001-03-15 00:00:00.000',\n"
            + "ADD c8 DECIMAL NULL,\n"
            + "ADD c9 BOOLEAN DEFAULT TRUE"
        ),
        dialect.buildAlterTable(tableId, sinkRecordFields)
    );
  }

  @Test
  public void shouldBuildInsertStatement() {
    TableDefinitionBuilder builder = new TableDefinitionBuilder().withTable("myTable");
    builder.withColumn("id1").type("int", JDBCType.INTEGER, Integer.class);
    builder.withColumn("id2").type("int", JDBCType.INTEGER, Integer.class);
    builder.withColumn("columnA").type(VARCHAR_TYPE, JDBCType.VARCHAR, String.class);
    builder.withColumn("columnB").type(VARCHAR_TYPE, JDBCType.VARCHAR, String.class);
    builder.withColumn("columnC").type(VARCHAR_TYPE, JDBCType.VARCHAR, String.class);
    builder.withColumn("columnD").type(VARCHAR_TYPE, JDBCType.VARCHAR, String.class);
    TableDefinition tableDefn = builder.build();
    assertEquals(
        "INSERT INTO \"myTable\" (\"id1\",\"id2\",\"columnA\",\"columnB\"," +
        "\"columnC\",\"columnD\") VALUES (?,?,?,?,?,?)",
        dialect.buildInsertStatement(tableId, pkColumns, columnsAtoD, tableDefn)
    );

    quoteIdentfiiers = QuoteMethod.NEVER;
    dialect = createDialect();

    assertEquals(
        "INSERT INTO myTable (id1,id2,columnA,columnB," +
        "columnC,columnD) VALUES (?,?,?,?,?,?)",
        dialect.buildInsertStatement(tableId, pkColumns, columnsAtoD, tableDefn)
    );

    builder = new TableDefinitionBuilder().withTable("myTable");
    builder.withColumn("id1").type("int", JDBCType.INTEGER, Integer.class);
    builder.withColumn("id2").type("int", JDBCType.INTEGER, Integer.class);
    builder.withColumn("columnA").type(VARCHAR_TYPE, JDBCType.VARCHAR, Integer.class);
    builder.withColumn("uuidColumn").type("uuid", JDBCType.OTHER, UUID.class);
    builder.withColumn("dateColumn").type("date", JDBCType.DATE, java.sql.Date.class);
    tableDefn = builder.build();
    List<ColumnId> nonPkColumns = new ArrayList<>();
    nonPkColumns.add(new ColumnId(tableId, "columnA"));
    nonPkColumns.add(new ColumnId(tableId, "uuidColumn"));
    nonPkColumns.add(new ColumnId(tableId, "dateColumn"));
    assertEquals(
        "INSERT INTO myTable (" +
        "id1,id2,columnA,uuidColumn,dateColumn" +
        ") VALUES (?,?,?,?::uuid,?)",
        dialect.buildInsertStatement(tableId, pkColumns, nonPkColumns, tableDefn)
    );
  }
  @Test
  public void shouldBuildUpsertStatement() {
    TableDefinitionBuilder builder = new TableDefinitionBuilder().withTable("myTable");
    builder.withColumn("id1").type("int", JDBCType.INTEGER, Integer.class);
    builder.withColumn("id2").type("int", JDBCType.INTEGER, Integer.class);
    builder.withColumn("columnA").type(VARCHAR_TYPE, JDBCType.VARCHAR, String.class);
    builder.withColumn("columnB").type(VARCHAR_TYPE, JDBCType.VARCHAR, String.class);
    builder.withColumn("columnC").type(VARCHAR_TYPE, JDBCType.VARCHAR, String.class);
    builder.withColumn("columnD").type(VARCHAR_TYPE, JDBCType.VARCHAR, String.class);
    TableDefinition tableDefn = builder.build();
    assertEquals(
        "INSERT INTO \"myTable\" (\"id1\",\"id2\",\"columnA\",\"columnB\"," +
        "\"columnC\",\"columnD\") VALUES (?,?,?,?,?,?) ON CONFLICT (\"id1\"," +
        "\"id2\") DO UPDATE SET \"columnA\"=EXCLUDED" +
        ".\"columnA\",\"columnB\"=EXCLUDED.\"columnB\",\"columnC\"=EXCLUDED" +
        ".\"columnC\",\"columnD\"=EXCLUDED.\"columnD\"",
        dialect.buildUpsertQueryStatement(tableId, pkColumns, columnsAtoD, tableDefn)
    );

    quoteIdentfiiers = QuoteMethod.NEVER;
    dialect = createDialect();

    assertEquals(
        "INSERT INTO myTable (id1,id2,columnA,columnB," +
        "columnC,columnD) VALUES (?,?,?,?,?,?) ON CONFLICT (id1," +
        "id2) DO UPDATE SET columnA=EXCLUDED" +
        ".columnA,columnB=EXCLUDED.columnB,columnC=EXCLUDED" +
        ".columnC,columnD=EXCLUDED.columnD",
        dialect.buildUpsertQueryStatement(tableId, pkColumns, columnsAtoD, tableDefn)
    );

    builder = new TableDefinitionBuilder().withTable("myTable");
    builder.withColumn("id1").type("int", JDBCType.INTEGER, Integer.class);
    builder.withColumn("id2").type("int", JDBCType.INTEGER, Integer.class);
    builder.withColumn("columnA").type(VARCHAR_TYPE, JDBCType.VARCHAR, Integer.class);
    builder.withColumn("uuidColumn").type("uuid", JDBCType.OTHER, UUID.class);
    builder.withColumn("dateColumn").type("date", JDBCType.DATE, java.sql.Date.class);
    tableDefn = builder.build();
    List<ColumnId> nonPkColumns = new ArrayList<>();
    nonPkColumns.add(new ColumnId(tableId, "columnA"));
    nonPkColumns.add(new ColumnId(tableId, "uuidColumn"));
    nonPkColumns.add(new ColumnId(tableId, "dateColumn"));
    assertEquals(
        "INSERT INTO myTable (" +
        "id1,id2,columnA,uuidColumn,dateColumn" +
        ") VALUES (?,?,?,?::uuid,?) ON CONFLICT (id1," +
        "id2) DO UPDATE SET " +
        "columnA=EXCLUDED.columnA," +
        "uuidColumn=EXCLUDED.uuidColumn," +
        "dateColumn=EXCLUDED.dateColumn",
        dialect.buildUpsertQueryStatement(tableId, pkColumns, nonPkColumns, tableDefn)
    );
  }

  @Test
  public void shouldComputeValueTypeCast() {
    TableDefinitionBuilder builder = new TableDefinitionBuilder().withTable("myTable");
    builder.withColumn("id1").type("int", JDBCType.INTEGER, Integer.class);
    builder.withColumn("id2").type("int", JDBCType.INTEGER, Integer.class);
    builder.withColumn("columnA").type(VARCHAR_TYPE, JDBCType.VARCHAR, Integer.class);
    builder.withColumn("uuidColumn").type("uuid", JDBCType.OTHER, UUID.class);
    builder.withColumn("dateColumn").type("date", JDBCType.DATE, java.sql.Date.class);
    builder.withColumn("jsonbColumn").type("jsonb", JDBCType.OTHER, String.class);
    TableDefinition tableDefn = builder.build();
    ColumnId uuidColumn = tableDefn.definitionForColumn("uuidColumn").id();
    ColumnId dateColumn = tableDefn.definitionForColumn("dateColumn").id();
    ColumnId jsonbColumn = tableDefn.definitionForColumn("jsonbColumn").id();
    assertEquals("", dialect.valueTypeCast(tableDefn, columnPK1));
    assertEquals("", dialect.valueTypeCast(tableDefn, columnPK2));
    assertEquals("", dialect.valueTypeCast(tableDefn, columnA));
    assertEquals("::uuid", dialect.valueTypeCast(tableDefn, uuidColumn));
    assertEquals("", dialect.valueTypeCast(tableDefn, dateColumn));
    // The cast that turns the bound JSON text into jsonb server-side.
    assertEquals("::jsonb", dialect.valueTypeCast(tableDefn, jsonbColumn));
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
    verifyAlterAddTwoCols(
        "ALTER TABLE \"myTable\" " + System.lineSeparator() + "ADD \"newcol1\" INT NULL," +
        System.lineSeparator() + "ADD \"newcol2\" INT DEFAULT 42");
  }

  @Test
  public void upsert() {
    TableDefinitionBuilder builder = new TableDefinitionBuilder().withTable("Customer");
    builder.withColumn("id").type("int", JDBCType.INTEGER, Integer.class);
    builder.withColumn("name").type(VARCHAR_TYPE, JDBCType.VARCHAR, String.class);
    builder.withColumn("salary").type("real", JDBCType.FLOAT, String.class);
    builder.withColumn("address").type(VARCHAR_TYPE, JDBCType.VARCHAR, String.class);
    TableDefinition tableDefn = builder.build();
    TableId customer = tableDefn.id();
    assertEquals(
        "INSERT INTO \"Customer\" (\"id\",\"name\",\"salary\",\"address\") " +
         "VALUES (?,?,?,?) ON CONFLICT (\"id\") DO UPDATE SET \"name\"=EXCLUDED.\"name\"," +
         "\"salary\"=EXCLUDED.\"salary\",\"address\"=EXCLUDED.\"address\"",
        dialect.buildUpsertQueryStatement(
            customer,
            columns(customer, "id"),
            columns(customer, "name", "salary", "address"),
            tableDefn
        )
    );

    assertEquals(
            "INSERT INTO \"Customer\" (\"id\",\"name\",\"salary\",\"address\") " +
                    "VALUES (?,?,?,?) ON CONFLICT (\"id\",\"name\",\"salary\",\"address\") DO NOTHING",
            dialect.buildUpsertQueryStatement(
                    customer,
                    columns(customer, "id", "name", "salary", "address"),
                    columns(customer),
                    tableDefn
            )
    );

    quoteIdentfiiers = QuoteMethod.NEVER;
    dialect = createDialect();

    assertEquals(
        "INSERT INTO Customer (id,name,salary,address) " +
        "VALUES (?,?,?,?) ON CONFLICT (id) DO UPDATE SET name=EXCLUDED.name," +
        "salary=EXCLUDED.salary,address=EXCLUDED.address",
        dialect.buildUpsertQueryStatement(
            customer,
            columns(customer, "id"),
            columns(customer, "name", "salary", "address"),
            tableDefn
        )
    );

    assertEquals(
            "INSERT INTO Customer (id,name,salary,address) " +
                    "VALUES (?,?,?,?) ON CONFLICT (id,name,salary,address) DO NOTHING",
            dialect.buildUpsertQueryStatement(
                    customer,
                    columns(customer, "id", "name", "salary", "address"),
                    columns(customer),
                    tableDefn
            )
    );
  }

  @Test
  public void shouldSanitizeUrlWithoutCredentialsInProperties() {
    assertSanitizedUrl(
        "jdbc:postgresql://localhost/test?user=fred&ssl=true",
        "jdbc:postgresql://localhost/test?user=fred&ssl=true"
    );
  }

  @Test
  public void shouldSanitizeUrlWithCredentialsInUrlProperties() {
    assertSanitizedUrl(
        "jdbc:postgresql://localhost/test?user=fred&password=secret&ssl=true",
        "jdbc:postgresql://localhost/test?user=fred&password=****&ssl=true"
    );
  }

  @Test
  @Override
  public void bindFieldArrayUnsupported() throws SQLException {
      // Overridden simply to dummy out the test.
  }

  @Test
  public void bindFieldPrimitiveValues() throws SQLException {
    PreparedStatement statement = mock(PreparedStatement.class);
    int index = ThreadLocalRandom.current().nextInt();

    super.verifyBindField(++index, SchemaBuilder.array(Schema.INT32_SCHEMA), Collections.singletonList(42)).setObject(index, new Object[] { 42 }, Types.ARRAY);
    super.verifyBindField(++index, SchemaBuilder.array(Schema.INT8_SCHEMA), Arrays.asList( (byte) 42, (byte) 12)).setObject(index, new Object[] { (short)42, (short)12 }, Types.ARRAY);
    super.verifyBindField(++index, SchemaBuilder.array(Schema.INT16_SCHEMA), Arrays.asList( (short) 42, (short) 12)).setObject(index, new Object[] { (short)42, (short)12 }, Types.ARRAY);
    super.verifyBindField(++index, SchemaBuilder.array(Schema.INT32_SCHEMA), Arrays.asList(42, 16 )).setObject(index, new Object[] { 42, 16 }, Types.ARRAY);
    super.verifyBindField(++index, SchemaBuilder.array(Schema.INT64_SCHEMA), Arrays.asList(42L, 16L )).setObject(index, new Object[] { (long)42, (long)16 }, Types.ARRAY);
    super.verifyBindField(++index, SchemaBuilder.array(Schema.FLOAT32_SCHEMA), Arrays.asList(42.5F, 16.2F )).setObject(index, new Object[] { 42.5F, 16.2F }, Types.ARRAY);
    super.verifyBindField(++index, SchemaBuilder.array(Schema.FLOAT64_SCHEMA), Arrays.asList(42.5D, 16.2D )).setObject(index, new Object[] { 42.5D, 16.2D }, Types.ARRAY);
    super.verifyBindField(++index, SchemaBuilder.array(Schema.STRING_SCHEMA), Arrays.asList("42", "16" )).setObject(index, new Object[] { "42", "16" }, Types.ARRAY);
    super.verifyBindField(++index, SchemaBuilder.array(Schema.BOOLEAN_SCHEMA), Arrays.asList(true, false, true )).setObject(index, new Object[] { true, false, true }, Types.ARRAY);
  }

  @Test
  public void shouldComputeMaxTableNameLength() throws Exception {
    int expectedMaxLength = 24;
    ResultSet resultSet = mock(ResultSet.class);
    when(resultSet.next()).thenReturn(true);
    when(resultSet.getInt(1)).thenReturn(expectedMaxLength);

    Statement statement = mock(Statement.class);
    when(statement.executeQuery("SELECT length(repeat('1234567890', 1000)::NAME);"))
        .thenReturn(resultSet);

    Connection connection = mock(Connection.class);
    when(connection.createStatement()).thenReturn(statement);

    int actualMaxLength = PostgreSqlDatabaseDialect.computeMaxIdentifierLength(connection);

    assertEquals(expectedMaxLength, actualMaxLength);
  }

  @Test
  public void shouldGracefullyHandleErrorWhenComputingMaxTableNameLength() throws Exception {
    Statement statement = mock(Statement.class);
    when(statement.executeQuery("SELECT length(repeat('1234567890', 1000)::NAME);"))
        .thenThrow(new SQLException("I plead the fifth"));

    Connection connection = mock(Connection.class);
    when(connection.createStatement()).thenReturn(statement);

    int actualMaxLength = PostgreSqlDatabaseDialect.computeMaxIdentifierLength(connection);

    assertEquals(Integer.MAX_VALUE, actualMaxLength);
  }

  @Test
  public void shouldGracefullyHandleEmptyResultSetWhenComputingMaxTableNameLength() throws Exception {
    ResultSet resultSet = mock(ResultSet.class);
    when(resultSet.next()).thenReturn(false);

    Statement statement = mock(Statement.class);
    when(statement.executeQuery("SELECT length(repeat('1234567890', 1000)::NAME);"))
        .thenReturn(resultSet);

    Connection connection = mock(Connection.class);
    when(connection.createStatement()).thenReturn(statement);

    int actualMaxLength = PostgreSqlDatabaseDialect.computeMaxIdentifierLength(connection);

    assertEquals(Integer.MAX_VALUE, actualMaxLength);
  }

  @Test
  public void shouldGracefullyHandleInvalidValueWhenComputingMaxTableNameLength() throws Exception {
    ResultSet resultSet = mock(ResultSet.class);
    when(resultSet.next()).thenReturn(true);
    when(resultSet.getInt(1)).thenReturn(0);

    Statement statement = mock(Statement.class);
    when(statement.executeQuery("SELECT length(repeat('1234567890', 1000)::NAME);"))
        .thenReturn(resultSet);

    Connection connection = mock(Connection.class);
    when(connection.createStatement()).thenReturn(statement);

    int actualMaxLength = PostgreSqlDatabaseDialect.computeMaxIdentifierLength(connection);

    assertEquals(Integer.MAX_VALUE, actualMaxLength);
  }

  @Test
  public void shouldStripCatalogFromDiscoveredTableIds() throws Exception {
    ResultSet tableTypesRs = mock(ResultSet.class);
    when(tableTypesRs.next()).thenReturn(true, false);
    when(tableTypesRs.getString(1)).thenReturn("TABLE");

    // pgjdbc 42.7.5+ populates TABLE_CAT with the database name; older drivers returned null
    ResultSet tablesRs = mock(ResultSet.class);
    when(tablesRs.next()).thenReturn(true, true, false);
    when(tablesRs.getString(1)).thenReturn(METADATA_CATALOG, METADATA_CATALOG);
    when(tablesRs.getString(2)).thenReturn(METADATA_SCHEMA, "app");
    when(tablesRs.getString(3)).thenReturn(CUSTOMERS_TABLE, ORDERS_TABLE);

    DatabaseMetaData metadata = mock(DatabaseMetaData.class);
    when(metadata.getTableTypes()).thenReturn(tableTypesRs);
    when(metadata.getTables(any(), any(), eq("%"), any(String[].class))).thenReturn(tablesRs);

    Connection connection = mock(Connection.class);
    when(connection.getMetaData()).thenReturn(metadata);

    assertEquals(
        Arrays.asList(
            new TableId(null, METADATA_SCHEMA, CUSTOMERS_TABLE),
            new TableId(null, "app", ORDERS_TABLE)
        ),
        dialect.tableIds(connection)
    );
  }

  @Test
  public void shouldKeepTwoPartTableIdsOnOlderDrivers() throws Exception {
    ResultSet tableTypesRs = mock(ResultSet.class);
    when(tableTypesRs.next()).thenReturn(true, false);
    when(tableTypesRs.getString(1)).thenReturn("TABLE");

    // Drivers before 42.7.5 return a null TABLE_CAT; an empty string is covered for safety
    ResultSet tablesRs = mock(ResultSet.class);
    when(tablesRs.next()).thenReturn(true, true, false);
    when(tablesRs.getString(1)).thenReturn(null, "");
    when(tablesRs.getString(2)).thenReturn(METADATA_SCHEMA, "app");
    when(tablesRs.getString(3)).thenReturn(CUSTOMERS_TABLE, ORDERS_TABLE);

    DatabaseMetaData metadata = mock(DatabaseMetaData.class);
    when(metadata.getTableTypes()).thenReturn(tableTypesRs);
    when(metadata.getTables(any(), any(), eq("%"), any(String[].class))).thenReturn(tablesRs);

    Connection connection = mock(Connection.class);
    when(connection.getMetaData()).thenReturn(metadata);

    assertEquals(
        Arrays.asList(
            new TableId(null, METADATA_SCHEMA, CUSTOMERS_TABLE),
            new TableId(null, "app", ORDERS_TABLE)
        ),
        dialect.tableIds(connection)
    );
  }

  @Test
  public void shouldPreserveCatalogFromParsedTableIdentifiers() {
    // Configured names (e.g. table.name.format) are user intent, not driver metadata, so the
    // catalog strip does NOT apply here. A configured database must survive parsing: dropping
    // it would let JdbcDbWriter back-fill the connected database and silently write to the
    // wrong one instead of failing loudly on the cross-database reference.
    assertEquals(
        new TableId("mydb", METADATA_SCHEMA, CUSTOMERS_TABLE),
        dialect.parseTableIdentifier("mydb." + METADATA_SCHEMA + "." + CUSTOMERS_TABLE)
    );
    assertEquals(
        new TableId(null, METADATA_SCHEMA, CUSTOMERS_TABLE),
        dialect.parseTableIdentifier(METADATA_SCHEMA + "." + CUSTOMERS_TABLE)
    );
  }

  @Test
  public void shouldStripCatalogFromMetadataColumnIds() throws Exception {
    // getPrimaryKeys and getColumns both report TABLE_CAT on pgjdbc 42.7.5+; the seam must
    // normalize both sides of the pkColumns.contains comparison, not just discovered tables.
    ResultSet pkRs = mock(ResultSet.class);
    when(pkRs.next()).thenReturn(true, false);
    when(pkRs.getString(1)).thenReturn(METADATA_CATALOG);
    when(pkRs.getString(2)).thenReturn(METADATA_SCHEMA);
    when(pkRs.getString(3)).thenReturn(CUSTOMERS_TABLE);
    when(pkRs.getString(4)).thenReturn("id");

    ResultSetMetaData colsRsMetadata = mock(ResultSetMetaData.class);
    when(colsRsMetadata.getColumnCount()).thenReturn(12);

    ResultSet colsRs = mock(ResultSet.class);
    when(colsRs.getMetaData()).thenReturn(colsRsMetadata);
    when(colsRs.next()).thenReturn(true, true, false);
    when(colsRs.getString(1)).thenReturn(METADATA_CATALOG, METADATA_CATALOG);
    when(colsRs.getString(2)).thenReturn(METADATA_SCHEMA, METADATA_SCHEMA);
    when(colsRs.getString(3)).thenReturn(CUSTOMERS_TABLE, CUSTOMERS_TABLE);
    when(colsRs.getString(4)).thenReturn("id", "name");
    when(colsRs.getInt(5)).thenReturn(Types.INTEGER, Types.VARCHAR);
    when(colsRs.getString(6)).thenReturn("int4", VARCHAR_TYPE);

    DatabaseMetaData metadata = mock(DatabaseMetaData.class);
    when(metadata.getPrimaryKeys(METADATA_CATALOG, METADATA_SCHEMA, CUSTOMERS_TABLE)).thenReturn(pkRs);
    when(metadata.getColumns(METADATA_CATALOG, METADATA_SCHEMA, CUSTOMERS_TABLE, null)).thenReturn(colsRs);

    Connection connection = mock(Connection.class);
    when(connection.getMetaData()).thenReturn(metadata);

    Map<ColumnId, ColumnDefinition> defns =
        dialect.describeColumns(connection, METADATA_CATALOG, METADATA_SCHEMA, CUSTOMERS_TABLE, null);

    TableId expectedTableId = new TableId(null, METADATA_SCHEMA, CUSTOMERS_TABLE);
    assertEquals(2, defns.size());
    for (ColumnId columnId : defns.keySet()) {
      assertEquals(expectedTableId, columnId.tableId());
    }
    // The pk flag is only set when the pk id and column id agree on the identifier, i.e.
    // both metadata paths were normalized consistently.
    assertTrue(defns.get(new ColumnId(expectedTableId, "id")).isPrimaryKey());
  }

  @Test
  public void shouldTruncateTableNames() {

    final String tableFqn = "some.table";

    // Table name is one byte longer than it's allowed to be; should be truncated
    dialect.maxIdentifierLength = 4;
    TableId expectedTableId = new TableId(
        null,
        "some",
        "tabl"
    );
    TableId actualTableId = dialect.parseTableIdentifier(tableFqn);
    assertEquals(expectedTableId, actualTableId);

    // Table name is exactly as long as it's allowed to be; should not be truncated
    dialect.maxIdentifierLength = 5;
    expectedTableId = new TableId(
        null,
        "some",
        "table"
    );
    actualTableId = dialect.parseTableIdentifier(tableFqn);
    assertEquals(expectedTableId, actualTableId);

    // Something went wrong when computing the max length
    dialect.maxIdentifierLength = Integer.MAX_VALUE;
    expectedTableId = new TableId(
        null,
        "some",
        "table"
    );
    actualTableId = dialect.parseTableIdentifier(tableFqn);
    assertEquals(expectedTableId, actualTableId);

    // We haven't computed the max length at all yet
    dialect.maxIdentifierLength = 0;
    expectedTableId = new TableId(
        null,
        "some",
        "table"
    );
    actualTableId = dialect.parseTableIdentifier(tableFqn);
    assertEquals(expectedTableId, actualTableId);
  }

  @Test
  public void shouldFallBackOnUnknownDecimalScale() {
    ColumnId columnId = new ColumnId(new TableId("catalog", "schema", "table"), "column");
    ColumnDefinition definition = mock(ColumnDefinition.class);
    when(definition.id()).thenReturn(columnId);

    when(definition.precision()).thenReturn(4);
    when(definition.scale()).thenReturn(GenericDatabaseDialect.NUMERIC_TYPE_SCALE_UNSET);

    assertEquals(GenericDatabaseDialect.NUMERIC_TYPE_SCALE_HIGH, dialect.decimalScale(definition));
  }

  @Test
  public void shouldFallBackOnUnfixedDecimalScale() {
    ColumnId columnId = new ColumnId(new TableId("catalog", "schema", "table"), "column");
    ColumnDefinition definition = mock(ColumnDefinition.class);
    when(definition.id()).thenReturn(columnId);

    when(definition.precision()).thenReturn(0);
    when(definition.scale()).thenReturn(0);

    assertEquals(GenericDatabaseDialect.NUMERIC_TYPE_SCALE_HIGH, dialect.decimalScale(definition));
  }

  @Test
  public void shouldNotFallBackOnKnownDecimalScale() {
    ColumnId columnId = new ColumnId(new TableId("catalog", "schema", "table"), "column");
    ColumnDefinition definition = mock(ColumnDefinition.class);
    when(definition.id()).thenReturn(columnId);

    when(definition.precision()).thenReturn(0);
    when(definition.scale()).thenReturn(5);

    assertEquals(5, dialect.decimalScale(definition));
  }
  @Test
  public void testArrayDefaultsFormatting() {
    PostgreSqlDatabaseDialect dialect = createDialect();

    verifyArrayFormatting(dialect, new ExpressionBuilder(),
            Collections.emptyList(),
            "ARRAY[]");

    verifyArrayFormatting(dialect,  new ExpressionBuilder(),
            Arrays.asList("simple", "string", "array"),
            "ARRAY['simple','string','array']");

    verifyArrayFormatting(dialect,  new ExpressionBuilder(),
            Arrays.asList("Van'Der Waal", "O'Neill", "l'église"),
            "ARRAY['Van''Der Waal','O''Neill','l''église']");

    verifyArrayFormatting(dialect,  new ExpressionBuilder(),
            Arrays.asList("double''quote", "already''escaped"),
            "ARRAY['double''''quote','already''''escaped']");

    verifyArrayFormatting(dialect,  new ExpressionBuilder(),
            Arrays.asList("contains \"quotes\"", "and 'apostrophes'"),
            "ARRAY['contains \"quotes\"','and ''apostrophes''']");

    verifyArrayFormatting(dialect,  new ExpressionBuilder(),
            Arrays.asList("backslash\\test", "percent%sign"),
            "ARRAY['backslash\\test','percent%sign']");


    verifyArrayFormatting(dialect,  new ExpressionBuilder(),
            Arrays.asList("newline\ntest", "tab\ttest", "return\rtest"),
            "ARRAY['newline\ntest','tab\ttest','return\rtest']");

    verifyArrayFormatting(dialect,  new ExpressionBuilder(),
            Arrays.asList("mixed", "array", null, "with", "null"),
            "ARRAY['mixed','array',NULL,'with','null']");

    verifyArrayFormatting(dialect, new ExpressionBuilder(),
            Arrays.asList("1", "2", "3", "4", "5"),
            "ARRAY['1','2','3','4','5']");

    verifyArrayFormatting(dialect, new ExpressionBuilder(),
            Arrays.asList(1, 2, 3, 4, 5),
            "ARRAY[1,2,3,4,5]");

    verifyArrayFormatting(dialect, new ExpressionBuilder(),
            Arrays.asList(true, false, true),
            "ARRAY[TRUE,FALSE,TRUE]");
  }

  private <T> void verifyArrayFormatting(PostgreSqlDatabaseDialect dialect, ExpressionBuilder builder,
                                         List<T> input, String expected) {
    dialect.formatColumnValue(builder, null, null, Schema.Type.ARRAY, input);
    assertEquals(expected, builder.toString());
  }


  // validateQuery is inherited from GenericDatabaseDialect; tested in GenericDatabaseDialectTest.

  // ========== Complex SQL types (sql.complex.types.enable) ==========

  @Test
  public void hstoreHandlingModeShouldSelectSourceSchema() {
    // "map": a Map<String,String>. Not the default, which is "none".
    assertEquals(Type.MAP,
        sourceFieldSchema(hstoreDialect("true", "map"), Types.OTHER, "hstore").type());

    // "json": a STRING tagged as the Json logical type, which the sink lands in JSONB.
    PostgreSqlDatabaseDialect jsonDialect = complexTypesDialect(
        JdbcSourceConnectorConfig.HSTORE_HANDLING_MODE_CONFIG, "json");
    Schema jsonMode = sourceFieldSchema(jsonDialect, Types.OTHER, "hstore");
    assertEquals(Type.STRING, jsonMode.type());
    assertEquals(Json.LOGICAL_NAME, jsonMode.name());
  }

  @Test
  public void hstoreJsonModeShouldConvertValueToJsonObjectString() throws Exception {
    // In json mode the driver's hstore Map is serialized to a JSON-object STRING on the topic.
    PostgreSqlDatabaseDialect jsonDialect = complexTypesDialect(
        JdbcSourceConnectorConfig.HSTORE_HANDLING_MODE_CONFIG, "json");
    Map<String, String> hstore = new LinkedHashMap<>();
    hstore.put("env", "prod");
    hstore.put("region", "us-west-2");

    assertEquals("{\"env\":\"prod\",\"region\":\"us-west-2\"}",
        hstoreConverter(jsonDialect).convert(hstoreResultSet(hstore)));
  }

  @Test
  public void hstoreMapModeShouldPassThroughDriverMap() throws Exception {
    // In map mode the driver's Map is emitted as-is for the Connect MAP schema.
    Map<String, String> hstore = Collections.singletonMap("env", "prod");
    assertEquals(hstore,
        hstoreConverter(hstoreDialect("true", "map")).convert(hstoreResultSet(hstore)));
  }

  @Test
  public void hstoreSourceSchemaShouldMapToSinkSqlTypePerMode() {
    // Starts from the schema the source path actually produces for an hstore column, so this
    // exercises hstoreSchema() rather than re-asserting generic MAP/STRING behaviour.
    Schema mapMode = sourceFieldSchema(hstoreDialect("true", "map"), Types.OTHER, "hstore");
    assertEquals("JSONB", sinkDialect().getSqlType(sinkField(mapMode)));

    PostgreSqlDatabaseDialect jsonDialect = complexTypesDialect(
        JdbcSourceConnectorConfig.HSTORE_HANDLING_MODE_CONFIG, "json");
    Schema jsonMode = sourceFieldSchema(jsonDialect, Types.OTHER, "hstore");
    assertEquals("JSONB", sinkDialect().getSqlType(sinkField(jsonMode)));
  }

  @Test
  public void shouldBindStringMapAsJsonTextForJsonbColumn() throws SQLException {
    // The value half of MAP -> JSONB: the map is serialized and bound as text, which the ::jsonb
    // cast then parses server-side. Only the DDL half was covered before.
    Map<String, String> value = new LinkedHashMap<>();
    value.put("env", "prod");
    value.put("absent", null);
    Schema schema = SchemaBuilder.map(Schema.STRING_SCHEMA, Schema.OPTIONAL_STRING_SCHEMA)
        .optional().build();

    PreparedStatement statement = mock(PreparedStatement.class);
    sinkDialect().bindField(statement, 1, schema, value);
    verify(statement).setString(1, "{\"env\":\"prod\",\"absent\":null}");

    // A null map never reaches maybeBindJson: bindFieldInternal short-circuits nulls before
    // maybeBindPrimitive, so the generic null path binds it.
    PreparedStatement nullStatement = mock(PreparedStatement.class);
    sinkDialect().bindField(nullStatement, 1, schema, null);
    verify(nullStatement).setObject(1, null);
  }

  @Test
  public void shouldBindJsonStringAsTextForJsonbColumn() throws SQLException {
    // The bind half for json mode: a Json-tagged STRING is not a string-to-string map, so
    // maybeBindJson declines and it binds as text — the ::jsonb cast parses it server-side.
    PreparedStatement statement = mock(PreparedStatement.class);
    sinkDialect().bindField(statement, 1, Json.optionalSchema(), "{\"env\":\"prod\"}");
    verify(statement).setString(1, "{\"env\":\"prod\"}");
  }

  @Test
  public void shouldNotBindStringMapWhenComplexTypesDisabled() {
    Schema schema = SchemaBuilder.map(Schema.STRING_SCHEMA, Schema.OPTIONAL_STRING_SCHEMA)
        .optional().build();
    PostgreSqlDatabaseDialect disabled = new PostgreSqlDatabaseDialect(
        sinkConfigWithUrl("jdbc:postgresql://something"));

    assertThrows(ConnectException.class, () -> disabled.bindField(
        mock(PreparedStatement.class), 1, schema, Collections.singletonMap("env", "prod")));
  }

  @Test
  public void shouldDropHstoreWhenHandlingModeIsNone() {
    // The flag alone is not enough: none is the default, so hstore stays skipped until a mode is
    // chosen. Both halves of the gate are required.
    PostgreSqlDatabaseDialect defaulted = new PostgreSqlDatabaseDialect(
        sourceConfigWithUrl("jdbc:postgresql://something",
            JdbcSourceConnectorConfig.SQL_COMPLEX_TYPES_ENABLE_CONFIG, "true"));
    assertNull(sourceFieldSchema(defaulted, Types.OTHER, "hstore"));
    assertNull(sourceFieldSchema(hstoreDialect("true", "none"), Types.OTHER, "hstore"));
  }

  @Test
  public void offSearchPathHstoreDoesNotWarnWhenHandlingModeIsNone() {
    // An operator who asked for none should not be told about a type they chose to ignore.
    CollectingAppender appender = new CollectingAppender();
    org.apache.log4j.Logger logger =
        org.apache.log4j.Logger.getLogger(PostgreSqlDatabaseDialect.class);
    org.apache.log4j.Level originalLevel = logger.getLevel();
    logger.setLevel(org.apache.log4j.Level.WARN);
    logger.addAppender(appender);
    try {
      assertNull(sourceFieldSchema(
          hstoreDialect("true", "none"), Types.OTHER, "\"ext\".\"hstore\""));
      assertTrue("none must not warn about search_path", appender.warnings.isEmpty());
    } finally {
      logger.removeAppender(appender);
      logger.setLevel(originalLevel);
    }
  }

  @Test
  public void shouldDropHstoreWhenComplexTypesDisabled() {
    // The default is false, so hstore keeps today's drop-with-WARN behaviour and produces no field.
    PostgreSqlDatabaseDialect disabled =
        new PostgreSqlDatabaseDialect(sourceConfigWithUrl("jdbc:postgresql://something"));
    assertNull(sourceFieldSchema(disabled, Types.OTHER, "hstore"));
  }

  @Test
  public void hstoreValueThatIsNotAMapShouldFollowColumnNullability() throws Exception {
    // Defence in depth for any non-Map shape the driver might hand back. Off the search_path the
    // type name is qualified, so the column is skipped before this runs; whatever else could reach
    // here has no known cause. Follows Debezium's handleUnknownData: a nullable column degrades to
    // null, a NOT NULL column fails because null would breach its schema anyway.
    ResultSet rawText = hstoreResultSet("\"env\"=>\"prod\"");

    PostgreSqlDatabaseDialect jsonDialect = complexTypesDialect(
        JdbcSourceConnectorConfig.HSTORE_HANDLING_MODE_CONFIG, "json");

    for (PostgreSqlDatabaseDialect dialect : Arrays.asList(hstoreDialect("true", "map"), jsonDialect)) {
      assertNull(hstoreConverter(dialect, ColumnDefinition.Nullability.NULL).convert(rawText));

      DataException e = assertThrows(DataException.class, () ->
          hstoreConverter(dialect, ColumnDefinition.Nullability.NOT_NULL).convert(rawText));
      assertTrue(e.getMessage().contains("hstore"));
    }
  }

  @Test
  public void hstoreShouldConvertNullColumnToNullInBothModes() throws Exception {
    PostgreSqlDatabaseDialect jsonDialect = complexTypesDialect(
        JdbcSourceConnectorConfig.HSTORE_HANDLING_MODE_CONFIG, "json");

    for (PostgreSqlDatabaseDialect dialect : Arrays.asList(hstoreDialect("true", "map"), jsonDialect)) {
      assertNull(hstoreConverter(dialect).convert(hstoreResultSet(null)));
    }
  }

  @Test
  public void shouldRejectMapShapesOtherThanStringToString() {
    // Only MAP<STRING,STRING> — the shape hstore produces — maps to JSONB. Every other map shape
    // must fall through to the generic dialect and fail rather than silently become jsonb.
    List<Schema> unsupported = Arrays.asList(
        SchemaBuilder.map(Schema.STRING_SCHEMA, Schema.INT32_SCHEMA).optional().build(),
        SchemaBuilder.map(Schema.INT32_SCHEMA, Schema.STRING_SCHEMA).optional().build(),
        SchemaBuilder.map(Schema.STRING_SCHEMA, Schema.OPTIONAL_BYTES_SCHEMA).optional().build(),
        SchemaBuilder.map(Schema.STRING_SCHEMA,
            SchemaBuilder.map(Schema.STRING_SCHEMA, Schema.STRING_SCHEMA).build()).build(),
        SchemaBuilder.map(Schema.STRING_SCHEMA,
            SchemaBuilder.array(Schema.STRING_SCHEMA).build()).build());

    for (Schema schema : unsupported) {
      assertThrows("expected " + schema.valueSchema().type() + " map value to be rejected",
          ConnectException.class, () -> sinkDialect().getSqlType(sinkField(schema)));
    }
  }

  @Test
  public void shouldNotBindMapShapesOtherThanStringToString() throws SQLException {
    // The bind half of the same restriction, with complex types enabled.
    Schema intValued = SchemaBuilder.map(Schema.STRING_SCHEMA, Schema.INT32_SCHEMA)
        .optional().build();
    assertThrows(ConnectException.class, () -> sinkDialect().bindField(
        mock(PreparedStatement.class), 1, intValued, Collections.singletonMap("n", 1)));

    Schema intKeyed = SchemaBuilder.map(Schema.INT32_SCHEMA, Schema.STRING_SCHEMA)
        .optional().build();
    assertThrows(ConnectException.class, () -> sinkDialect().bindField(
        mock(PreparedStatement.class), 1, intKeyed, Collections.singletonMap(1, "v")));
  }

  @Test
  public void offSearchPathHstoreWarnsOncePerColumn() {
    CollectingAppender appender = new CollectingAppender();
    org.apache.log4j.Logger logger =
        org.apache.log4j.Logger.getLogger(PostgreSqlDatabaseDialect.class);
    // The test log config pins io.confluent.connect to ERROR, which would filter the warning out
    // before any appender sees it.
    org.apache.log4j.Level originalLevel = logger.getLevel();
    logger.setLevel(org.apache.log4j.Level.WARN);
    logger.addAppender(appender);
    try {
      PostgreSqlDatabaseDialect dialect = hstoreDialect("true", "map");
      SchemaBuilder builder = SchemaBuilder.struct();
      String offPath = "\"ext\".\"hstore\"";

      assertNull("the column stays unsupported",
          dialect.addFieldToSchema(column(Types.OTHER, offPath, "a"), builder));
      assertEquals("first occurrence must warn", 1, appender.warnings.size());
      assertTrue("the warning must name the actionable cause",
          appender.warnings.get(0).contains("search_path"));

      // The schema is rebuilt every query cycle, so the same column must not warn again.
      assertNull(dialect.addFieldToSchema(column(Types.OTHER, offPath, "a"), builder));
      assertEquals("repeat on the same column must not warn again", 1, appender.warnings.size());

      assertNull(dialect.addFieldToSchema(column(Types.OTHER, offPath, "b"), builder));
      assertEquals("a second column must warn on its own", 2, appender.warnings.size());

      // An unrelated Types.OTHER type must not attract the hstore hint.
      assertNull(dialect.addFieldToSchema(column(Types.OTHER, "citext", "c"), builder));
      assertEquals("an unrelated type must not warn", 2, appender.warnings.size());
    } finally {
      logger.removeAppender(appender);
      logger.setLevel(originalLevel);
    }
  }

  /** Collects WARN events from the dialect's logger so the dedupe can be asserted. */
  private static class CollectingAppender extends org.apache.log4j.AppenderSkeleton {
    private final List<String> warnings = new ArrayList<>();

    @Override
    protected void append(org.apache.log4j.spi.LoggingEvent event) {
      if (event.getLevel().isGreaterOrEqual(org.apache.log4j.Level.WARN)) {
        warnings.add(event.getRenderedMessage());
      }
    }

    @Override
    public void close() {
    }

    @Override
    public boolean requiresLayout() {
      return false;
    }
  }

  @Test
  public void shouldNotTreatNonHstoreOtherTypesAsHstore() {
    // Another Types.OTHER type must not be captured by the hstore branch just because the flag is
    // on. The qualified name is the off-search_path rendering, which stays unsupported.
    PostgreSqlDatabaseDialect dialect = hstoreDialect("true", "map");
    assertNull(sourceFieldSchema(dialect, Types.OTHER, "citext"));
    assertNull(sourceFieldSchema(dialect, Types.OTHER, "hstore_extra"));
    assertNull(sourceFieldSchema(dialect, Types.OTHER, "\"ext\".\"hstore\""));
  }

  @Test
  public void shouldMatchHstoreTypeNameCaseInsensitively() {
    // The driver's reported type name casing must not decide whether the feature works.
    assertEquals(Type.MAP,
        sourceFieldSchema(hstoreDialect("true", "map"), Types.OTHER, "HSTORE").type());
  }

  // ----- complex-type test helpers -----

  private PostgreSqlDatabaseDialect sinkDialect() {
    return new PostgreSqlDatabaseDialect(sinkConfigWithUrl(
        "jdbc:postgresql://something", JdbcSinkConfig.SQL_COMPLEX_TYPES_ENABLE, "true"));
  }

  private static SinkRecordField sinkField(Schema schema) {
    return new SinkRecordField(schema, "col", false);
  }

  /** Column converter for an {@code hstore} column, as produced by the source path. */
  private DatabaseDialect.ColumnConverter hstoreConverter(PostgreSqlDatabaseDialect dialect) {
    return hstoreConverter(dialect, ColumnDefinition.Nullability.NULL);
  }

  private DatabaseDialect.ColumnConverter hstoreConverter(
      PostgreSqlDatabaseDialect dialect, ColumnDefinition.Nullability nullability) {
    ColumnDefinition column = column(Types.OTHER, "hstore", nullability);
    DatabaseDialect.ColumnConverter converter = dialect.columnConverterFor(
        new ColumnMapping(column, 1, new Field("col", 0, Schema.OPTIONAL_STRING_SCHEMA)),
        column, 1, true);
    assertNotNull(converter);
    return converter;
  }

  /** A ResultSet whose column 1 returns the given hstore value, as pgjdbc does. */
  private static ResultSet hstoreResultSet(Object value) throws SQLException {
    ResultSet resultSet = mock(ResultSet.class);
    when(resultSet.getObject(1)).thenReturn(value);
    return resultSet;
  }

  private PostgreSqlDatabaseDialect complexTypesDialect(String... extraProps) {
    String[] props = new String[extraProps.length + 2];
    props[0] = JdbcSourceConnectorConfig.SQL_COMPLEX_TYPES_ENABLE_CONFIG;
    props[1] = "true";
    System.arraycopy(extraProps, 0, props, 2, extraProps.length);
    return new PostgreSqlDatabaseDialect(sourceConfigWithUrl("jdbc:postgresql://something", props));
  }

  private Schema sourceFieldSchema(
      PostgreSqlDatabaseDialect dialect, int jdbcType, String typeName) {
    ColumnDefinition column = column(jdbcType, typeName);
    SchemaBuilder builder = SchemaBuilder.struct();
    String fieldName = dialect.addFieldToSchema(column, builder);
    return fieldName == null ? null : builder.build().field(fieldName).schema();
  }

  private ColumnDefinition column(int jdbcType, String typeName) {
    return column(jdbcType, typeName, ColumnDefinition.Nullability.NULL);
  }

  private ColumnDefinition column(
      int jdbcType, String typeName, ColumnDefinition.Nullability nullability) {
    return column(jdbcType, typeName, nullability, "col");
  }

  private ColumnDefinition column(int jdbcType, String typeName, String columnName) {
    return column(jdbcType, typeName, ColumnDefinition.Nullability.NULL, columnName);
  }

  private ColumnDefinition column(
      int jdbcType, String typeName, ColumnDefinition.Nullability nullability, String columnName) {
    return new ColumnDefinition(
        new ColumnId(new TableId(null, null, "t"), columnName),
        jdbcType, typeName, Object.class.getName(),
        nullability, ColumnDefinition.Mutability.UNKNOWN,
        0, 0, false, 1, false, false, false, false, false);
  }

  @Test
  public void hstoreMappingIsNotSelectedByDefault() {
    // Half of the hstore gate: the mode must select a representation. The other half is the
    // complex types flag, checked alongside it at every call site, so neither alone maps hstore.
    assertFalse("the default must not select a mapping",
        new PostgreSqlDatabaseDialect(
            sourceConfigWithUrl("jdbc:postgresql://something",
                JdbcSourceConnectorConfig.SQL_COMPLEX_TYPES_ENABLE_CONFIG, "true"))
            .hstoreMappingSelected());

    assertFalse("none must not select a mapping",
        hstoreDialect("true", "none").hstoreMappingSelected());
    for (String mode : new String[]{"map", "json"}) {
      assertTrue(mode + " must select a mapping",
          hstoreDialect("true", mode).hstoreMappingSelected());
    }
  }

  private PostgreSqlDatabaseDialect hstoreDialect(String complexTypes, String mode) {
    return new PostgreSqlDatabaseDialect(sourceConfigWithUrl("jdbc:postgresql://something",
        JdbcSourceConnectorConfig.SQL_COMPLEX_TYPES_ENABLE_CONFIG, complexTypes,
        JdbcSourceConnectorConfig.HSTORE_HANDLING_MODE_CONFIG, mode));
  }

  @Test
  public void hstoreSchemaFollowsHandlingModeAndOptionality() {
    // Shared by the scalar column path (#1661) and the array element path (#1662), so the contract
    // is pinned here rather than in either consumer.
    PostgreSqlDatabaseDialect mapMode = new PostgreSqlDatabaseDialect(sourceConfigWithUrl(
        "jdbc:postgresql://something",
        JdbcSourceConnectorConfig.HSTORE_HANDLING_MODE_CONFIG, "map"));
    Schema optionalMap = mapMode.hstoreSchema(true);
    assertEquals(Type.MAP, optionalMap.type());
    assertEquals(Type.STRING, optionalMap.keySchema().type());
    assertTrue("an hstore value may be NULL", optionalMap.valueSchema().isOptional());
    assertTrue(optionalMap.isOptional());
    assertFalse(mapMode.hstoreSchema(false).isOptional());

    PostgreSqlDatabaseDialect jsonMode = new PostgreSqlDatabaseDialect(sourceConfigWithUrl(
        "jdbc:postgresql://something",
        JdbcSourceConnectorConfig.HSTORE_HANDLING_MODE_CONFIG, "json"));
    assertEquals(Json.optionalSchema(), jsonMode.hstoreSchema(true));
    assertEquals(Json.schema(), jsonMode.hstoreSchema(false));
  }

  @Test
  public void shouldMapStringToStringMapToJsonbOnlyWhenComplexTypesEnabled() {
    SinkRecordField field = new SinkRecordField(stringToStringMap(), "col", false);

    PostgreSqlDatabaseDialect enabled = new PostgreSqlDatabaseDialect(sinkConfigWithUrl(
        "jdbc:postgresql://something", JdbcSinkConfig.SQL_COMPLEX_TYPES_ENABLE, "true"));
    assertEquals("JSONB", enabled.getSqlType(field));

    // Without the flag the generic dialect fails at DDL time rather than inventing a column type,
    // which is what makes forgetting the flag on the sink a loud error.
    PostgreSqlDatabaseDialect disabled =
        new PostgreSqlDatabaseDialect(sinkConfigWithUrl("jdbc:postgresql://something"));
    assertThrows(ConnectException.class, () -> disabled.getSqlType(field));
  }

  @Test
  public void shouldBindStringToStringMapAsJsonbTextOnlyWhenComplexTypesEnabled()
      throws SQLException {
    Schema schema = stringToStringMap();
    Map<String, String> value = new LinkedHashMap<>();
    value.put("env", "prod");
    value.put("absent", null);

    // Serialized and bound as text; the ::jsonb cast from valueTypeCast parses it server-side.
    PreparedStatement statement = mock(PreparedStatement.class);
    new PostgreSqlDatabaseDialect(sinkConfigWithUrl(
        "jdbc:postgresql://something", JdbcSinkConfig.SQL_COMPLEX_TYPES_ENABLE, "true"))
        .bindField(statement, 1, schema, value);
    verify(statement).setString(1, "{\"env\":\"prod\",\"absent\":null}");

    PostgreSqlDatabaseDialect disabled =
        new PostgreSqlDatabaseDialect(sinkConfigWithUrl("jdbc:postgresql://something"));
    assertThrows(ConnectException.class,
        () -> disabled.bindField(mock(PreparedStatement.class), 1, schema, value));
  }

  private Schema stringToStringMap() {
    return SchemaBuilder.map(Schema.STRING_SCHEMA, Schema.OPTIONAL_STRING_SCHEMA)
        .optional().build();
  }

}
