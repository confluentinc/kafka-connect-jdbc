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
import io.confluent.connect.jdbc.sink.TableAlterOrCreateException;
import io.confluent.connect.jdbc.sink.metadata.SinkRecordField;
import io.confluent.connect.jdbc.source.ColumnMapping;
import io.confluent.connect.jdbc.source.JdbcSourceConnectorConfig;
import io.confluent.connect.jdbc.util.ColumnDefinition;
import io.confluent.connect.jdbc.util.HstoreConverter;
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
import static org.mockito.Matchers.startsWith;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import io.confluent.connect.jdbc.data.VariableScaleDecimal;
import org.apache.kafka.connect.data.Struct;
import org.mockito.ArgumentCaptor;
import java.math.BigDecimal;
import java.sql.Array;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.GregorianCalendar;
import java.util.Locale;
import java.util.TimeZone;
import static org.junit.Assert.assertNotEquals;

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
    // exercises hstoreSchema() rather than re-asserting generic MAP/STRING behaviour. The two
    // modes deliberately land in different columns, matching Debezium: a map is hstore, while a
    // Json string is a JSON document that happens to have come from hstore.
    Schema mapMode = sourceFieldSchema(hstoreDialect("true", "map"), Types.OTHER, "hstore");
    assertEquals("hstore", sinkDialect().getSqlType(sinkField(mapMode)));

    PostgreSqlDatabaseDialect jsonDialect = complexTypesDialect(
        JdbcSourceConnectorConfig.HSTORE_HANDLING_MODE_CONFIG, "json");
    Schema jsonMode = sourceFieldSchema(jsonDialect, Types.OTHER, "hstore");
    assertEquals("JSONB", sinkDialect().getSqlType(sinkField(jsonMode)));
  }

  @Test
  public void shouldBindStringMapAsHstoreTextForHstoreColumn() throws SQLException {
    // The value half of MAP -> hstore: the map is serialized and bound as text, which the ::hstore
    // cast then parses server-side. Only the DDL half was covered before.
    Map<String, String> value = new LinkedHashMap<>();
    value.put("env", "prod");
    value.put("absent", null);
    Schema schema = SchemaBuilder.map(Schema.STRING_SCHEMA, Schema.OPTIONAL_STRING_SCHEMA)
        .optional().build();

    PreparedStatement statement = mock(PreparedStatement.class);
    sinkDialect().bindField(statement, 1, schema, value);
    verify(statement).setString(1, "\"env\"=>\"prod\",\"absent\"=>NULL");

    // A null map never reaches maybeBindHstore: bindFieldInternal short-circuits nulls before
    // maybeBindPrimitive, so the generic null path binds it.
    PreparedStatement nullStatement = mock(PreparedStatement.class);
    sinkDialect().bindField(nullStatement, 1, schema, null);
    verify(nullStatement).setObject(1, null);
  }

  @Test
  public void shouldBindJsonStringAsTextForJsonbColumn() throws SQLException {
    // The bind half for json mode: a Json-tagged STRING is not a string-to-string map, so
    // maybeBindHstore declines and it binds as text — the ::jsonb cast parses it server-side.
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
  public void offSearchPathHstoreIsSkippedWhenHandlingModeIsNone() {
    // none means skip, wherever the extension lives. Also holds when the feature flag itself is off.
    assertNull(sourceFieldSchema(
        hstoreDialect("true", "none"), Types.OTHER, "\"ext\".\"hstore\""));
    assertNull(sourceFieldSchema(
        hstoreDialect("false", "map"), Types.OTHER, "\"ext\".\"hstore\""));
  }

  @Test
  public void shouldDropHstoreWhenComplexTypesDisabled() {
    // The default is false, so hstore keeps today's drop-with-WARN behaviour and produces no field.
    PostgreSqlDatabaseDialect disabled =
        new PostgreSqlDatabaseDialect(sourceConfigWithUrl("jdbc:postgresql://something"));
    assertNull(sourceFieldSchema(disabled, Types.OTHER, "hstore"));
  }

  /**
   * The raw text form is what the driver returns for an hstore it could not resolve, so it is valid
   * input and must decode to the same map a resolved column would give.
   */
  @Test
  public void hstoreRawTextShouldDecodeLikeADriverMap() throws Exception {
    ResultSet rawText = hstoreResultSet("\"env\"=>\"prod\", \"absent\"=>NULL");
    Map<String, String> expected = new LinkedHashMap<>();
    expected.put("env", "prod");
    expected.put("absent", null);

    assertEquals(expected, hstoreConverter(hstoreDialect("true", "map")).convert(rawText));
    assertEquals("{\"env\":\"prod\",\"absent\":null}",
        hstoreConverter(jsonHstoreDialect()).convert(hstoreResultSet(
            "\"env\"=>\"prod\", \"absent\"=>NULL")));
  }

  /**
   * Anything that is neither a map nor parseable hstore text follows Debezium's handleUnknownData:
   * a nullable column degrades to null, a NOT NULL column fails because null would breach its
   * schema anyway.
   */
  @Test
  public void hstoreValueThatIsNotParseableShouldFollowColumnNullability() throws Exception {
    ResultSet garbage = hstoreResultSet("not hstore at all");

    PostgreSqlDatabaseDialect jsonDialect = complexTypesDialect(
        JdbcSourceConnectorConfig.HSTORE_HANDLING_MODE_CONFIG, "json");

    for (PostgreSqlDatabaseDialect dialect : Arrays.asList(hstoreDialect("true", "map"), jsonDialect)) {
      assertNull(hstoreConverter(dialect, ColumnDefinition.Nullability.NULL).convert(garbage));

      DataException e = assertThrows(DataException.class, () ->
          hstoreConverter(dialect, ColumnDefinition.Nullability.NOT_NULL).convert(garbage));
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
    // Only MAP<STRING,STRING> — the shape hstore produces — maps to hstore. Every other map shape
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

  /**
   * An hstore installed outside the search_path is reported schema-qualified, and is mapped exactly
   * as a bare one — the extension's location is not the operator's problem. Any schema qualifies.
   */
  @Test
  public void offSearchPathHstoreIsMappedInEveryMode() {
    for (String qualified : new String[]{"\"ext\".\"hstore\"", "\"my_extensions\".\"hstore\""}) {
      assertEquals(hstoreDialect("true", "map").hstoreSchema(true),
          sourceFieldSchema(hstoreDialect("true", "map"), Types.OTHER, qualified));
      assertEquals(Json.optionalSchema(),
          sourceFieldSchema(hstoreDialect("true", "json"), Types.OTHER, qualified));
    }
  }

  @Test
  public void shouldNotTreatNonHstoreOtherTypesAsHstore() {
    // Another Types.OTHER type must be neither captured by the hstore branch nor mistaken for an
    // off-search_path hstore, which would fail the task.
    PostgreSqlDatabaseDialect dialect = hstoreDialect("true", "map");
    assertNull(sourceFieldSchema(dialect, Types.OTHER, "citext"));
    assertNull(sourceFieldSchema(dialect, Types.OTHER, "hstore_extra"));
    assertNull(sourceFieldSchema(dialect, Types.OTHER, "\"ext\".\"citext\""));
  }

  @Test
  public void shouldMatchHstoreTypeNameCaseInsensitively() {
    // The driver's reported type name casing must not decide whether the feature works.
    assertEquals(Type.MAP,
        sourceFieldSchema(hstoreDialect("true", "map"), Types.OTHER, "HSTORE").type());
  }

  @Test
  public void jsonColumnMapsToLogicalJsonStringSchema() {
    // json/jsonb map to a logical JSON STRING tagged with the Json logical name, and optionality
    // follows the column: a nullable column needs an optional schema or a NULL value breaches it.
    // Schema equality covers the type, the logical name and the optional flag together.
    assertEquals(Json.optionalSchema(), jsonColumnSchema(ColumnDefinition.Nullability.NULL));
    assertEquals(Json.schema(), jsonColumnSchema(ColumnDefinition.Nullability.NOT_NULL));
  }

  private Schema jsonColumnSchema(ColumnDefinition.Nullability nullability) {
    SchemaBuilder builder = SchemaBuilder.struct();
    String fieldName = complexTypesDialect()
        .addFieldToSchema(column(Types.OTHER, "jsonb", nullability), builder);
    return builder.build().field(fieldName).schema();
  }

  @Test
  public void jsonColumnValueShouldStayRawJsonText() throws Exception {
    // The value is the document's raw text (lossless), not a re-serialized/projected form.
    ColumnDefinition column = column(Types.OTHER, "jsonb");
    DatabaseDialect.ColumnConverter converter = complexTypesDialect().columnConverterFor(
        new ColumnMapping(column, 1, new Field("col", 0, Json.optionalSchema())),
        column, 1, true);
    ResultSet resultSet = mock(ResultSet.class);
    when(resultSet.getString(1)).thenReturn("{\"b\":2,\"a\":[1,null]}");

    assertEquals("{\"b\":2,\"a\":[1,null]}", converter.convert(resultSet));
  }

  @Test
  public void shouldMapComplexTypesToSqlTypes() {
    // The sink half of this PR's contract: the source now tags json/jsonb columns as Json, so a
    // round trip only lands back in jsonb if the sink maps that tag. The mapping itself is #1651's
    // and is covered there for both flag arms; this pins the half the schema change depends on.
    assertEquals("JSONB",
        sinkDialect().getSqlType(new SinkRecordField(Json.schema(), "col", false)));
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

  @Test
  public void shouldRecogniseAnHstoreTypeInAnySchema() {
    // pgjdbc reports the type bare while its schema is on the search_path and qualified otherwise;
    // both are the same type, so both must be recognised, whatever the schema is called.
    PostgreSqlDatabaseDialect dialect = hstoreDialect("true", "map");
    assertTrue(dialect.isHstoreType(column(Types.OTHER, "hstore")));
    assertTrue(dialect.isHstoreType(column(Types.OTHER, "\"ext\".\"hstore\"")));
    assertTrue(dialect.isHstoreType(column(Types.OTHER, "\"my_extensions\".\"hstore\"")));
    assertFalse(dialect.isHstoreType(column(Types.OTHER, "\"ext\".\"citext\"")));
    assertFalse(dialect.isHstoreType(column(Types.OTHER, "hstore_extra")));

    // The array type is its own pg_type row in the same schema, so it qualifies the same way.
    assertEquals("_hstore", PostgreSqlDatabaseDialect.localTypeName("\"ext\".\"_hstore\""));
    assertEquals("_hstore", PostgreSqlDatabaseDialect.localTypeName("_hstore"));
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
  public void shouldMapStringToStringMapToHstoreOnlyWhenComplexTypesEnabled() {
    SinkRecordField field = new SinkRecordField(stringToStringMap(), "col", false);

    PostgreSqlDatabaseDialect enabled = new PostgreSqlDatabaseDialect(sinkConfigWithUrl(
        "jdbc:postgresql://something", JdbcSinkConfig.SQL_COMPLEX_TYPES_ENABLE, "true"));
    assertEquals("hstore", enabled.getSqlType(field));

    // Without the flag the generic dialect fails at DDL time rather than inventing a column type,
    // which is what makes forgetting the flag on the sink a loud error.
    PostgreSqlDatabaseDialect disabled =
        new PostgreSqlDatabaseDialect(sinkConfigWithUrl("jdbc:postgresql://something"));
    assertThrows(ConnectException.class, () -> disabled.getSqlType(field));
  }

  @Test
  public void shouldBindStringToStringMapAsHstoreTextOnlyWhenComplexTypesEnabled()
      throws SQLException {
    Schema schema = stringToStringMap();
    Map<String, String> value = new LinkedHashMap<>();
    value.put("env", "prod");
    value.put("absent", null);

    // Serialized and bound as text; the ::hstore cast from valueTypeCast parses it server-side.
    PreparedStatement statement = mock(PreparedStatement.class);
    new PostgreSqlDatabaseDialect(sinkConfigWithUrl(
        "jdbc:postgresql://something", JdbcSinkConfig.SQL_COMPLEX_TYPES_ENABLE, "true"))
        .bindField(statement, 1, schema, value);
    verify(statement).setString(1, "\"env\"=>\"prod\",\"absent\"=>NULL");

    PostgreSqlDatabaseDialect disabled =
        new PostgreSqlDatabaseDialect(sinkConfigWithUrl("jdbc:postgresql://something"));
    assertThrows(ConnectException.class,
        () -> disabled.bindField(mock(PreparedStatement.class), 1, schema, value));
  }

  /**
   * The hstore text form the bind produces. Everything is quoted, so a delimiter inside a key or
   * value is inert and the string {@code NULL} stays distinct from a NULL value.
   */
  @Test
  public void shouldSerializeMapsToHstoreText() {
    Map<String, String> map = new LinkedHashMap<>();
    map.put("env", "prod");
    map.put("absent", null);
    map.put("literal", "NULL");
    map.put("a=>b", "c,d");
    map.put("say \"hi\"", "back\\slash");

    assertEquals("\"env\"=>\"prod\",\"absent\"=>NULL,\"literal\"=>\"NULL\","
            + "\"a=>b\"=>\"c,d\",\"say \\\"hi\\\"\"=>\"back\\\\slash\"",
        HstoreConverter.connectMapToHstore(map));

    assertEquals("", HstoreConverter.connectMapToHstore(Collections.emptyMap()));
    assertNull(HstoreConverter.connectMapToHstore(null));
  }

  /**
   * The text form the driver hands back for an hstore it could not resolve. Quoting makes the
   * delimiters inert, and a bare NULL is a null value while a quoted one is the string.
   */
  @Test
  public void shouldParseHstoreText() {
    Map<String, String> expected = new LinkedHashMap<>();
    expected.put("env", "prod");
    expected.put("absent", null);
    expected.put("literal", "NULL");
    expected.put("a=>b", "c,d");
    expected.put("say \"hi\"", "back\\slash");

    // PostgreSQL renders pairs separated by ", " and escapes quotes and backslashes.
    assertEquals(expected, HstoreConverter.hstoreToConnectMap(
        "\"env\"=>\"prod\", \"absent\"=>NULL, \"literal\"=>\"NULL\", "
            + "\"a=>b\"=>\"c,d\", \"say \\\"hi\\\"\"=>\"back\\\\slash\""));

    assertEquals(Collections.emptyMap(), HstoreConverter.hstoreToConnectMap(""));
    assertEquals(Collections.emptyMap(), HstoreConverter.hstoreToConnectMap("   "));
    assertNull(HstoreConverter.hstoreToConnectMap(null));
  }

  /** Whatever the serializer writes, the parser must read back identically. */
  @Test
  public void shouldRoundTripHstoreTextThroughBothDirections() {
    Map<String, String> original = new LinkedHashMap<>();
    original.put("env", "prod");
    original.put("absent", null);
    original.put("literal", "NULL");
    original.put("a=>b", "c,d");
    original.put("say \"hi\"", "back\\slash");
    original.put("key 2", " ##123 78");
    original.put("empty", "");

    assertEquals(original, HstoreConverter.hstoreToConnectMap(
        HstoreConverter.connectMapToHstore(original)));
    assertEquals(Collections.emptyMap(), HstoreConverter.hstoreToConnectMap(
        HstoreConverter.connectMapToHstore(Collections.emptyMap())));
  }

  @Test
  public void shouldRejectMalformedHstoreText() {
    for (String malformed : new String[]{
        "not hstore at all",          // no => at all
        "\"k\"",                      // key with no value
        "\"k\"=>",                    // separator with no value
        "\"k\"=>\"unterminated",      // unclosed quote
        "\"a\"=>\"1\" \"b\"=>\"2\"",     // missing comma between pairs
        "a=>1",                       // unquoted: hstore_out always quotes, so this is not ours
        "\"a\"=>1",                   // unquoted value, likewise
        "\"a\"=>null"                 // lowercase null is the string, never the NULL literal
    }) {
      assertThrows("should reject: " + malformed, DataException.class,
          () -> HstoreConverter.hstoreToConnectMap(malformed));
    }
  }

  @Test
  public void shouldRejectValuesThatAreNotStringMaps() {
    assertThrows(DataException.class, () -> HstoreConverter.connectMapToHstore("not a map"));
    assertThrows(DataException.class,
        () -> HstoreConverter.connectMapToHstore(Collections.singletonMap(null, "v")));
    // A non-String key or value fails rather than being coerced through toString, so a schema and
    // value that disagree surface here instead of silently reaching the database.
    assertThrows(DataException.class,
        () -> HstoreConverter.connectMapToHstore(Collections.singletonMap("k", 1)));
    assertThrows(DataException.class,
        () -> HstoreConverter.connectMapToHstore(Collections.singletonMap(1, "v")));
  }

  /**
   * An extension installed outside the search_path must still be usable: the DDL and the array
   * element type take the schema-qualified name resolved from the connection.
   */
  @Test
  public void shouldUseTheResolvedHstoreTypeNameForDdl() {
    PostgreSqlDatabaseDialect sink = new PostgreSqlDatabaseDialect(sinkConfigWithUrl(
        "jdbc:postgresql://something", JdbcSinkConfig.SQL_COMPLEX_TYPES_ENABLE, "true"));
    SinkRecordField field = new SinkRecordField(stringToStringMap(), "col", false);

    sink.hstoreTypeName = "\"ext\".hstore";
    sink.hstoreTypeResolved = true;
    assertEquals("\"ext\".hstore", sink.getSqlType(field));
    assertEquals("\"ext\".hstore[]",
        sink.getSqlType(new SinkRecordField(arraySchema(stringToStringMap()), "col", false)));
  }

  /**
   * A schema name is user-chosen and may legally contain a double quote, which has to be doubled
   * for the qualified type name to parse. Without escaping, a schema named {@code my"schema}
   * produces {@code "my"schema".hstore}, which terminates the identifier early.
   */
  @Test
  public void shouldEscapeQuotesInTheResolvedHstoreSchemaName() throws Exception {
    ResultSet searchPath = mock(ResultSet.class);
    when(searchPath.next()).thenReturn(true);
    when(searchPath.getBoolean(1)).thenReturn(false);

    ResultSet extension = mock(ResultSet.class);
    when(extension.next()).thenReturn(true);
    when(extension.getString(1)).thenReturn("my\"schema");

    Statement statement = mock(Statement.class);
    when(statement.executeQuery("SELECT to_regtype('hstore') IS NOT NULL")).thenReturn(searchPath);
    when(statement.executeQuery(startsWith("SELECT n.nspname"))).thenReturn(extension);

    Connection connection = mock(Connection.class);
    when(connection.createStatement()).thenReturn(statement);

    assertEquals("\"my\"\"schema\".hstore",
        PostgreSqlDatabaseDialect.resolveHstoreTypeName(connection));
  }

  /**
   * A catalog read that fails is attempted once, not once per connection: a persistent failure
   * would otherwise cost a round-trip and a WARN on every batch. It still leaves the type
   * unresolved, so the bare name is assumed rather than the extension reported as missing.
   */
  @Test
  public void shouldRetryTheHstoreLookupAfterACatalogFailure() throws Exception {
    ResultSet resolved = mock(ResultSet.class);
    when(resolved.next()).thenReturn(true);
    when(resolved.getBoolean(1)).thenReturn(true);

    Statement statement = mock(Statement.class);
    when(statement.executeQuery("SELECT to_regtype('hstore') IS NOT NULL"))
        .thenThrow(new SQLException("permission denied for table pg_extension"))
        .thenReturn(resolved);
    Connection connection = mock(Connection.class);
    when(connection.createStatement()).thenReturn(statement);

    PostgreSqlDatabaseDialect sink = new PostgreSqlDatabaseDialect(sinkConfigWithUrl(
        "jdbc:postgresql://something", JdbcSinkConfig.SQL_COMPLEX_TYPES_ENABLE, "true"));

    sink.maybeResolveHstoreType(connection);
    assertFalse("a failed read is not a resolution", sink.hstoreTypeResolved);
    assertEquals("the bare name is assumed while unresolved", "hstore",
        sink.getSqlType(new SinkRecordField(stringToStringMap(), "tags", false)));

    // The next connection retries, so a transient failure heals.
    sink.maybeResolveHstoreType(connection);
    assertTrue("the retry should resolve", sink.hstoreTypeResolved);
    verify(statement, times(2)).executeQuery("SELECT to_regtype('hstore') IS NOT NULL");
  }

  /**
   * An unparseable array element degrades to null, as the scalar path does for an optional column,
   * rather than failing the whole source task on one bad element.
   */
  @Test
  public void shouldEmitNullForAnUnparseableHstoreArrayElement() throws Exception {
    PostgreSqlDatabaseDialect dialect = complexTypesDialect(
        JdbcSourceConnectorConfig.HSTORE_HANDLING_MODE_CONFIG, "map");
    ColumnDefinition column = column(Types.ARRAY, "_hstore");
    ColumnMapping mapping = new ColumnMapping(
        column, 1, new Field("col", 0, arraySchema(stringToStringMap())));
    DatabaseDialect.ColumnConverter converter =
        dialect.columnConverterFor(mapping, column, 1, true);
    assertNotNull(converter);

    ResultSet resultSet = mock(ResultSet.class);
    Array array = mock(Array.class);
    ResultSet elementRs = mock(ResultSet.class);
    when(resultSet.getArray(1)).thenReturn(array);
    when(array.getArray()).thenReturn(new Object[]{"not hstore at all"});
    when(array.getResultSet()).thenReturn(elementRs);
    when(elementRs.next()).thenReturn(true, false);
    when(elementRs.getObject(2)).thenReturn("not hstore at all");

    assertEquals(Collections.singletonList(null), converter.convert(resultSet));
  }

  /**
   * A missing extension surfaces from {@code getSqlType} while the table is being built, so it
   * keeps {@link TableAlterOrCreateException}: the writer rolls back and unrolls, which can still
   * place the records that do not need the column.
   */
  @Test
  public void shouldRaiseAMissingHstoreExtensionAsTableAlterOrCreate() {
    PostgreSqlDatabaseDialect sink = new PostgreSqlDatabaseDialect(sinkConfigWithUrl(
        "jdbc:postgresql://something", JdbcSinkConfig.SQL_COMPLEX_TYPES_ENABLE, "true"));
    sink.hstoreTypeName = null;
    sink.hstoreTypeResolved = true;
    assertThrows("a missing extension must roll back and be reportable",
        TableAlterOrCreateException.class,
        () -> sink.getSqlType(new SinkRecordField(stringToStringMap(), "tags", false)));
  }

  /**
   * A wrong column type is refused at bind time, where the mismatch holds for every record, so it
   * fails the task as any unbindable value does instead of inheriting the DDL unroll. Asserted on
   * the exact class: {@link TableAlterOrCreateException} would also satisfy a ConnectException
   * check, leaving a regression to the DDL type invisible.
   */
  @Test
  public void shouldRaiseAWrongHstoreColumnTypeAsConnectException() {
    TableDefinitionBuilder builder = new TableDefinitionBuilder().withTable("t");
    builder.withColumn("tags").type("jsonb", JDBCType.OTHER, Object.class);
    TableDefinition tableDefn = builder.build();
    ConnectException e = assertThrows(ConnectException.class,
        () -> sinkDialect().bindField(mock(PreparedStatement.class), 1, stringToStringMap(),
            Collections.singletonMap("k", "v"),
            tableDefn.definitionForColumn("tags"), "tags"));
    assertEquals("a bind refusal must not carry the DDL exception",
        ConnectException.class, e.getClass());
  }

  /** Selected but unavailable must fail, naming the field and how to install the extension. */
  @Test
  public void shouldFailWhenTheHstoreExtensionIsNotInstalled() {
    PostgreSqlDatabaseDialect sink = new PostgreSqlDatabaseDialect(sinkConfigWithUrl(
        "jdbc:postgresql://something", JdbcSinkConfig.SQL_COMPLEX_TYPES_ENABLE, "true"));
    sink.hstoreTypeName = null;
    sink.hstoreTypeResolved = true;

    ConnectException thrown = assertThrows(ConnectException.class, () -> sink.getSqlType(
        new SinkRecordField(stringToStringMap(), "tags", false)));
    assertTrue("message should name the field, but was: " + thrown.getMessage(),
        thrown.getMessage().contains("tags"));
    assertTrue("message should say how to install it, but was: " + thrown.getMessage(),
        thrown.getMessage().contains("CREATE EXTENSION hstore"));

    // An array element fails the same way, and must name its column rather than "a map value".
    ConnectException fromArray = assertThrows(ConnectException.class,
        () -> sink.bindField(mock(PreparedStatement.class), 1,
            arraySchema(stringToStringMap()), Collections.singletonList(
                Collections.singletonMap("env", "prod")),
            mock(ColumnDefinition.class), "tags"));
    assertTrue("array message should name the field, but was: " + fromArray.getMessage(),
        fromArray.getMessage().contains("tags"));
  }

  /**
   * A map is written as hstore text, so an existing column of any other type must be refused before
   * the bind. Otherwise the statement carries that column's cast and PostgreSQL reports a syntax
   * error that never mentions hstore.
   */
  @Test
  public void shouldRefuseAMapBoundIntoANonHstoreColumn() {
    PostgreSqlDatabaseDialect sink = complexTypesSinkDialect();

    ConnectException thrown = assertThrows(ConnectException.class,
        () -> sink.bindField(mock(PreparedStatement.class), 1, stringToStringMap(),
            Collections.singletonMap("env", "prod"), columnOfType("jsonb"), "tags"));
    assertEquals("Cannot write field tags as hstore: column type is jsonb, not hstore. "
            + "Recreate the column as hstore, or set sql.complex.types.enable=false.",
        thrown.getMessage());

    // An hstore column is accepted, wherever the extension lives.
    for (String hstore : new String[]{"hstore", "\"ext\".\"hstore\""}) {
      sinkBindMap(sink, hstore);
    }
  }

  /**
   * The array branch must name {@code hstore[]}, not pgjdbc's internal {@code _hstore}. This
   * dialect writes maps to native hstore where an earlier build wrote jsonb, so an upgrade over an
   * existing {@code jsonb[]} column lands here — and a remediation reading "recreate the column as
   * hstore" would produce a scalar that fails the very next batch.
   */
  @Test
  public void shouldNameTheArrayFormWhenRefusingAnHstoreArrayColumn() {
    PostgreSqlDatabaseDialect sink = complexTypesSinkDialect();

    ConnectException thrown = assertThrows(ConnectException.class,
        () -> sink.bindField(mock(PreparedStatement.class), 1, arraySchema(stringToStringMap()),
            Collections.singletonList(Collections.singletonMap("env", "prod")),
            columnOfType("_jsonb"), "tags"));
    assertEquals("Cannot write field tags as hstore: column type is jsonb[], not hstore[]. "
            + "Recreate the column as hstore[], or set sql.complex.types.enable=false.",
        thrown.getMessage());

    // An hstore[] column is accepted, wherever the extension lives.
    for (String hstoreArray : new String[]{"_hstore", "\"ext\".\"_hstore\""}) {
      sinkBindMapArray(sink, hstoreArray);
    }
  }

  /** A non-array column reported without the prefix is named as-is, with no {@code []} appended. */
  @Test
  public void shouldNameANonArrayColumnWithoutAnArraySuffix() {
    PostgreSqlDatabaseDialect sink = complexTypesSinkDialect();

    ConnectException thrown = assertThrows(ConnectException.class,
        () -> sink.bindField(mock(PreparedStatement.class), 1, stringToStringMap(),
            Collections.singletonMap("env", "prod"), columnOfType("\"ext\".\"text\""), "tags"));
    assertFalse("a scalar column must not be reported as an array: " + thrown.getMessage(),
        thrown.getMessage().contains("[]"));
  }

  private void sinkBindMap(PostgreSqlDatabaseDialect sink, String columnType) {
    try {
      sink.bindField(mock(PreparedStatement.class), 1, stringToStringMap(),
          Collections.singletonMap("env", "prod"), columnOfType(columnType), "tags");
    } catch (SQLException e) {
      throw new AssertionError("binding into a " + columnType + " column should succeed", e);
    }
  }

  private void sinkBindMapArray(PostgreSqlDatabaseDialect sink, String columnType) {
    try {
      PreparedStatement statement = mock(PreparedStatement.class);
      Connection connection = mock(Connection.class);
      when(statement.getConnection()).thenReturn(connection);
      when(connection.createArrayOf(any(), any())).thenReturn(mock(Array.class));
      sink.bindField(statement, 1, arraySchema(stringToStringMap()),
          Collections.singletonList(Collections.singletonMap("env", "prod")),
          columnOfType(columnType), "tags");
    } catch (SQLException e) {
      throw new AssertionError("binding into a " + columnType + " column should succeed", e);
    }
  }

  private ColumnDefinition columnOfType(String typeName) {
    ColumnDefinition colDef = mock(ColumnDefinition.class);
    when(colDef.typeName()).thenReturn(typeName);
    return colDef;
  }

  /**
   * Unresolved is not the same as absent. Before a connection exists, or after a catalog read
   * failed, the bare type name is assumed rather than reporting the extension as missing — which
   * would name the wrong cause and fail a write that PostgreSQL might well accept.
   */
  @Test
  public void shouldAssumeTheBareHstoreTypeNameWhileUnresolved() {
    PostgreSqlDatabaseDialect sink = new PostgreSqlDatabaseDialect(sinkConfigWithUrl(
        "jdbc:postgresql://something", JdbcSinkConfig.SQL_COMPLEX_TYPES_ENABLE, "true"));
    assertFalse("precondition: nothing has been resolved yet", sink.hstoreTypeResolved);

    assertEquals("hstore",
        sink.getSqlType(new SinkRecordField(stringToStringMap(), "tags", false)));
  }

  /** An hstore column off the search_path is reported qualified and must be cast by that name. */
  @Test
  public void shouldCastHstoreColumnsByTheirReportedTypeName() {
    TableDefinitionBuilder builder = new TableDefinitionBuilder().withTable("myTable");
    builder.withColumn("plain").type("hstore", JDBCType.OTHER, Object.class);
    builder.withColumn("qualified").type("\"ext\".\"hstore\"", JDBCType.OTHER, Object.class);
    TableDefinition tableDefn = builder.build();

    assertEquals("::hstore",
        dialect.valueTypeCast(tableDefn, tableDefn.definitionForColumn("plain").id()));
    assertEquals("::\"ext\".\"hstore\"",
        dialect.valueTypeCast(tableDefn, tableDefn.definitionForColumn("qualified").id()));
  }

  /** pgjdbc leaves embedded quotes unescaped, so the cast must requote the reported name. */
  @Test
  public void shouldRequoteAnHstoreTypeNameTheDriverLeftUnescaped() {
    TableDefinitionBuilder builder = new TableDefinitionBuilder().withTable("myTable");
    builder.withColumn("hs").type("\"e\"x\".\"hstore\"", JDBCType.OTHER, Object.class);
    TableDefinition tableDefn = builder.build();

    assertEquals("::\"e\"\"x\".\"hstore\"",
        dialect.valueTypeCast(tableDefn, tableDefn.definitionForColumn("hs").id()));
  }

  private Schema stringToStringMap() {
    return SchemaBuilder.map(Schema.STRING_SCHEMA, Schema.OPTIONAL_STRING_SCHEMA)
        .optional().build();
  }


  @Test
  public void shouldMapSupportedArrayElementTypesToSourceSchema() {
    assertArrayElement("_text", Type.STRING, null);
    assertArrayElement("_varchar", Type.STRING, null);
    assertArrayElement("_bpchar", Type.STRING, null);
    assertArrayElement("_int2", Type.INT16, null);
    assertArrayElement("_int4", Type.INT32, null);
    assertArrayElement("_int8", Type.INT64, null);
    assertArrayElement("_float4", Type.FLOAT32, null);
    assertArrayElement("_float8", Type.FLOAT64, null);
    assertArrayElement("_bool", Type.BOOLEAN, null);
    assertArrayElement("_numeric", Type.STRUCT, VariableScaleDecimal.LOGICAL_NAME);
    assertArrayElement("_json", Type.STRING, Json.LOGICAL_NAME);
    assertArrayElement("_jsonb", Type.STRING, Json.LOGICAL_NAME);
    assertArrayElement("_date", Type.INT32, Date.LOGICAL_NAME);
    assertArrayElement("_time", Type.INT32, Time.LOGICAL_NAME);
    assertArrayElement("_timestamp", Type.INT64, Timestamp.LOGICAL_NAME);
    // timestamptz drops the zone and maps to the same timestamp schema as scalar timestamptz.
    assertArrayElement("_timestamptz", Type.INT64, Timestamp.LOGICAL_NAME);
  }

  @Test
  public void timestampArraysHonorTimestampGranularity() {
    PostgreSqlDatabaseDialect microsString = complexTypesDialect(
        JdbcSourceConnectorConfig.TIMESTAMP_GRANULARITY_CONFIG, "micros_string");
    Schema tsElement = sourceFieldSchema(microsString, Types.ARRAY, "_timestamp").valueSchema();
    Schema tsTzElement = sourceFieldSchema(microsString, Types.ARRAY, "_timestamptz").valueSchema();
    assertEquals(Type.STRING, tsElement.type());
    assertNull(tsElement.name());
    assertEquals(Type.STRING, tsTzElement.type());
    assertNull(tsTzElement.name());
  }

  @Test
  public void shouldSkipUnsupportedArrayElementTypes() {
    // uuid[]/inet[]/money[] are not in the supported element set, so the column is skipped.
    assertNull(sourceFieldSchema(complexTypesDialect(), Types.ARRAY, "_uuid"));
    assertNull(sourceFieldSchema(complexTypesDialect(), Types.ARRAY, "_inet"));
    assertNull(sourceFieldSchema(complexTypesDialect(), Types.ARRAY, "_money"));
  }

  @Test
  public void shouldDropArraysWhenComplexTypesDisabled() {
    // The base 'dialect' has the feature off; arrays are skipped, as before the change.
    assertNull(sourceFieldSchema(dialect, Types.ARRAY, "_int4"));
  }

  @Test
  public void shouldBuildOptionalArrayFieldEvenForNotNullColumn() {
    // A multi-dimensional array reports the same type name as 1-D and is skipped (null) at read
    // time; array fields are therefore always optional so that null is accepted rather than
    // failing a required field, even when the source column is NOT NULL.
    ColumnDefinition notNull = column(Types.ARRAY, "_int4", ColumnDefinition.Nullability.NOT_NULL);
    SchemaBuilder builder = SchemaBuilder.struct();
    String fieldName = complexTypesDialect().addFieldToSchema(notNull, builder);
    Schema fieldSchema = builder.build().field(fieldName).schema();
    assertEquals(Type.ARRAY, fieldSchema.type());
    assertTrue("array field must be optional even for a NOT NULL column", fieldSchema.isOptional());
  }

  @Test
  public void shouldAcceptNullValueForNotNullArrayColumnField() {
    // The multi-dim skip returns null; the (optional) field must accept it without throwing,
    // which is what keeps the task alive instead of crashing on the record.
    ColumnDefinition notNull = column(Types.ARRAY, "_int4", ColumnDefinition.Nullability.NOT_NULL);
    SchemaBuilder builder = SchemaBuilder.struct();
    String fieldName = complexTypesDialect().addFieldToSchema(notNull, builder);
    Schema schema = builder.build();
    new Struct(schema).put(fieldName, null); // no exception => skipped multi-dim value is tolerated
  }

  @Test
  public void gatedLogicalTypesFallBackWhenComplexTypesDisabled() {
    PostgreSqlDatabaseDialect disabled =
        new PostgreSqlDatabaseDialect(sinkConfigWithUrl("jdbc:postgresql://something"));

    assertEquals("TEXT", disabled.getSqlType(
        new SinkRecordField(Json.optionalSchema(), "col", false)));
    assertEquals("TEXT[]", disabled.getSqlType(
        new SinkRecordField(arraySchema(Json.optionalSchema()), "col", false)));
    assertThrows(ConnectException.class, () -> disabled.getSqlType(
        new SinkRecordField(VariableScaleDecimal.optionalSchema(), "col", false)));
  }

  private PostgreSqlDatabaseDialect complexTypesSinkDialect() {
    return new PostgreSqlDatabaseDialect(sinkConfigWithUrl(
        "jdbc:postgresql://something", JdbcSinkConfig.SQL_COMPLEX_TYPES_ENABLE, "true"));
  }

  @Test
  public void shouldBindJsonArrayAsNativeJsonbArray() throws Exception {
    verifyArrayBind(
        Json.optionalSchema(),
        Arrays.asList("{\"k\":\"v\"}", "{\"a\":1}"),
        "jsonb",
        new Object[]{"{\"k\":\"v\"}", "{\"a\":1}"});
  }

  /**
   * A fixed-scale {@code Decimal} element is what an upstream connector emits for
   * {@code numeric(p,s)[]}. The converter already decoded it to BigDecimal, so only the array type
   * matters, and it must be the unmodified {@code numeric}: {@code createArrayOf} resolves the name
   * against {@code pg_type.typname}, which carries no modifiers.
   */
  @Test
  public void shouldBindDecimalArrayAsNativeNumericArray() throws Exception {
    verifyArrayBind(
        Decimal.builder(2).optional().build(),
        Arrays.asList(new BigDecimal("1.20"), new BigDecimal("3.40")),
        "numeric",
        new Object[]{new BigDecimal("1.20"), new BigDecimal("3.40")});
  }

  @Test
  public void shouldBindNumericArrayAsNativeNumericArray() throws Exception {
    Schema element = VariableScaleDecimal.optionalSchema();
    verifyArrayBind(
        element,
        Arrays.asList(
            VariableScaleDecimal.fromLogical(element, new BigDecimal("1.50")),
            VariableScaleDecimal.fromLogical(element, new BigDecimal("3.14159"))),
        "numeric",
        new Object[]{new BigDecimal("1.50"), new BigDecimal("3.14159")});
  }

  @Test
  public void shouldBindTemporalArraysAsNativeArrays() throws Exception {
    // epoch 0 == 1970-01-01T00:00:00Z, rendered here with the default db.timezone of UTC.
    java.util.Date epoch = new java.util.Date(0L);
    verifyArrayBind(Date.builder().optional().build(),
        Collections.singletonList(epoch), "date", new Object[]{"1970-01-01"});
    verifyArrayBind(Time.builder().optional().build(),
        Collections.singletonList(epoch), "time", new Object[]{"00:00:00.000"});
    verifyArrayBind(Timestamp.builder().optional().build(),
        Collections.singletonList(epoch), "timestamp", new Object[]{"1970-01-01 00:00:00.000"});
  }

  @Test
  public void arrayBindPreservesNullElements() throws Exception {
    Schema numeric = VariableScaleDecimal.optionalSchema();
    verifyArrayBind(numeric,
        Arrays.asList(VariableScaleDecimal.fromLogical(numeric, new BigDecimal("1.50")), null),
        "numeric", new Object[]{new BigDecimal("1.50"), null});
    verifyArrayBind(Json.optionalSchema(),
        Arrays.asList("{\"k\":1}", null), "jsonb", new Object[]{"{\"k\":1}", null});
    verifyArrayBind(Timestamp.builder().optional().build(),
        Arrays.asList(new java.util.Date(0L), null), "timestamp",
        new Object[]{"1970-01-01 00:00:00.000", null});
    verifyArrayBind(Decimal.builder(2).optional().build(),
        Arrays.asList(new BigDecimal("1.20"), null),
        "numeric", new Object[]{new BigDecimal("1.20"), null});
  }

  /**
   * {@code numeric.mapping} narrows a scalar NUMERIC, but array elements always map to
   * VariableScaleDecimal so each keeps its own scale. The asymmetry is deliberate.
   */
  @Test
  public void numericArrayElementsIgnoreNumericMapping() {
    for (String mapping : new String[]{"none", "best_fit", "best_fit_eager_double",
        "precision_only"}) {
      PostgreSqlDatabaseDialect dialect = complexTypesDialect(
          JdbcSourceConnectorConfig.NUMERIC_MAPPING_CONFIG, mapping);
      Schema schema = sourceFieldSchema(dialect, Types.ARRAY, "_numeric");

      assertNotNull("numeric[] should produce a field for numeric.mapping=" + mapping, schema);
      assertEquals("numeric.mapping=" + mapping, Type.ARRAY, schema.type());
      assertEquals("numeric.mapping=" + mapping,
          VariableScaleDecimal.LOGICAL_NAME, schema.valueSchema().name());
    }
  }

  /**
   * Element shapes the bind path cannot write must not be advertised in DDL either, so auto-create
   * never builds a column every insert then fails on. A nested array and a plain BYTES element both
   * recursed to valid DDL before; a {@code Decimal} element is also BYTES-based but is named and
   * binds, so it must still be accepted.
   */
  @Test
  public void shouldNotMapUnbindableArrayElementsToSqlType() {
    PostgreSqlDatabaseDialect sink = complexTypesSinkDialect();
    Schema nested = arraySchema(arraySchema(Schema.OPTIONAL_INT32_SCHEMA));
    Schema bytes = arraySchema(Schema.OPTIONAL_BYTES_SCHEMA);

    assertThrows(ConnectException.class,
        () -> sink.getSqlType(new SinkRecordField(nested, "col", false)));
    assertThrows(ConnectException.class,
        () -> sink.getSqlType(new SinkRecordField(bytes, "col", false)));
    assertEquals("a named BYTES element that does bind is still mapped", "DECIMAL[]",
        sink.getSqlType(
            new SinkRecordField(arraySchema(Decimal.builder(2).optional().build()), "col", false)));
  }

  /**
   * {@code timestamp.fields.list} selects TIMESTAMP by field name, but there is no array binding
   * for it — the element stays a plain INT64/STRING and would be sent as {@code int8[]}/{@code
   * text[]}.
   * Rejecting is better than quietly emitting {@code BIGINT[]}, which would store raw epoch values
   * for someone who explicitly asked for timestamp semantics.
   */
  @Test
  public void shouldNotMapTimestampFieldsListArrayToSqlType() {
    PostgreSqlDatabaseDialect sink = new PostgreSqlDatabaseDialect(sinkConfigWithUrl(
        "jdbc:postgresql://something",
        JdbcSinkConfig.SQL_COMPLEX_TYPES_ENABLE, "true",
        JdbcSinkConfig.TIMESTAMP_FIELDS_LIST, "ts_col"));

    for (Schema element
        : Arrays.asList(Schema.OPTIONAL_INT64_SCHEMA, Schema.OPTIONAL_STRING_SCHEMA)) {
      assertThrows(ConnectException.class, () -> sink.getSqlType(
          new SinkRecordField(arraySchema(element), "ts_col", false)));
      // A field not in the list is unaffected.
      assertNotNull(sink.getSqlType(new SinkRecordField(arraySchema(element), "other", false)));
    }
  }

  /** An empty list binds as an empty array, staying distinguishable from a NULL one. */
  @Test
  public void shouldBindEmptyArrayAsEmptyNativeArray() throws Exception {
    verifyArrayBind(Json.optionalSchema(), Collections.emptyList(), "jsonb", new Object[]{});
  }

  @Test
  public void shouldNotBindStructArray() {
    Schema element = SchemaBuilder.struct().optional().field("a", Schema.INT32_SCHEMA).build();
    Schema schema = SchemaBuilder.array(element).optional().build();
    List<?> value = Collections.singletonList(new Struct(element).put("a", 1));

    assertThrows(ConnectException.class, () -> dialect.bindField(mock(PreparedStatement.class), 1,
        schema, value, mock(ColumnDefinition.class), "field"));
    assertThrows(ConnectException.class, () -> complexTypesDialect().bindField(
        mock(PreparedStatement.class), 1, schema, value, mock(ColumnDefinition.class), "field"));
  }

  @Test
  public void shouldNotMapStructArrayToSqlType() {
    // The DDL side must agree with the bind side above: no column type is advertised for an
    // ARRAY<STRUCT>, so auto-create fails rather than creating a column that cannot be written.
    Schema schema = arraySchema(
        SchemaBuilder.struct().optional().field("a", Schema.INT32_SCHEMA).build());
    SinkRecordField field = new SinkRecordField(schema, "col", false);
    // A sink config with the feature on is the only case that could have produced JSONB[].
    PostgreSqlDatabaseDialect sink = new PostgreSqlDatabaseDialect(sinkConfigWithUrl(
        "jdbc:postgresql://something", JdbcSinkConfig.SQL_COMPLEX_TYPES_ENABLE, "true"));

    assertThrows(ConnectException.class, () -> dialect.getSqlType(field));
    assertThrows(ConnectException.class, () -> sink.getSqlType(field));
  }

  /**
   * DDL and DML must agree for every logical element type: {@code getSqlType} and
   * {@code arrayElementBinding} are separate switches, so a type in one but not the other yields a
   * column auto-create builds and no insert can write. New logical types belong in this map too.
   */
  @Test
  public void arrayDdlAndBindAgreeForEveryLogicalElementType() throws Exception {
    Schema variableScaleDecimal = VariableScaleDecimal.optionalSchema();
    Map<Schema, Object> sampleByElementSchema = new LinkedHashMap<>();
    sampleByElementSchema.put(Decimal.builder(2).optional().build(), new BigDecimal("1.20"));
    sampleByElementSchema.put(Date.builder().optional().build(), new java.util.Date(0L));
    sampleByElementSchema.put(Time.builder().optional().build(), new java.util.Date(0L));
    sampleByElementSchema.put(Timestamp.builder().optional().build(), new java.util.Date(0L));
    sampleByElementSchema.put(Json.optionalSchema(), "{\"k\":1}");
    sampleByElementSchema.put(variableScaleDecimal,
        VariableScaleDecimal.fromLogical(variableScaleDecimal, new BigDecimal("1.50")));

    PostgreSqlDatabaseDialect sink = complexTypesSinkDialect();
    for (Map.Entry<Schema, Object> entry : sampleByElementSchema.entrySet()) {
      Schema elementSchema = entry.getKey();
      Schema schema = SchemaBuilder.array(elementSchema).optional().build();
      String element = elementSchema.name();

      assertNotNull("No array column type advertised for element " + element,
          sink.getSqlType(new SinkRecordField(schema, "col", false)));

      PreparedStatement statement = mock(PreparedStatement.class);
      Connection connection = mock(Connection.class);
      ColumnDefinition colDef = mock(ColumnDefinition.class);
      when(colDef.type()).thenReturn(Types.ARRAY);
      when(statement.getConnection()).thenReturn(connection);
      when(connection.createArrayOf(any(String.class), any())).thenReturn(mock(Array.class));

      sink.bindField(statement, 1, schema, Collections.singletonList(entry.getValue()), colDef,
          "col");

      verify(connection).createArrayOf(any(String.class), any());
    }
  }

  @Test
  public void shouldBindPrimitiveArrayWhenComplexTypesDisabled() throws Exception {
    PreparedStatement statement = mock(PreparedStatement.class);
    Schema schema = SchemaBuilder.array(Schema.OPTIONAL_INT32_SCHEMA).optional().build();

    dialect.bindField(statement, 1, schema, Arrays.asList(1, 2),
        mock(ColumnDefinition.class), "field");

    verify(statement).setObject(eq(1), any(Integer[].class), eq(Types.ARRAY));
  }

  /**
   * With the flag off a gated element degrades according to its underlying Connect type rather than
   * failing uniformly. {@code Json} is STRING-based, so it falls to the primitive path and binds as
   * {@code text[]} with the documents intact; {@code VariableScaleDecimal} is a STRUCT, has no
   * primitive equivalent, and is rejected. Both halves matter: the first is a lossless degrade
   * for a partially upgraded pipeline, the second a hard failure.
   */
  @Test
  public void gatedArrayElementsDegradeByUnderlyingTypeWhenComplexTypesDisabled() throws Exception {
    PreparedStatement statement = mock(PreparedStatement.class);
    Schema jsonArray = SchemaBuilder.array(Json.optionalSchema()).optional().build();

    dialect.bindField(statement, 1, jsonArray, Arrays.asList("{\"a\":1}", "{\"b\":2}"),
        mock(ColumnDefinition.class), "field");
    verify(statement).setObject(eq(1), any(String[].class), eq(Types.ARRAY));

    Schema numericArray = SchemaBuilder.array(VariableScaleDecimal.optionalSchema())
        .optional().build();
    assertThrows(ConnectException.class, () -> dialect.bindField(
        mock(PreparedStatement.class), 1, numericArray,
        Collections.singletonList(VariableScaleDecimal.fromLogical(
            VariableScaleDecimal.optionalSchema(), new BigDecimal("1.5"))),
        mock(ColumnDefinition.class), "field"));
  }

  @Test
  public void temporalArrayBindHonorsConfiguredDbTimezone() throws Exception {
    // Mirrors the scalar sink path: time/timestamp render in db.timezone, date in the date zone
    // (UTC on the source side). epoch 0 in America/New_York is 1969-12-31 19:00:00.
    PostgreSqlDatabaseDialect dialect = complexTypesDialect(
        JdbcSourceConnectorConfig.DB_TIMEZONE_CONFIG, "America/New_York");
    java.util.Date epoch = new java.util.Date(0L);
    verifyArrayBind(dialect, Timestamp.builder().optional().build(),
        Collections.singletonList(epoch), "timestamp",
        new Object[]{"1969-12-31 19:00:00.000"});
    verifyArrayBind(dialect, Time.builder().optional().build(),
        Collections.singletonList(epoch), "time", new Object[]{"19:00:00.000"});
    verifyArrayBind(dialect, Date.builder().optional().build(),
        Collections.singletonList(epoch), "date", new Object[]{"1970-01-01"});
  }

  @Test
  public void temporalArrayBindHonorsCalendarSystem() throws Exception {
    // The two calendar systems only diverge before the 1582 Gregorian cutover.
    java.util.Date ancient = java.sql.Timestamp.valueOf("1500-01-01 00:00:00");
    assertNotEquals(
        "date.calendar.system must change how a pre-1582 timestamp is rendered",
        bindTemporalArrayLiteral(complexTypesDialect(), ancient),
        bindTemporalArrayLiteral(prolepticGregorianDialect(), ancient));

    // A modern timestamp is rendered identically under both calendar systems.
    java.util.Date epoch = new java.util.Date(0L);
    assertEquals(
        bindTemporalArrayLiteral(complexTypesDialect(), epoch),
        bindTemporalArrayLiteral(prolepticGregorianDialect(), epoch));
  }

  /**
   * The Julian calendar runs nine days behind proleptic Gregorian in January 1500, so the same
   * milliseconds render as 1500-01-01 hybrid and 1500-01-10 proleptic. Post-1582 they agree.
   */
  @Test
  public void dateArrayBindHonorsCalendarSystem() throws Exception {
    Schema element = Date.builder().optional().build();
    java.util.Date ancient = new java.util.Date(utcHybridMillis(1500, Calendar.JANUARY, 1));

    assertEquals("1500-01-01",
        bindTemporalArrayLiteral(complexTypesDialect(), element, "date", ancient));
    assertEquals("1500-01-10",
        bindTemporalArrayLiteral(prolepticGregorianDialect(), element, "date", ancient));

    java.util.Date epoch = new java.util.Date(0L);
    assertEquals("1970-01-01",
        bindTemporalArrayLiteral(complexTypesDialect(), element, "date", epoch));
    assertEquals("1970-01-01",
        bindTemporalArrayLiteral(prolepticGregorianDialect(), element, "date", epoch));
  }

  /**
   * Array elements must render exactly as the scalar bind renders the same value. The scalar path
   * hands a {@code java.sql} value plus a {@link Calendar} to the driver, so the expectation is
   * derived that way rather than from the production formatter. Checked at 1500-01-01, where the
   * calendars disagree, under both settings.
   */
  @Test
  public void temporalArrayElementsRenderAsTheScalarBind() throws Exception {
    java.util.Date value = new java.util.Date(utcHybridMillis(1500, Calendar.JANUARY, 1));

    for (PostgreSqlDatabaseDialect dialect :
        Arrays.asList(complexTypesDialect(), prolepticGregorianDialect())) {
      assertEquals("date element must match the scalar bind",
          scalarTemporalLiteral(dialect, Date.builder().optional().build(), value, "yyyy-MM-dd"),
          bindTemporalArrayLiteral(dialect, Date.builder().optional().build(), "date", value));
      assertEquals("timestamp element must match the scalar bind",
          scalarTemporalLiteral(dialect, Timestamp.builder().optional().build(), value,
              "yyyy-MM-dd HH:mm:ss.SSS"),
          bindTemporalArrayLiteral(dialect, Timestamp.builder().optional().build(), "timestamp",
              value));
    }
  }

  @Test
  public void shouldEmitNullPerElementForMultiDimensionalArrays() throws Exception {
    DatabaseDialect.ColumnConverter converter = arrayColumnConverter("_int4");

    // Single-dimension int[] is read into a list of elements.
    ResultSet single = arrayResultSet(new Object[]{1, 2, 3});
    assertEquals(Arrays.asList(1, 2, 3), converter.convert(single));

    // Nested elements cannot be represented by the 1-D element schema, so each becomes null,
    // preserving the outer cardinality as Debezium does rather than dropping the column.
    ResultSet multi = arrayResultSet(new Object[]{new Integer[]{1, 2}, new Integer[]{3}});
    assertEquals(Arrays.asList(null, null), converter.convert(multi));

    // Detection happens on getArray() before the element-ResultSet route is chosen, so element
    // types read that way are covered too.
    ResultSet multiTimestamp = arrayResultSet(
        new Object[]{new Object[]{new java.sql.Timestamp(0)}});
    assertEquals(Collections.singletonList(null),
        arrayColumnConverter("_timestamp").convert(multiTimestamp));

    // String elements take the same route; a pass-through mapping would emit the JVM array
    // toString() here, which reads like data rather than a gap.
    ResultSet multiText = arrayResultSet(new Object[]{new String[]{"a", "b"}, new String[]{"c"}});
    assertEquals(Arrays.asList(null, null), arrayColumnConverter("_text").convert(multiText));
  }

  /**
   * The emitted nulls are invisible in the schema, so the warning is the only signal — but
   * multi-dimensionality is detectable per value, so an undeduped warning would repeat for every
   * row of every poll. It must fire once per column and drop to DEBUG after, with a second column
   * warning on its own.
   */
  @Test
  public void multiDimensionalArrayWarnsOncePerColumn() throws Exception {
    CollectingAppender appender = new CollectingAppender();
    org.apache.log4j.Logger logger =
        org.apache.log4j.Logger.getLogger(PostgreSqlDatabaseDialect.class);
    // The test log config pins io.confluent.connect to ERROR, which would filter the warning out
    // before any appender sees it.
    org.apache.log4j.Level originalLevel = logger.getLevel();
    logger.setLevel(org.apache.log4j.Level.WARN);
    logger.addAppender(appender);
    try {
      PostgreSqlDatabaseDialect dialect = complexTypesDialect();
      Object[] nested = new Object[]{new Integer[]{1, 2}, new Integer[]{3}};

      DatabaseDialect.ColumnConverter first = arrayColumnConverter(dialect, "_int4", "a");
      assertEquals(Arrays.asList(null, null), first.convert(arrayResultSet(nested)));
      assertEquals("first occurrence must warn", 1, appender.warnings.size());

      // Same column again — still nulls, but no second warning.
      assertEquals(Arrays.asList(null, null), first.convert(arrayResultSet(nested)));
      assertEquals("repeat on the same column must not warn again",
          1, appender.warnings.size());

      // A different column is tracked independently.
      DatabaseDialect.ColumnConverter second = arrayColumnConverter(dialect, "_int4", "b");
      assertEquals(Arrays.asList(null, null), second.convert(arrayResultSet(nested)));
      assertEquals("a second column must warn on its own", 2, appender.warnings.size());
    } finally {
      logger.removeAppender(appender);
      logger.setLevel(originalLevel);
    }
  }

  @Test
  public void hstoreArrayElementSchemaShouldFollowHandlingMode() {
    // map mode: ARRAY<MAP<STRING,STRING>> with optional values so a NULL survives.
    Schema mapMode = sourceFieldSchema(hstoreDialect("true", "map"), Types.ARRAY, "_hstore");
    assertEquals(Type.ARRAY, mapMode.type());
    assertEquals(Type.MAP, mapMode.valueSchema().type());
    assertTrue(mapMode.valueSchema().isOptional());
    assertTrue(mapMode.valueSchema().valueSchema().isOptional());

    // json mode: ARRAY<Json>, so the sink provisions jsonb[] either way.
    Schema jsonMode = sourceFieldSchema(jsonHstoreDialect(), Types.ARRAY, "_hstore");
    assertEquals(Type.ARRAY, jsonMode.type());
    assertEquals(Json.LOGICAL_NAME, jsonMode.valueSchema().name());
  }

  @Test
  public void hstoreArrayShouldReadElementsPerHandlingMode() throws Exception {
    Map<String, String> withNull = new LinkedHashMap<>();
    withNull.put("k", null);

    // map mode: the driver's maps pass through, including an hstore NULL value.
    assertEquals(
        Arrays.asList(Collections.singletonMap("env", "prod"), withNull),
        arrayColumnConverter(hstoreDialect("true", "map"), "_hstore").convert(
            hstoreArrayResultSet(Collections.singletonMap("env", "prod"), withNull)));

    // json mode: each element is serialized, with the hstore NULL becoming a JSON null.
    assertEquals(
        Arrays.asList("{\"env\":\"prod\"}", "{\"k\":null}"),
        arrayColumnConverter(jsonHstoreDialect(), "_hstore").convert(
            hstoreArrayResultSet(Collections.singletonMap("env", "prod"), withNull)));
  }

  @Test
  public void hstoreArrayShouldBindPerMode() throws SQLException {
    // map mode: each element serialized to hstore text, bound as hstore[].
    verifyArrayBind(
        SchemaBuilder.map(Schema.STRING_SCHEMA, Schema.OPTIONAL_STRING_SCHEMA).optional().build(),
        Arrays.asList(Collections.singletonMap("env", "prod"), null),
        PostgreSqlDatabaseDialect.HSTORE_TYPE_NAME,
        new Object[]{"\"env\"=>\"prod\"", null});

    // json mode: elements are already JSON text, and stay a jsonb[] document array.
    verifyArrayBind(
        Json.optionalSchema(),
        Arrays.asList("{\"env\":\"prod\"}", null),
        PostgreSqlDatabaseDialect.JSONB_TYPE_NAME,
        new Object[]{"{\"env\":\"prod\"}", null});
  }

  @Test
  public void hstoreArrayShouldBeDroppedWhenComplexTypesDisabled() {
    PostgreSqlDatabaseDialect disabled =
        new PostgreSqlDatabaseDialect(sourceConfigWithUrl("jdbc:postgresql://something"));
    assertNull(sourceFieldSchema(disabled, Types.ARRAY, "_hstore"));
  }

  /**
   * The array type is its own pg_type row in the same schema, so it is qualified exactly as the
   * scalar type is, and is mapped the same way.
   */
  @Test
  public void offSearchPathHstoreArrayIsMappedInEveryMode() {
    for (String mode : new String[]{"map", "json"}) {
      Schema schema = sourceFieldSchema(
          hstoreDialect("true", mode), Types.ARRAY, "\"ext\".\"_hstore\"");
      assertNotNull(mode + " must map the column", schema);
      assertEquals(Type.ARRAY, schema.type());
      assertEquals(hstoreDialect("true", mode).hstoreSchema(true), schema.valueSchema());
    }

    // none stays the escape hatch, and an unrelated array element type is untouched.
    assertNull(sourceFieldSchema(
        hstoreDialect("true", "none"), Types.ARRAY, "\"ext\".\"_hstore\""));
    assertNull(sourceFieldSchema(
        hstoreDialect("true", "map"), Types.ARRAY, "\"ext\".\"_citext\""));
  }

  @Test
  public void hstoreArrayShouldBeDroppedWhenHandlingModeIsNone() {
    // none applies to hstore[] exactly as to a scalar hstore column, so the two cannot disagree.
    // The element has no representation, so the whole array column is skipped.
    assertNull(sourceFieldSchema(hstoreDialect("true", "none"), Types.ARRAY, "_hstore"));
    assertNull(sourceFieldSchema(
        new PostgreSqlDatabaseDialect(sourceConfigWithUrl("jdbc:postgresql://something",
            JdbcSourceConnectorConfig.SQL_COMPLEX_TYPES_ENABLE_CONFIG, "true")),
        Types.ARRAY, "_hstore"));

    // Other element types are unaffected by the hstore mode.
    assertEquals(Type.ARRAY,
        sourceFieldSchema(hstoreDialect("true", "none"), Types.ARRAY, "_int4").type());
  }

  @Test
  public void arrayConverterRoutesByElementLogicalType() throws Exception {
    // numeric[] -> each element decoded into a VariableScaleDecimal struct (per-value scale).
    Object num = ((List<?>) arrayColumnConverter("_numeric")
        .convert(arrayResultSet(new Object[]{new BigDecimal("1.5")}))).get(0);
    assertEquals(0, new BigDecimal("1.5").compareTo(
        VariableScaleDecimal.toLogical((Struct) num)));

    // jsonb[] -> each element carried as its raw JSON text.
    Object json = ((List<?>) arrayColumnConverter("_jsonb")
        .convert(arrayResultSet(new Object[]{"{\"k\":1}"}))).get(0);
    assertEquals("{\"k\":1}", json);
  }

  @Test
  public void arrayReadPreservesNullElements() throws Exception {
    List<?> numeric = (List<?>) arrayColumnConverter("_numeric")
        .convert(arrayResultSet(new Object[]{new BigDecimal("1.5"), null}));
    assertEquals(2, numeric.size());
    assertNull(numeric.get(1));

    assertEquals(Arrays.asList("{\"k\":1}", null), arrayColumnConverter("_jsonb")
        .convert(arrayResultSet(new Object[]{"{\"k\":1}", null})));

    List<?> temporal = (List<?>) arrayColumnConverter("_timestamp")
        .convert(temporalArrayResultSetWithNullElement());
    assertEquals(2, temporal.size());
    assertNull(temporal.get(1));
  }

  @Test
  public void temporalArrayReadHonorsConfiguredDbTimezone() throws Exception {
    // timestamp[] elements must be decoded with the configured db.timezone, matching the scalar
    // (non-array) timestamp path, rather than a hard-coded UTC calendar.
    TimeZone tz = TimeZone.getTimeZone("America/New_York");
    ResultSet elementRs = captureTemporalArrayRead("_timestamp", tz);
    ArgumentCaptor<Calendar> cal = ArgumentCaptor.forClass(Calendar.class);
    verify(elementRs).getTimestamp(eq(2), cal.capture());
    assertEquals(tz, cal.getValue().getTimeZone());
  }

  @Test
  public void timestamptzArrayReadHonorsConfiguredDbTimezone() throws Exception {
    // timestamptz[] drops the zone and is decoded like timestamp[], honoring db.timezone (scalar).
    TimeZone tz = TimeZone.getTimeZone("America/New_York");
    ResultSet elementRs = captureTemporalArrayRead("_timestamptz", tz);
    ArgumentCaptor<Calendar> cal = ArgumentCaptor.forClass(Calendar.class);
    verify(elementRs).getTimestamp(eq(2), cal.capture());
    assertEquals(tz, cal.getValue().getTimeZone());
  }

  @Test
  public void dateAndTimeArrayReadsFollowTheScalarZonePrecedence() throws Exception {
    // date is decoded with the date time zone (UTC on the source), time with db.timezone.
    TimeZone tz = TimeZone.getTimeZone("America/New_York");

    ResultSet dateRs = captureTemporalArrayRead("_date", tz);
    ArgumentCaptor<Calendar> dateCal = ArgumentCaptor.forClass(Calendar.class);
    verify(dateRs).getDate(eq(2), dateCal.capture());
    assertEquals(TimeZone.getTimeZone("UTC"), dateCal.getValue().getTimeZone());

    ResultSet timeRs = captureTemporalArrayRead("_time", tz);
    ArgumentCaptor<Calendar> timeCal = ArgumentCaptor.forClass(Calendar.class);
    verify(timeRs).getTime(eq(2), timeCal.capture());
    assertEquals(tz, timeCal.getValue().getTimeZone());
  }

  @Test
  public void timestampArrayReadHonorsCalendarSystem() throws Exception {
    // A pre-1582 timestamp is shifted by the proleptic Gregorian conversion.
    java.sql.Timestamp ancient = java.sql.Timestamp.valueOf("1500-01-01 00:00:00");
    assertNotEquals(
        "date.calendar.system must change how a pre-1582 timestamp is decoded",
        readSingleTimestampElement(complexTypesDialect(), ancient),
        readSingleTimestampElement(prolepticGregorianDialect(), ancient));

    // A modern timestamp decodes identically under both calendar systems.
    java.sql.Timestamp epoch = new java.sql.Timestamp(0L);
    assertEquals(
        readSingleTimestampElement(complexTypesDialect(), epoch),
        readSingleTimestampElement(prolepticGregorianDialect(), epoch));
  }

  @Test
  public void timestampArrayReadHonorsPrecisionModeValue() throws Exception {
    // micros_long emits epoch microseconds instead of a Connect Timestamp: 1000 ms -> 1_000_000 us.
    Object value = readSingleTimestampElement(
        complexTypesDialect(JdbcSourceConnectorConfig.TIMESTAMP_GRANULARITY_CONFIG, "micros_long"),
        new java.sql.Timestamp(1000L));
    assertEquals(1_000_000L, value);
  }

  @Test
  public void shouldReturnNullWhenArrayHasNoContents() throws Exception {
    // getArray() returning null short circuits before any element decoding.
    ResultSet resultSet = mock(ResultSet.class);
    Array array = mock(Array.class);
    when(resultSet.getArray(1)).thenReturn(array);
    when(array.getArray()).thenReturn(null);

    assertNull(arrayColumnConverter("_int4").convert(resultSet));
  }

  @Test
  public void shouldReturnEmptyListWhenArrayHasNoElements() throws Exception {
    assertEquals(Collections.emptyList(),
        arrayColumnConverter("_int4").convert(arrayResultSet(new Object[0])));
  }

  @Test
  public void shouldRejectUnsupportedArrayValueType() {
    // A Connect ARRAY value must be a Collection or a Java array; anything else is a DataException.
    Schema schema = SchemaBuilder.array(Schema.OPTIONAL_INT32_SCHEMA).optional().build();
    ColumnDefinition colDef = mock(ColumnDefinition.class);
    assertThrows(DataException.class, () -> complexTypesDialect().bindField(
        mock(PreparedStatement.class), 1, schema, "not-an-array", colDef, "field"));
  }

  private PostgreSqlDatabaseDialect prolepticGregorianDialect() {
    return complexTypesDialect(
        JdbcSourceConnectorConfig.DATE_CALENDAR_SYSTEM_CONFIG, "PROLEPTIC_GREGORIAN");
  }

  /** Epoch millis of a hybrid-calendar date at UTC midnight, independent of the JVM zone. */
  private static long utcHybridMillis(int year, int month, int day) {
    Calendar calendar = new GregorianCalendar(TimeZone.getTimeZone("UTC"));
    calendar.clear();
    calendar.set(year, month, day);
    return calendar.getTimeInMillis();
  }

  /**
   * What the scalar bind sends: the {@code java.sql} value and {@link Calendar} handed to the driver,
   * rendered as the driver would. Uses no production formatter, so it is an independent oracle.
   */
  private String scalarTemporalLiteral(PostgreSqlDatabaseDialect dialect, Schema schema,
      java.util.Date value, String pattern) throws SQLException {
    PreparedStatement statement = mock(PreparedStatement.class);
    dialect.bindField(statement, 1, schema, value, mock(ColumnDefinition.class), "field");

    ArgumentCaptor<java.util.Date> bound = ArgumentCaptor.forClass(java.util.Date.class);
    ArgumentCaptor<Calendar> calendar = ArgumentCaptor.forClass(Calendar.class);
    if (Date.LOGICAL_NAME.equals(schema.name())) {
      verify(statement).setDate(eq(1), (java.sql.Date) bound.capture(), calendar.capture());
    } else {
      verify(statement).setTimestamp(eq(1), (java.sql.Timestamp) bound.capture(),
          calendar.capture());
    }
    SimpleDateFormat format = new SimpleDateFormat(pattern, Locale.ROOT);
    format.setTimeZone(calendar.getValue().getTimeZone());
    return format.format(bound.getValue());
  }

  /**
   * Drive the array column converter for a single-element temporal array with the given
   * {@code db.timezone}, wiring a mock element {@link ResultSet}. Returns that element ResultSet so
   * the caller can verify the Calendar (time zone) used to decode the element.
   */
  private ResultSet captureTemporalArrayRead(String pgArrayType, TimeZone tz)
      throws SQLException, java.io.IOException {
    PostgreSqlDatabaseDialect dialect = complexTypesDialect(
        JdbcSourceConnectorConfig.DB_TIMEZONE_CONFIG, tz.getID());
    ColumnDefinition column = column(Types.ARRAY, pgArrayType);
    ColumnMapping mapping = new ColumnMapping(
        column, 1, new Field("col", 0, arraySchema(Schema.OPTIONAL_INT64_SCHEMA)));
    DatabaseDialect.ColumnConverter converter =
        dialect.columnConverterFor(mapping, column, 1, true);
    assertNotNull(converter);

    ResultSet resultSet = mock(ResultSet.class);
    Array array = mock(Array.class);
    ResultSet elementRs = mock(ResultSet.class);
    when(resultSet.getArray(1)).thenReturn(array);
    when(array.getArray()).thenReturn(new Object[]{new java.sql.Timestamp(0L)});
    when(array.getResultSet()).thenReturn(elementRs);
    when(elementRs.next()).thenReturn(true, false);
    when(elementRs.getTimestamp(eq(2), any(Calendar.class)))
        .thenReturn(new java.sql.Timestamp(0L));
    when(elementRs.getDate(eq(2), any(Calendar.class))).thenReturn(new java.sql.Date(0L));
    when(elementRs.getTime(eq(2), any(Calendar.class))).thenReturn(new java.sql.Time(0L));
    when(elementRs.wasNull()).thenReturn(false);

    converter.convert(resultSet);
    return elementRs;
  }

  /**
   * Bind a single-element {@code timestamp[]} with the given dialect and return the rendered text
   * literal, so tests can assert the zone and calendar system applied on the sink path.
   */
  private String bindTemporalArrayLiteral(PostgreSqlDatabaseDialect dialect, java.util.Date value)
      throws SQLException {
    return bindTemporalArrayLiteral(
        dialect, Timestamp.builder().optional().build(), "timestamp", value);
  }

  private String bindTemporalArrayLiteral(PostgreSqlDatabaseDialect dialect, Schema elementSchema,
      String pgElementType, java.util.Date value) throws SQLException {
    PreparedStatement statement = mock(PreparedStatement.class);
    Connection connection = mock(Connection.class);
    ColumnDefinition colDef = mock(ColumnDefinition.class);
    when(colDef.type()).thenReturn(Types.ARRAY);
    when(statement.getConnection()).thenReturn(connection);
    when(connection.createArrayOf(eq(pgElementType), any())).thenReturn(mock(Array.class));

    Schema schema = SchemaBuilder.array(elementSchema).optional().build();
    dialect.bindField(statement, 1, schema, Collections.singletonList(value), colDef, "field");

    ArgumentCaptor<Object[]> captor = ArgumentCaptor.forClass(Object[].class);
    verify(connection).createArrayOf(eq(pgElementType), captor.capture());
    return (String) captor.getValue()[0];
  }

  /**
   * Read a single-element {@code timestamp[]} with the given dialect and return the decoded Connect
   * value, so tests can assert the calendar system and precision mode applied on the source path.
   */
  private Object readSingleTimestampElement(
      PostgreSqlDatabaseDialect dialect, java.sql.Timestamp value) throws Exception {
    ColumnDefinition column = column(Types.ARRAY, "_timestamp");
    ColumnMapping mapping = new ColumnMapping(
        column, 1, new Field("col", 0, arraySchema(Schema.OPTIONAL_INT64_SCHEMA)));
    DatabaseDialect.ColumnConverter converter =
        dialect.columnConverterFor(mapping, column, 1, true);
    assertNotNull(converter);

    ResultSet resultSet = mock(ResultSet.class);
    Array array = mock(Array.class);
    ResultSet elementRs = mock(ResultSet.class);
    when(resultSet.getArray(1)).thenReturn(array);
    when(array.getArray()).thenReturn(new Object[]{value});
    when(array.getResultSet()).thenReturn(elementRs);
    when(elementRs.next()).thenReturn(true, false);
    when(elementRs.getTimestamp(eq(2), any(Calendar.class))).thenReturn(value);
    when(elementRs.wasNull()).thenReturn(false);

    return ((List<?>) converter.convert(resultSet)).get(0);
  }

  private static Schema arraySchema(Schema elementSchema) {
    return SchemaBuilder.array(elementSchema).build();
  }

  private void assertArrayElement(String pgArrayType, Type elementType, String elementName) {
    Schema schema = sourceFieldSchema(complexTypesDialect(), Types.ARRAY, pgArrayType);
    assertNotNull("Array column " + pgArrayType + " should produce a field", schema);
    assertEquals(Type.ARRAY, schema.type());
    assertEquals(elementType, schema.valueSchema().type());
    assertEquals(elementName, schema.valueSchema().name());
  }

  private ColumnDefinition column(int jdbcType, String typeName,
      ColumnDefinition.Nullability nullability, String columnName) {
    return new ColumnDefinition(
        new ColumnId(new TableId(null, null, "t"), columnName),
        jdbcType, typeName, Object.class.getName(),
        nullability, ColumnDefinition.Mutability.UNKNOWN,
        0, 0, false, 1, false, false, false, false, false);
  }

  private DatabaseDialect.ColumnConverter arrayColumnConverter(String pgArrayType) {
    return arrayColumnConverter(complexTypesDialect(), pgArrayType);
  }

  private DatabaseDialect.ColumnConverter arrayColumnConverter(
      PostgreSqlDatabaseDialect dialect, String pgArrayType) {
    return arrayColumnConverter(dialect, pgArrayType, "col");
  }

  private DatabaseDialect.ColumnConverter arrayColumnConverter(
      PostgreSqlDatabaseDialect dialect, String pgArrayType, String columnName) {
    ColumnDefinition column =
        column(Types.ARRAY, pgArrayType, ColumnDefinition.Nullability.NULL, columnName);
    ColumnMapping mapping = new ColumnMapping(
        column, 1, new Field("col", 0, arraySchema(Schema.OPTIONAL_INT32_SCHEMA)));
    DatabaseDialect.ColumnConverter converter =
        dialect.columnConverterFor(mapping, column, 1, true);
    assertNotNull(converter);
    return converter;
  }

  /** A source dialect with complex types enabled and {@code hstore.handling.mode=json}. */
  private PostgreSqlDatabaseDialect jsonHstoreDialect() {
    return new PostgreSqlDatabaseDialect(sourceConfigWithUrl("jdbc:postgresql://something",
        JdbcSourceConnectorConfig.SQL_COMPLEX_TYPES_ENABLE_CONFIG, "true",
        JdbcSourceConnectorConfig.HSTORE_HANDLING_MODE_CONFIG, "json"));
  }

  private static ResultSet arrayResultSet(Object[] elements) throws SQLException {
    ResultSet resultSet = mock(ResultSet.class);
    Array array = mock(Array.class);
    when(resultSet.getArray(1)).thenReturn(array);
    when(array.getArray()).thenReturn(elements);
    return resultSet;
  }

  /**
   * An {@code hstore[]} ResultSet of two elements. {@code getArray()} yields opaque PGobjects, as
   * pgjdbc does, while the element ResultSet yields the driver-decoded maps.
   */
  private static ResultSet hstoreArrayResultSet(Map<?, ?> first, Map<?, ?> second)
      throws SQLException {
    ResultSet resultSet = arrayResultSet(new Object[]{new Object(), new Object()});
    ResultSet elementRs = mock(ResultSet.class);
    when(resultSet.getArray(1).getResultSet()).thenReturn(elementRs);
    when(elementRs.next()).thenReturn(true, true, false);
    when(elementRs.getObject(2)).thenReturn(first, second);
    return resultSet;
  }

  private static ResultSet temporalArrayResultSetWithNullElement() throws SQLException {
    java.sql.Timestamp epoch = new java.sql.Timestamp(0L);
    ResultSet resultSet = arrayResultSet(new Object[]{epoch, null});
    ResultSet elementRs = mock(ResultSet.class);
    when(resultSet.getArray(1).getResultSet()).thenReturn(elementRs);
    when(elementRs.next()).thenReturn(true, true, false);
    when(elementRs.getTimestamp(eq(2), any(Calendar.class))).thenReturn(epoch);
    when(elementRs.wasNull()).thenReturn(false, true);
    return resultSet;
  }

  private void verifyArrayBind(
      Schema elementSchema,
      List<?> value,
      String expectedPgType,
      Object[] expectedElements
  ) throws SQLException {
    verifyArrayBind(complexTypesDialect(), elementSchema, value, expectedPgType, expectedElements);
  }

  private void verifyArrayBind(
      PostgreSqlDatabaseDialect dialect,
      Schema elementSchema,
      List<?> value,
      String expectedPgType,
      Object[] expectedElements
  ) throws SQLException {
    PreparedStatement statement = mock(PreparedStatement.class);
    Connection connection = mock(Connection.class);
    Array boundArray = mock(Array.class);
    ColumnDefinition colDef = mock(ColumnDefinition.class);
    when(colDef.type()).thenReturn(Types.ARRAY);
    when(statement.getConnection()).thenReturn(connection);
    when(connection.createArrayOf(eq(expectedPgType), any())).thenReturn(boundArray);

    Schema arraySchema = SchemaBuilder.array(elementSchema).optional().build();
    dialect.bindField(statement, 7, arraySchema, value, colDef, "field");

    ArgumentCaptor<Object[]> captor = ArgumentCaptor.forClass(Object[].class);
    verify(connection).createArrayOf(eq(expectedPgType), captor.capture());
    assertEquals(Arrays.asList(expectedElements), Arrays.asList(captor.getValue()));
    verify(statement).setArray(7, boundArray);
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
}
