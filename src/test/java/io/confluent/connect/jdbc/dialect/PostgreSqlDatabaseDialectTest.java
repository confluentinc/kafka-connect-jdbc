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
import io.confluent.connect.jdbc.data.VariableScaleDecimal;
import io.confluent.connect.jdbc.data.ZonedTimestamp;
import io.confluent.connect.jdbc.sink.JdbcSinkConfig;
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
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.data.Time;
import org.apache.kafka.connect.data.Timestamp;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

import java.math.BigDecimal;
import java.sql.Array;
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
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;


import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
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
    TableDefinition tableDefn = builder.build();
    ColumnId uuidColumn = tableDefn.definitionForColumn("uuidColumn").id();
    ColumnId dateColumn = tableDefn.definitionForColumn("dateColumn").id();
    assertEquals("", dialect.valueTypeCast(tableDefn, columnPK1));
    assertEquals("", dialect.valueTypeCast(tableDefn, columnPK2));
    assertEquals("", dialect.valueTypeCast(tableDefn, columnA));
    assertEquals("::uuid", dialect.valueTypeCast(tableDefn, uuidColumn));
    assertEquals("", dialect.valueTypeCast(tableDefn, dateColumn));
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
    // timestamptz carries the zone via the ZonedTimestamp logical STRING (not built-in Timestamp).
    assertArrayElement("_timestamptz", Type.STRING, ZonedTimestamp.LOGICAL_NAME);
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
  public void jsonHandlingModeShouldSelectSourceSchema() {
    // Default is "string": a logical JSON STRING tagged with the Json logical name.
    Schema stringMode = sourceFieldSchema(complexTypesDialect(), Types.OTHER, "jsonb");
    assertEquals(Type.STRING, stringMode.type());
    assertEquals(Json.LOGICAL_NAME, stringMode.name());

    // "map": a shallow Map<String,String>.
    PostgreSqlDatabaseDialect mapDialect = complexTypesDialect(
        JdbcSourceConnectorConfig.JSON_HANDLING_MODE_CONFIG,
        JdbcSourceConnectorConfig.JSON_HANDLING_MODE_MAP);
    assertEquals(Type.MAP, sourceFieldSchema(mapDialect, Types.OTHER, "json").type());
  }

  @Test
  public void hstoreHandlingModeShouldSelectSourceSchema() {
    // Default is "map": a Map<String,String>.
    assertEquals(Type.MAP,
        sourceFieldSchema(complexTypesDialect(), Types.OTHER, "hstore").type());

    // "json": a plain (untagged) STRING, which the sink lands in a TEXT column.
    PostgreSqlDatabaseDialect jsonDialect = complexTypesDialect(
        JdbcSourceConnectorConfig.HSTORE_HANDLING_MODE_CONFIG,
        JdbcSourceConnectorConfig.HSTORE_HANDLING_MODE_JSON);
    Schema jsonMode = sourceFieldSchema(jsonDialect, Types.OTHER, "hstore");
    assertEquals(Type.STRING, jsonMode.type());
    assertNull(jsonMode.name());
  }

  @Test
  public void shouldMapComplexTypesToSqlTypes() {
    // Scalar logical JSON STRING -> native JSONB.
    verifyDataTypeMapping("JSONB", Json.schema());
    // Arrays -> native elementType[]; the element DDL is exercised through the ARRAY recursion.
    verifyDataTypeMapping("TEXT[]", arraySchema(Schema.STRING_SCHEMA));
    verifyDataTypeMapping("SMALLINT[]", arraySchema(Schema.INT16_SCHEMA));
    verifyDataTypeMapping("INT[]", arraySchema(Schema.INT32_SCHEMA));
    verifyDataTypeMapping("BIGINT[]", arraySchema(Schema.INT64_SCHEMA));
    verifyDataTypeMapping("REAL[]", arraySchema(Schema.FLOAT32_SCHEMA));
    verifyDataTypeMapping("DOUBLE PRECISION[]", arraySchema(Schema.FLOAT64_SCHEMA));
    verifyDataTypeMapping("BOOLEAN[]", arraySchema(Schema.BOOLEAN_SCHEMA));
    verifyDataTypeMapping("JSONB[]", arraySchema(Json.optionalSchema()));
    verifyDataTypeMapping("NUMERIC[]", arraySchema(VariableScaleDecimal.optionalSchema()));
    verifyDataTypeMapping("DATE[]", arraySchema(Date.builder().optional().build()));
    verifyDataTypeMapping("TIME[]", arraySchema(Time.builder().optional().build()));
    verifyDataTypeMapping("TIMESTAMP[]", arraySchema(Timestamp.builder().optional().build()));
    verifyDataTypeMapping("TIMESTAMP WITH TIME ZONE[]",
        arraySchema(ZonedTimestamp.optionalSchema()));
  }

  @Test
  public void shouldBindJsonArrayAsNativeJsonbArray() throws Exception {
    verifyArrayBind(
        Json.optionalSchema(),
        Arrays.asList("{\"k\":\"v\"}", "{\"a\":1}"),
        "jsonb",
        new Object[]{"{\"k\":\"v\"}", "{\"a\":1}"});
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
  public void shouldBindTemporalArraysAsNativeArraysInUtc() throws Exception {
    // epoch 0 == 1970-01-01T00:00:00Z; each element is rendered as a UTC text literal.
    java.util.Date epoch = new java.util.Date(0L);
    verifyArrayBind(Date.builder().optional().build(),
        Collections.singletonList(epoch), "date", new Object[]{"1970-01-01"});
    verifyArrayBind(Time.builder().optional().build(),
        Collections.singletonList(epoch), "time", new Object[]{"00:00:00.000"});
    verifyArrayBind(Timestamp.builder().optional().build(),
        Collections.singletonList(epoch), "timestamp", new Object[]{"1970-01-01 00:00:00.000"});
  }

  @Test
  public void shouldBindZonedTimestampArrayAsNativeTimestamptzArray() throws Exception {
    // ZonedTimestamp elements are ISO-8601 offset strings; they bind straight into timestamptz[].
    verifyArrayBind(
        ZonedTimestamp.optionalSchema(),
        Collections.singletonList("2025-06-10T13:00:00Z"),
        "timestamptz",
        new Object[]{"2025-06-10T13:00:00Z"});
  }

  @Test
  public void shouldSkipMultiDimensionalArraysWithoutFailing() throws Exception {
    DatabaseDialect.ColumnConverter converter = arrayColumnConverter("_int4");

    // Single-dimension int[] is read into a list of elements.
    ResultSet single = arrayResultSet(new Object[]{1, 2, 3});
    assertEquals(Arrays.asList(1, 2, 3), converter.convert(single));

    // Multi-dimension int[][] is skipped (null) rather than crashing the task.
    ResultSet multi = arrayResultSet(new Object[]{new Integer[]{1, 2}, new Integer[]{3}});
    assertNull(converter.convert(multi));
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
  public void shouldBindStructValueAsJsonStringForJsonbColumn() throws Exception {
    // STRUCT/MAP (json/hstore map mode) serialize to JSON, bind as String, cast ::jsonb on the sink.
    PreparedStatement statement = mock(PreparedStatement.class);
    ColumnDefinition colDef = mock(ColumnDefinition.class);
    Schema schema = SchemaBuilder.struct().field("a", Schema.INT32_SCHEMA).optional().build();
    Struct value = new Struct(schema).put("a", 1);

    sinkDialect().bindField(statement, 3, schema, value, colDef, "field");

    verify(statement).setString(3, "{\"a\":1}");
  }

  // ----- complex-type test helpers -----

  private PostgreSqlDatabaseDialect complexTypesDialect(String... extraProps) {
    String[] props = new String[extraProps.length + 2];
    props[0] = JdbcSourceConnectorConfig.SQL_COMPLEX_TYPES_ENABLE_CONFIG;
    props[1] = "true";
    System.arraycopy(extraProps, 0, props, 2, extraProps.length);
    return new PostgreSqlDatabaseDialect(sourceConfigWithUrl("jdbc:postgresql://something", props));
  }

  private PostgreSqlDatabaseDialect sinkDialect() {
    return new PostgreSqlDatabaseDialect(sinkConfigWithUrl(
        "jdbc:postgresql://something", JdbcSinkConfig.SQL_COMPLEX_TYPES_ENABLE, "true"));
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

  private Schema sourceFieldSchema(
      PostgreSqlDatabaseDialect dialect, int jdbcType, String typeName) {
    ColumnDefinition column = column(jdbcType, typeName);
    SchemaBuilder builder = SchemaBuilder.struct();
    String fieldName = dialect.addFieldToSchema(column, builder);
    return fieldName == null ? null : builder.build().field(fieldName).schema();
  }

  private ColumnDefinition column(int jdbcType, String typeName) {
    return new ColumnDefinition(
        new ColumnId(new TableId(null, null, "t"), "col"),
        jdbcType, typeName, Object.class.getName(),
        ColumnDefinition.Nullability.NULL, ColumnDefinition.Mutability.UNKNOWN,
        0, 0, false, 1, false, false, false, false, false);
  }

  private DatabaseDialect.ColumnConverter arrayColumnConverter(String pgArrayType) {
    ColumnDefinition column = column(Types.ARRAY, pgArrayType);
    ColumnMapping mapping = new ColumnMapping(
        column, 1, new Field("col", 0, arraySchema(Schema.OPTIONAL_INT32_SCHEMA)));
    DatabaseDialect.ColumnConverter converter =
        complexTypesDialect().columnConverterFor(mapping, column, 1, true);
    assertNotNull(converter);
    return converter;
  }

  private static ResultSet arrayResultSet(Object[] elements) throws SQLException {
    ResultSet resultSet = mock(ResultSet.class);
    Array array = mock(Array.class);
    when(resultSet.getArray(1)).thenReturn(array);
    when(array.getArray()).thenReturn(elements);
    return resultSet;
  }

  private void verifyArrayBind(
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
    complexTypesDialect().bindField(statement, 7, arraySchema, value, colDef, "field");

    ArgumentCaptor<Object[]> captor = ArgumentCaptor.forClass(Object[].class);
    verify(connection).createArrayOf(eq(expectedPgType), captor.capture());
    assertEquals(Arrays.asList(expectedElements), Arrays.asList(captor.getValue()));
    verify(statement).setArray(7, boundArray);
  }

}
