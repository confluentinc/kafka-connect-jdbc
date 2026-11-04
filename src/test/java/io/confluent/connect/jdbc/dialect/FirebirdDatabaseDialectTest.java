/*
 * Copyright 2021 Confluent Inc.
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

import io.confluent.connect.jdbc.util.QuoteMethod;

import static org.junit.Assert.assertEquals;

public class FirebirdDatabaseDialectTest extends BaseDialectTest<FirebirdDatabaseDialect> {

  @Override
  protected FirebirdDatabaseDialect createDialect() {
    return new FirebirdDatabaseDialect(sourceConfigWithUrl("jdbc:firebirdsql://server:port"));
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
  public void shouldBuildAlterTableStatement() {
    assertStatements(
        new String[]{
            "ALTER TABLE \"myTable\" \n"
                + "ADD \"c1\" INT NOT NULL,\n"
                + "ADD \"c2\" BIGINT NOT NULL,\n"
                + "ADD \"c3\" BLOB SUB_TYPE TEXT NOT NULL,\n"
                + "ADD \"c4\" BLOB SUB_TYPE TEXT NULL,\n"
                + "ADD \"c5\" DATE DEFAULT '2001-03-15',\n"
                + "ADD \"c6\" TIME DEFAULT '00:00:00.000',\n"
                + "ADD \"c7\" TIMESTAMP DEFAULT '2001-03-15 00:00:00.000',\n"
                + "ADD \"c8\" DECIMAL(18,4) NULL,\n"
                + "ADD \"c9\" BOOLEAN DEFAULT 1"
        },
        dialect.buildAlterTable(tableId, sinkRecordFields)
    );

    quoteIdentfiiers = QuoteMethod.NEVER;
    dialect = createDialect();
    assertStatements(
        new String[]{
            "ALTER TABLE myTable \n"
                + "ADD c1 INT NOT NULL,\n"
                + "ADD c2 BIGINT NOT NULL,\n"
                + "ADD c3 BLOB SUB_TYPE TEXT NOT NULL,\n"
                + "ADD c4 BLOB SUB_TYPE TEXT NULL,\n"
                + "ADD c5 DATE DEFAULT '2001-03-15',\n"
                + "ADD c6 TIME DEFAULT '00:00:00.000',\n"
                + "ADD c7 TIMESTAMP DEFAULT '2001-03-15 00:00:00.000',\n"
                + "ADD c8 DECIMAL(18,4) NULL,\n"
                + "ADD c9 BOOLEAN DEFAULT 1"
        },
        dialect.buildAlterTable(tableId, sinkRecordFields)
    );
  }

  @Test
  public void shouldBuildUpsertStatement() {
    String expected = "MERGE INTO \"myTable\" USING (SELECT ? \"id1\", ? \"id2\", ? \"columnA\", " +
        "? \"columnB\", ? \"columnC\", ? \"columnD\" FROM RDB$DATABASE) SOURCE ON" +
        "(\"myTable\".\"id1\"=SOURCE.\"id1\" AND \"myTable\".\"id2\"=SOURCE" +
        ".\"id2\") WHEN MATCHED THEN UPDATE SET \"myTable\".\"columnA\"=SOURCE" +
        ".\"columnA\",\"myTable\".\"columnB\"=SOURCE.\"columnB\",\"myTable\"" +
        ".\"columnC\"=SOURCE.\"columnC\",\"myTable\".\"columnD\"=SOURCE" +
        ".\"columnD\" WHEN NOT MATCHED THEN INSERT(\"myTable\".\"columnA\"," +
        "\"myTable\".\"columnB\",\"myTable\".\"columnC\",\"myTable\".\"columnD\"," +
        "\"myTable\".\"id1\",\"myTable\".\"id2\") VALUES(SOURCE.\"columnA\"," +
        "SOURCE.\"columnB\",SOURCE.\"columnC\",SOURCE.\"columnD\",SOURCE" +
        ".\"id1\",SOURCE.\"id2\")";
    String sql = dialect.buildUpsertQueryStatement(tableId, pkColumns, columnsAtoD);
    assertEquals(expected, sql);
  }

  @Test
  public void shouldSanitizeUrlWithoutCredentialsInProperties() {
    assertSanitizedUrl(
        "jdbc:firebirdsql://localhost:3050/employee",
        "jdbc:firebirdsql://localhost:3050/employee"
    );
  }

  @Test
  public void shouldSanitizeUrlWithCredentialsInUrlProperties() {
    assertSanitizedUrl(
        "jdbc:firebirdsql://localhost/employee?user=SYSDBA&password=masterkey&authPlugins=Legacy_Auth,Srp512",
        "jdbc:firebirdsql://localhost/employee?user=SYSDBA&password=****&authPlugins=Legacy_Auth,Srp512"
    );

  }
}
