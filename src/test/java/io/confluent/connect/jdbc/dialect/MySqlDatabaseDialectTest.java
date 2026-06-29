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
import org.apache.kafka.connect.data.Time;
import org.apache.kafka.connect.data.Timestamp;
import org.apache.kafka.connect.errors.ConnectException;
import org.junit.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import io.confluent.connect.jdbc.source.JdbcSourceConnectorConfig;
import io.confluent.connect.jdbc.util.QuoteMethod;
import io.confluent.connect.jdbc.util.TableId;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;

public class MySqlDatabaseDialectTest extends BaseDialectTest<MySqlDatabaseDialect> {

  @Override
  protected MySqlDatabaseDialect createDialect() {
    return new MySqlDatabaseDialect(sourceConfigWithUrl("jdbc:mysql://something"));
  }

  @Test
  public void shouldMapPrimitiveSchemaTypeToSqlTypes() {
    assertPrimitiveMapping(Type.INT8, "TINYINT");
    assertPrimitiveMapping(Type.INT16, "SMALLINT");
    assertPrimitiveMapping(Type.INT32, "INT");
    assertPrimitiveMapping(Type.INT64, "BIGINT");
    assertPrimitiveMapping(Type.FLOAT32, "FLOAT");
    assertPrimitiveMapping(Type.FLOAT64, "DOUBLE");
    assertPrimitiveMapping(Type.BOOLEAN, "TINYINT");
    assertPrimitiveMapping(Type.BYTES, "VARBINARY(1024)");
    assertPrimitiveMapping(Type.STRING, "TEXT");
  }

  @Test
  public void shouldMapDecimalSchemaTypeToDecimalSqlType() {
    assertDecimalMapping(0, "DECIMAL(65,0)");
    assertDecimalMapping(3, "DECIMAL(65,3)");
    assertDecimalMapping(4, "DECIMAL(65,4)");
    assertDecimalMapping(5, "DECIMAL(65,5)");
  }

  @Test
  public void shouldMapDataTypes() {
    verifyDataTypeMapping("TINYINT", Schema.INT8_SCHEMA);
    verifyDataTypeMapping("SMALLINT", Schema.INT16_SCHEMA);
    verifyDataTypeMapping("INT", Schema.INT32_SCHEMA);
    verifyDataTypeMapping("BIGINT", Schema.INT64_SCHEMA);
    verifyDataTypeMapping("FLOAT", Schema.FLOAT32_SCHEMA);
    verifyDataTypeMapping("DOUBLE", Schema.FLOAT64_SCHEMA);
    verifyDataTypeMapping("TINYINT", Schema.BOOLEAN_SCHEMA);
    verifyDataTypeMapping("TEXT", Schema.STRING_SCHEMA);
    verifyDataTypeMapping("VARBINARY(1024)", Schema.BYTES_SCHEMA);
    verifyDataTypeMapping("DECIMAL(65,0)", Decimal.schema(0));
    verifyDataTypeMapping("DECIMAL(65,2)", Decimal.schema(2));
    verifyDataTypeMapping("DATE", Date.SCHEMA);
    verifyDataTypeMapping("TIME(3)", Time.SCHEMA);
    verifyDataTypeMapping("DATETIME(3)", Timestamp.SCHEMA);
  }

  @Test
  public void shouldMapDateSchemaTypeToDateSqlType() {
    assertDateMapping("DATE");
  }

  @Test
  public void shouldMapTimeSchemaTypeToTimeSqlType() {
    assertTimeMapping("TIME(3)");
  }

  @Test
  public void shouldMapTimestampSchemaTypeToTimestampSqlType() {
    assertTimestampMapping("DATETIME(3)");
  }

  @Test
  public void shouldBuildCreateQueryStatement() {
    String expected =
        "CREATE TABLE `myTable` (\n" + "`c1` INT NOT NULL,\n" + "`c2` BIGINT NOT NULL,\n" +
        "`c3` TEXT NOT NULL,\n" + "`c4` TEXT NULL,\n" +
        "`c5` DATE DEFAULT '2001-03-15',\n" + "`c6` TIME(3) DEFAULT '00:00:00.000',\n" +
        "`c7` DATETIME(3) DEFAULT '2001-03-15 00:00:00.000',\n" + "`c8` DECIMAL(65,4) NULL,\n" +
        "`c9` TINYINT DEFAULT 1,\n" +
        "PRIMARY KEY(`c1`))";
    String sql = dialect.buildCreateTableStatement(tableId, sinkRecordFields);
    assertEquals(expected, sql);
  }

  @Test
  public void shouldBuildAlterTableStatement() {
    List<String> statements = dialect.buildAlterTable(tableId, sinkRecordFields);
    String[] sql = {
        "ALTER TABLE `myTable` \n" + "ADD `c1` INT NOT NULL,\n" + "ADD `c2` BIGINT NOT NULL,\n" +
        "ADD `c3` TEXT NOT NULL,\n" + "ADD `c4` TEXT NULL,\n" +
        "ADD `c5` DATE DEFAULT '2001-03-15',\n" + "ADD `c6` TIME(3) DEFAULT '00:00:00.000',\n" +
        "ADD `c7` DATETIME(3) DEFAULT '2001-03-15 00:00:00.000',\n" +
        "ADD `c8` DECIMAL(65,4) NULL,\n" +
        "ADD `c9` TINYINT DEFAULT 1"};
    assertStatements(sql, statements);
  }

  @Test
  public void shouldBuildUpsertStatement() {
    String expected = "insert into `myTable`(`id1`,`id2`,`columnA`,`columnB`,`columnC`,`columnD`)" +
                      " values(?,?,?,?,?,?) on duplicate key update `columnA`=values(`columnA`)," +
                      "`columnB`=values(`columnB`),`columnC`=values(`columnC`),`columnD`=values" +
                      "(`columnD`)";
    String sql = dialect.buildUpsertQueryStatement(tableId, pkColumns, columnsAtoD);
    assertEquals(expected, sql);
  }

  @Test
  public void createOneColNoPk() {
    verifyCreateOneColNoPk(
        "CREATE TABLE `myTable` (" + System.lineSeparator() + "`col1` INT NOT NULL)");

    quoteIdentfiiers = QuoteMethod.NEVER;
    dialect = createDialect();

    verifyCreateOneColNoPk(
        "CREATE TABLE myTable (" + System.lineSeparator() + "col1 INT NOT NULL)");
  }

  @Test
  public void createOneColOnePk() {
    verifyCreateOneColOnePk(
        "CREATE TABLE `myTable` (" + System.lineSeparator() + "`pk1` INT NOT NULL," +
        System.lineSeparator() + "PRIMARY KEY(`pk1`))");
  }

  @Test
  public void createThreeColTwoPk() {
    verifyCreateThreeColTwoPk(
        "CREATE TABLE `myTable` (" + System.lineSeparator() + "`pk1` INT NOT NULL," +
        System.lineSeparator() + "`pk2` INT NOT NULL," + System.lineSeparator() +
        "`col1` INT NOT NULL," + System.lineSeparator() + "PRIMARY KEY(`pk1`,`pk2`))");

    quoteIdentfiiers = QuoteMethod.NEVER;
    dialect = createDialect();

    verifyCreateThreeColTwoPk(
        "CREATE TABLE myTable (" + System.lineSeparator() + "pk1 INT NOT NULL," +
        System.lineSeparator() + "pk2 INT NOT NULL," + System.lineSeparator() +
        "col1 INT NOT NULL," + System.lineSeparator() + "PRIMARY KEY(pk1,pk2))");
  }

  @Test
  public void alterAddOneCol() {
    verifyAlterAddOneCol("ALTER TABLE `myTable` ADD `newcol1` INT NULL");
  }

  @Test
  public void alterAddTwoCol() {
    verifyAlterAddTwoCols(
        "ALTER TABLE `myTable` " + System.lineSeparator() + "ADD `newcol1` INT NULL," +
        System.lineSeparator() + "ADD `newcol2` INT DEFAULT 42");
  }

  @Test
  public void upsert() {
    TableId actor = tableId("actor");
    String expected = "insert into `actor`(`actor_id`,`first_name`,`last_name`,`score`) " +
                      "values(?,?,?,?) on duplicate key update `first_name`=values(`first_name`)," +
                      "`last_name`=values(`last_name`),`score`=values(`score`)";
    String sql = dialect.buildUpsertQueryStatement(actor, columns(actor, "actor_id"),
                                                   columns(actor, "first_name", "last_name",
                                                           "score"));
    assertEquals(expected, sql);

    quoteIdentfiiers = QuoteMethod.NEVER;
    dialect = createDialect();

    expected = "insert into actor(actor_id,first_name,last_name,score) " +
               "values(?,?,?,?) on duplicate key update first_name=values(first_name)," +
               "last_name=values(last_name),score=values(score)";
    sql = dialect.buildUpsertQueryStatement(actor, columns(actor, "actor_id"),
        columns(actor, "first_name", "last_name",
            "score"));
    assertEquals(expected, sql);
  }

  @Test
  public void upsertOnlyKeyCols() {
    TableId actor = tableId("actor");
    String expected = "insert into `actor`(`actor_id`) " +
                      "values(?) on duplicate key update `actor_id`=values(`actor_id`)";
    String sql = dialect
        .buildUpsertQueryStatement(actor, columns(actor, "actor_id"), columns(actor));
    assertEquals(expected, sql);
  }

  @Test
  public void insert() {
    TableId customers = tableId("customers");
    String expected = "INSERT INTO `customers`(`age`,`firstName`,`lastName`) VALUES(?,?,?)";
    String sql = dialect.buildInsertStatement(customers, columns(customers),
                                              columns(customers, "age", "firstName", "lastName"));
    assertEquals(expected, sql);
  }

  @Test
  public void update() {
    TableId customers = tableId("customers");
    String expected =
        "UPDATE `customers` SET `age` = ?, `firstName` = ?, `lastName` = ? WHERE " + "`id` = ?";
    String sql = dialect.buildUpdateStatement(customers, columns(customers, "id"),
                                              columns(customers, "age", "firstName", "lastName"));
    assertEquals(expected, sql);
  }

  @Test
  public void shouldSanitizeUrlWithCredentialsInHosts() {
    assertSanitizedUrl(
        "mysqlx://sandy:secret@(host=myhost1,port=1111)/db?key1=value1",
        "mysqlx://sandy:****@(host=myhost1,port=1111)/db?key1=value1"
    );
  }

  @Test
  public void shouldSanitizeUrlWithCredentialsInProperties() {
    assertSanitizedUrl(
        "jdbc:mysql://[(host=myhost1,port=1111,user=sandy,password=secret),"
        + "(password=secret,host=myhost2,port=2222,user=finn,password=secret)]/db",
        "jdbc:mysql://[(host=myhost1,port=1111,user=sandy,password=****),"
        + "(password=****,host=myhost2,port=2222,user=finn,password=****)]/db"
    );
  }

  @Test
  public void shouldSanitizeUrlWithCredentialsInUrlProperties() {
    assertSanitizedUrl(
        "jdbc:mysql://(host=myhost1,port=1111),(host=myhost2,port=2222)/"
        + "db?password=secret&key1=value1&key2=value2&key3=value3&"
        + "user=smith&password=secret&other=value",
        "jdbc:mysql://(host=myhost1,port=1111),(host=myhost2,port=2222)/"
        + "db?password=****&key1=value1&key2=value2&key3=value3&"
        + "user=smith&password=****&other=value"
    );
  }

  // validateQuery behaviour is inherited from GenericDatabaseDialect and exercised in
  // GenericDatabaseDialectTest; no MySQL-specific override exists to test here.

  // ----- Security: blocked JDBC URL parameters — query string -----

  @Test
  public void shouldRejectAllowLoadLocalInfileInUrl() {
    assertBlockedInUrl("allowLoadLocalInfile=true");
  }

  @Test
  public void shouldRejectAllowLocalInfileInUrl() {
    assertBlockedInUrl("allowLocalInfile=true");
  }

  @Test
  public void shouldRejectAllowUrlInLocalInfileInUrl() {
    assertBlockedInUrl("allowUrlInLocalInfile=true");
  }

  @Test
  public void shouldRejectAllowLoadLocalInfileInPathInUrl() {
    assertBlockedInUrl("allowLoadLocalInfileInPath=/");
  }

  @Test
  public void shouldRejectClassLoadingPropertiesInUrl() {
    assertBlockedInUrl("socketFactory=com.example.Evil");
    assertBlockedInUrl("authenticationPlugins=com.example.Evil");
    assertBlockedInUrl("defaultAuthenticationPlugin=com.example.Evil");
    assertBlockedInUrl("clientInfoProvider=com.example.Evil");
    assertBlockedInUrl("propertiesTransform=com.example.Evil");
    assertBlockedInUrl("serverRSAPublicKeyFile=/etc/passwd");
  }

  @Test
  public void shouldRejectAutoDeserializeInUrl() {
    assertBlockedInUrl("autoDeserialize=true");
  }

  @Test
  public void shouldRejectQueryInterceptorsInUrl() {
    assertBlockedInUrl(
        "queryInterceptors=com.mysql.cj.jdbc.interceptors.ServerStatusDiffInterceptor");
  }

  @Test
  public void shouldRejectStatementInterceptorsInUrl() {
    assertBlockedInUrl("statementInterceptors=com.example.Evil");
  }

  @Test
  public void shouldRejectAllowMultiQueriesInUrl() {
    assertBlockedInUrl("allowMultiQueries=true");
  }

  @Test
  public void shouldRejectBlockedParamCaseInsensitiveInUrl() {
    // Driver normalises property names case-insensitively; our check must too
    assertBlockedInUrl("ALLOWLOADLOCALINFILE=true");
    assertBlockedInUrl("AutoDeserialize=true");
    assertBlockedInUrl("allowMULTIqueries=true");
  }

  @Test
  public void shouldRejectUrlEncodedBlockedParamInUrl() {
    // Driver URL-decodes %xx before applying properties; e.g. allow%4CoadLocalInfile == allowLoadLocalInfile
    assertBlockedInUrl("allow%4CoadLocalInfile=true");   // %4C = L
    assertBlockedInUrl("autoDeseriali%7Ae=true");        // %7A = z
  }

  @Test
  public void shouldRejectBlockedParamAmongOthersInUrl() {
    MySqlDatabaseDialect d = new MySqlDatabaseDialect(
        sourceConfigWithUrl("jdbc:mysql://host:3306/db"));
    assertThrows(ConnectException.class, () -> d.validateJdbcUrlParams(
        "jdbc:mysql://host:3306/db?useSSL=true&allowLoadLocalInfile=true&charset=utf8"));
  }

  @Test
  public void shouldRejectAmpersandBeforeQueryStringInUrl() {
    // connection.host="evil.host&allowLoadLocalInfile=true" produces & before ?
    MySqlDatabaseDialect d = new MySqlDatabaseDialect(
        sourceConfigWithUrl("jdbc:mysql://host:3306/db"));
    assertThrows(ConnectException.class, () -> d.validateJdbcUrlParams(
        "jdbc:mysql://evil.host&allowLoadLocalInfile=true:3306/db"));
  }

  @Test
  public void shouldRejectFragmentInUrl() {
    MySqlDatabaseDialect d = new MySqlDatabaseDialect(
        sourceConfigWithUrl("jdbc:mysql://host:3306/db"));
    assertThrows(ConnectException.class, () -> d.validateJdbcUrlParams(
        "jdbc:mysql://host:3306/db#fragment"));
  }

  // ----- Security: blocked params in authority property bags -----

  @Test
  public void shouldRejectAllowLoadLocalInfileInAuthorityBag() {
    // jdbc:mysql://(host=h,port=p,allowLoadLocalInfile=true)/db bypasses ?-query parsing
    MySqlDatabaseDialect d = new MySqlDatabaseDialect(
        sourceConfigWithUrl("jdbc:mysql://host:3306/db"));
    assertThrows(ConnectException.class, () -> d.validateJdbcUrlParams(
        "jdbc:mysql://(host=myhost,port=3306,allowLoadLocalInfile=true)/db"));
  }

  @Test
  public void shouldRejectAutoDeserializeInAuthorityBag() {
    MySqlDatabaseDialect d = new MySqlDatabaseDialect(
        sourceConfigWithUrl("jdbc:mysql://host:3306/db"));
    assertThrows(ConnectException.class, () -> d.validateJdbcUrlParams(
        "jdbc:mysql://[(host=myhost,port=3306,autoDeserialize=true)]/db"));
  }

  @Test
  public void shouldRejectBlockedParamCaseInsensitiveInAuthorityBag() {
    MySqlDatabaseDialect d = new MySqlDatabaseDialect(
        sourceConfigWithUrl("jdbc:mysql://host:3306/db"));
    assertThrows(ConnectException.class, () -> d.validateJdbcUrlParams(
        "jdbc:mysql://(host=myhost,port=3306,ALLOWLOADLOCALINFILE=true)/db"));
  }

  @Test
  public void shouldAllowSafeAuthorityBag() {
    MySqlDatabaseDialect d = new MySqlDatabaseDialect(
        sourceConfigWithUrl("jdbc:mysql://host:3306/db"));
    // Legitimate multi-host URL with safe properties in authority bags
    d.validateJdbcUrlParams(
        "jdbc:mysql://[(host=myhost1,port=3306,user=alice),(host=myhost2,port=3306)]/db");
  }

  @Test
  public void shouldAllowSafeUrlParams() {
    MySqlDatabaseDialect d = new MySqlDatabaseDialect(
        sourceConfigWithUrl("jdbc:mysql://host:3306/db?useSSL=true&charset=utf8"));
    d.validateJdbcUrlParams("jdbc:mysql://host:3306/db?useSSL=true&charset=utf8");
  }

  @Test
  public void shouldAllowUrlWithNoParams() {
    MySqlDatabaseDialect d = new MySqlDatabaseDialect(
        sourceConfigWithUrl("jdbc:mysql://host:3306/db"));
    d.validateJdbcUrlParams("jdbc:mysql://host:3306/db");
  }

  @Test
  public void shouldAllowNullUrl() {
    MySqlDatabaseDialect d = new MySqlDatabaseDialect(
        sourceConfigWithUrl("jdbc:mysql://host:3306/db"));
    // null url must not throw NPE — caller may pass null when url is not yet set
    d.validateJdbcUrlParams(null);
  }

  // ----- Security: blocked connection.* properties -----

  @Test
  public void shouldRejectAllowLoadLocalInfileAsConnectionProperty() {
    assertBlockedAsConnectionProperty("connection.allowLoadLocalInfile", "true");
  }

  @Test
  public void shouldRejectAllowLocalInfileAsConnectionProperty() {
    assertBlockedAsConnectionProperty("connection.allowLocalInfile", "true");
  }

  @Test
  public void shouldRejectAllowUrlInLocalInfileAsConnectionProperty() {
    assertBlockedAsConnectionProperty("connection.allowUrlInLocalInfile", "true");
  }

  @Test
  public void shouldRejectAutoDeserializeAsConnectionProperty() {
    assertBlockedAsConnectionProperty("connection.autoDeserialize", "true");
  }

  @Test
  public void shouldRejectQueryInterceptorsAsConnectionProperty() {
    assertBlockedAsConnectionProperty("connection.queryInterceptors",
        "com.mysql.cj.jdbc.interceptors.ServerStatusDiffInterceptor");
  }

  @Test
  public void shouldRejectStatementInterceptorsAsConnectionProperty() {
    assertBlockedAsConnectionProperty("connection.statementInterceptors", "com.example.Evil");
  }

  @Test
  public void shouldRejectAllowMultiQueriesAsConnectionProperty() {
    assertBlockedAsConnectionProperty("connection.allowMultiQueries", "true");
  }

  @Test
  public void shouldRejectAllowLoadLocalInfileInPathAsConnectionProperty() {
    assertBlockedAsConnectionProperty("connection.allowLoadLocalInfileInPath", "/");
  }

  @Test
  public void shouldRejectSocketFactoryAsConnectionProperty() {
    assertBlockedAsConnectionProperty("connection.socketFactory", "com.example.Evil");
  }

  @Test
  public void shouldRejectAuthenticationPluginsAsConnectionProperty() {
    assertBlockedAsConnectionProperty("connection.authenticationPlugins", "com.example.Evil");
  }

  @Test
  public void shouldRejectDefaultAuthenticationPluginAsConnectionProperty() {
    assertBlockedAsConnectionProperty("connection.defaultAuthenticationPlugin", "com.example.Evil");
  }

  @Test
  public void shouldRejectClientInfoProviderAsConnectionProperty() {
    assertBlockedAsConnectionProperty("connection.clientInfoProvider", "com.example.Evil");
  }

  @Test
  public void shouldRejectPropertiesTransformAsConnectionProperty() {
    assertBlockedAsConnectionProperty("connection.propertiesTransform", "com.example.Evil");
  }

  @Test
  public void shouldRejectServerRsaPublicKeyFileAsConnectionProperty() {
    assertBlockedAsConnectionProperty("connection.serverRSAPublicKeyFile", "/etc/passwd");
  }

  @Test
  public void shouldRejectBlockedPropertyCaseInsensitiveAsConnectionProperty() {
    // Kafka configs arrive with the casing the user typed; driver accepts any casing
    assertBlockedAsConnectionProperty("connection.ALLOWLOADLOCALINFILE", "true");
    assertBlockedAsConnectionProperty("connection.AutoDeserialize", "true");
  }

  @Test
  public void shouldAllowSafeConnectionProperties() {
    Map<String, String> props = baseConnProps("jdbc:mysql://host:3306/db");
    props.put("connection.useSSL", "true");
    props.put("connection.characterEncoding", "utf8");
    MySqlDatabaseDialect d = new MySqlDatabaseDialect(new JdbcSourceConnectorConfig(props));
    Properties result = d.addConnectionProperties(new Properties());
    assertEquals("true", result.get("useSSL"));
    assertEquals("utf8", result.get("characterEncoding"));
  }

  // ----- Security: safe defaults are pinned regardless of driver defaults -----

  @Test
  public void shouldPinAllowLoadLocalInfileToFalse() {
    MySqlDatabaseDialect d = new MySqlDatabaseDialect(
        new JdbcSourceConnectorConfig(baseConnProps("jdbc:mysql://host:3306/db")));
    Properties result = d.addConnectionProperties(new Properties());
    assertEquals("false", result.getProperty("allowLoadLocalInfile"));
  }

  @Test
  public void shouldPinAllowLocalInfileToFalse() {
    MySqlDatabaseDialect d = new MySqlDatabaseDialect(
        new JdbcSourceConnectorConfig(baseConnProps("jdbc:mysql://host:3306/db")));
    Properties result = d.addConnectionProperties(new Properties());
    assertEquals("false", result.getProperty("allowLocalInfile"));
  }

  @Test
  public void shouldPinAllowUrlInLocalInfileToFalse() {
    MySqlDatabaseDialect d = new MySqlDatabaseDialect(
        new JdbcSourceConnectorConfig(baseConnProps("jdbc:mysql://host:3306/db")));
    Properties result = d.addConnectionProperties(new Properties());
    assertEquals("false", result.getProperty("allowUrlInLocalInfile"));
  }

  @Test
  public void shouldPinAutoDeserializeToFalse() {
    MySqlDatabaseDialect d = new MySqlDatabaseDialect(
        new JdbcSourceConnectorConfig(baseConnProps("jdbc:mysql://host:3306/db")));
    Properties result = d.addConnectionProperties(new Properties());
    assertEquals("false", result.getProperty("autoDeserialize"));
  }

  // ----- helpers -----

  private void assertBlockedInUrl(String param) {
    String url = "jdbc:mysql://host:3306/db?" + param;
    MySqlDatabaseDialect d = new MySqlDatabaseDialect(sourceConfigWithUrl("jdbc:mysql://host:3306/db"));
    assertThrows(ConnectException.class, () -> d.validateJdbcUrlParams(url));
  }

  private void assertBlockedAsConnectionProperty(String key, String value) {
    Map<String, String> props = baseConnProps("jdbc:mysql://host:3306/db");
    props.put(key, value);
    MySqlDatabaseDialect d = new MySqlDatabaseDialect(new JdbcSourceConnectorConfig(props));
    assertThrows(ConnectException.class, () -> d.addConnectionProperties(new Properties()));
  }

  private Map<String, String> baseConnProps(String url) {
    Map<String, String> props = new HashMap<>();
    props.put(JdbcSourceConnectorConfig.CONNECTION_URL_CONFIG, url);
    props.put(JdbcSourceConnectorConfig.MODE_CONFIG, JdbcSourceConnectorConfig.MODE_BULK);
    props.put(JdbcSourceConnectorConfig.TOPIC_PREFIX_CONFIG, "test-");
    return props;
  }
}
