/**
 * Copyright 2016 Confluent Inc.
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
 **/

package io.confluent.connect.jdbc.source;

import org.apache.kafka.connect.data.Decimal;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.ConnectException;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import io.confluent.connect.jdbc.util.CachedConnectionProvider;
import io.confluent.connect.jdbc.util.DateTimeUtils;
import io.confluent.connect.jdbc.util.JdbcUtils;

import java.math.BigDecimal;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.sql.Types;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.TimeZone;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;

public class TimestampIncrementingTableQuerierTest {
  
  private EmbeddedDerby db;
  private CachedConnectionProvider cachedConnectionProvider;
  
  @Before
  public void setup() {
    db = new EmbeddedDerby();
    cachedConnectionProvider = new CachedConnectionProvider(db.getUrl());
  }
  
  @After
  public void tearDown() throws Exception {
    db.close();
    db.dropDatabase();
  }

  @Test
  public void extractIntOffset() throws SQLException {
    final Schema schema = SchemaBuilder.struct().field("id", SchemaBuilder.INT32_SCHEMA).build();
    final Struct record = new Struct(schema).put("id", 42);
    assertEquals(42L, newQuerier().extractOffset(schema, record).getIncrementingOffset());
  }

  @Test
  public void extractLongOffset() throws SQLException {
    final Schema schema = SchemaBuilder.struct().field("id", SchemaBuilder.INT64_SCHEMA).build();
    final Struct record = new Struct(schema).put("id", 42L);
    assertEquals(42L, newQuerier().extractOffset(schema, record).getIncrementingOffset());
  }

  @Test
  public void extractDecimalOffset() throws SQLException {
    final Schema decimalSchema = Decimal.schema(0);
    final Schema schema = SchemaBuilder.struct().field("id", decimalSchema).build();
    final Struct record = new Struct(schema).put("id", new BigDecimal(42));
    assertEquals(42L, newQuerier().extractOffset(schema, record).getIncrementingOffset());
  }

  @Test(expected = ConnectException.class)
  public void extractTooLargeDecimalOffset() throws SQLException {
    final Schema decimalSchema = Decimal.schema(0);
    final Schema schema = SchemaBuilder.struct().field("id", decimalSchema).build();
    final Struct record = new Struct(schema).put("id", new BigDecimal(Long.MAX_VALUE).add(new BigDecimal(1)));
    newQuerier().extractOffset(schema, record).getIncrementingOffset();
  }

  @Test(expected = ConnectException.class)
  public void extractFractionalDecimalOffset() throws SQLException {
    final Schema decimalSchema = Decimal.schema(2);
    final Schema schema = SchemaBuilder.struct().field("id", decimalSchema).build();
    final Struct record = new Struct(schema).put("id", new BigDecimal("42.42"));
    newQuerier().extractOffset(schema, record).getIncrementingOffset();
  }

  @Test
  public void testCreatePreparedStatemenInTimeStampMode() throws Exception {
    String tableName = "test1";
    SimpleDateFormat derbySdf = new SimpleDateFormat("yyyy-MM-dd-HH.mm.ss");
    int pastOffsetInMinutes = 10;
    String dbDateUTC = derbySdf.format(new Date().getTime() - (60000*pastOffsetInMinutes));
    db.createTable(tableName, "id", "INT NOT NULL", "ts", "TIMESTAMP NOT NULL");
    db.insert(tableName, "id", 1, "ts", dbDateUTC);
    TimestampIncrementingTableQuerier querier =
        newQuerier(TableQuerier.QueryMode.TABLE, tableName, "ts", null, "UTC", null);
    querier.createPreparedStatement(cachedConnectionProvider.getValidConnection());
    ResultSet rs = querier.executeQuery();
    rs.next();
    int id = rs.getInt("id");
    String ts = derbySdf.format(rs.getTimestamp("ts", DateTimeUtils.UTC_CALENDAR.get()));
    assertEquals(1, id);
    assertEquals(ts, dbDateUTC); 
  }
  
  @Test
  public void testCreatePreparedStatemenInTimeStampModeForInlineView() throws Exception {
    db.createTable("parent", "id", "INT NOT NULL", "foo", "VARCHAR(20)", "creation_ts", "TIMESTAMP NOT NULL", "update_ts", "TIMESTAMP");
    db.createTable("child", "id", "INT NOT NULL", "bar", "VARCHAR(20)");
    SimpleDateFormat derbySdf = new SimpleDateFormat("yyyy-MM-dd-HH.mm.ss.SSS");
    long now = new Date().getTime();
    String creationTs = derbySdf.format(now - (60000*30));
    String updateTs = derbySdf.format(now - (60000*5));
    System.out.println("now: " + now);
    System.out.println("creationTs: " + creationTs);
    System.out.println("updateTs: " + updateTs);
    db.insert("parent", "id", 1, "foo", "fooVal1", "creation_ts", creationTs);
    db.insert("parent", "id", 2, "foo", "fooVal2",
        "creation_ts", creationTs, "update_ts", updateTs);
    db.insert("child", "id", 1, "bar", "barVal1");
    db.insert("child", "id", 2, "bar", "barVal2");
    final String inlineViewName= "V_test";
    final String inlineViewDefinition = "(SELECT \"parent\".\"id\", \"parent\".\"foo\", \"child\".\"bar\","
        + "coalesce(\"parent\".\"update_ts\", \"parent\".\"creation_ts\") AS \"ts\" "
        + "FROM \"parent\" "
        + "JOIN \"child\" ON \"child\".\"id\" = \"parent\".\"id\""
        + "ORDER BY \"parent\".\"id\" DESC)";
    
    TimestampIncrementingTableQuerier querier =
        newQuerier(TableQuerier.QueryMode.TABLE, inlineViewName, "ts", null, "UTC", inlineViewDefinition);
    querier.createPreparedStatement(cachedConnectionProvider.getValidConnection());
    ResultSet rs = querier.executeQuery();
    rs.next();
   
    Struct struct = DataConverter.convertRecord(DataConverter.convertSchema(inlineViewName, rs.getMetaData(),
        false), rs, false, null);
    System.out.println("struct: " + struct);
    int id = rs.getInt("id");
    String foo = rs.getString("foo");
    String bar = rs.getString("bar");
    String ts = derbySdf.format(rs.getTimestamp("ts"));
    assertEquals(1, id);
    assertEquals("fooVal1", foo);
    assertEquals("barVal1", bar);
    assertEquals(ts, creationTs);
    
    rs.next();
    struct = DataConverter.convertRecord(DataConverter.convertSchema(inlineViewName, rs.getMetaData(),
        false), rs, false, null);
    System.out.println("struct: " + struct);
    id = rs.getInt("id");
    foo = rs.getString("foo");
    bar = rs.getString("bar");
    ts = derbySdf.format(rs.getTimestamp("ts"));
    assertEquals(2, id);
    assertEquals("fooVal2", foo);
    assertEquals("barVal2", bar);
    assertEquals(ts, updateTs);
  }
  
  @Test
  public void testConvertValuesFromDbColumnTypeTimeStampWithoutTimezone() throws Exception {
    // This test is an attempt to show that when dealing with SQL types similar to
    // 'timestamp without timezone', in a DB running with a timezone different than UTC,
    // a calendar may be necessary to retrieve and store the correct value in Kafka.
    String tableName = "test1";
    int timeShift = 5;
    String dbTimezone = "GMT-" + timeShift;
    // here we simulate a database clock running in timezone different than UTC
    TimeZone.setDefault(TimeZone.getTimeZone(dbTimezone));
    SimpleDateFormat derbySdf = new SimpleDateFormat("yyyy-MM-dd-HH.mm.ss.SSS");
    long dbTime = new Date().getTime();
    System.out.println("dbTime: " + dbTime);
    String dbTimestampWithoutTimezone = derbySdf.format(dbTime);
    System.out.println("dbTimestampWithoutTimezone: " + dbTimestampWithoutTimezone);
    db.createTable(tableName, "id", "INT NOT NULL", "ts", "TIMESTAMP NOT NULL");
    db.insert(tableName, "id", 1, "ts", dbTimestampWithoutTimezone);
    
    ResultSet rs = db.getConnection().createStatement().executeQuery("SELECT * FROM \"" + tableName + "\"");
    rs.next();
    long timeWrittenInTable = rs.getTimestamp("ts").getTime();
    System.out.println("timeWrittenInTable: " + timeWrittenInTable);
    assertEquals(dbTime, timeWrittenInTable);
    
    Timestamp dbTimeUTC = JdbcUtils.getCurrentTimeOnDB(cachedConnectionProvider.getValidConnection(),
        DateTimeUtils.UTC_CALENDAR.get());
    Timestamp dbTimeDbTimeZone = JdbcUtils.getCurrentTimeOnDB(cachedConnectionProvider.getValidConnection(),
        DateTimeUtils.getCalendarWithTimeZone(dbTimezone));
    Timestamp dbTimeJvmTimeZone = JdbcUtils.getCurrentTimeOnDB(cachedConnectionProvider.getValidConnection(),
        null);
    
    System.out.println("dbTimeUTC: " + dbTimeUTC);
    System.out.println("dbTimeDbTimeZone: " + dbTimeDbTimeZone);
    System.out.println("dbTimeJvmTimeZone: " + dbTimeJvmTimeZone);
    System.out.println("dbTimeUTCTimezoneOffset: " + dbTimeUTC.getTimezoneOffset());
    System.out.println("dbTimeDbTimeZoneTimezoneOffset: " + dbTimeDbTimeZone.getTimezoneOffset());
    System.out.println("dbTimeJvmTimeZoneTimezoneOffset: " + dbTimeJvmTimeZone.getTimezoneOffset());
    System.out.println("dbTimeUTCTime " + dbTimeUTC.getTime());
    System.out.println("dbTimeDbTime: " + dbTimeDbTimeZone.getTime());
    System.out.println("dbTimeJvmTime: " + dbTimeJvmTimeZone.getTime());
    assertTrue(dbTimeUTC.getTime() < dbTimeJvmTimeZone.getTime());
    assertEquals(dbTimeDbTimeZone.getTime(), dbTimeJvmTimeZone.getTime());
    
    // here the querier will use UTC calendar to get current time on db and determine query window end time
    // it works even if previous test shown that db time get with UTC Calendar is lower than the one
    // get with the jvm time zone. Anyway this doesn't change the retrieved value since only the window
    // end time is concerned. 
    TimestampIncrementingTableQuerier querier =
        newQuerier(TableQuerier.QueryMode.TABLE, tableName, "ts", null, "UTC", null);
    querier.createPreparedStatement(cachedConnectionProvider.getValidConnection());
    rs = querier.executeQuery();
    rs.next();
    // But DataConverter uses a Calendar to construct a timestamp and send it to Kafka.
    // In this case, we can store incorrect timestamps in Kafka depending on the Calendar.
    Struct struct = DataConverter.convertRecord(DataConverter.convertSchema(tableName, rs.getMetaData(),
        false), rs, false, "UTC");
    Timestamp ts1 = (Timestamp)struct.get("ts");
    System.out.println("ts 1: " + ts1.getTime());
    
    rs = querier.executeQuery();
    rs.next();
    struct = DataConverter.convertRecord(DataConverter.convertSchema(tableName, rs.getMetaData(),
        false), rs, false, dbTimezone);
    Timestamp ts2 = (Timestamp)struct.get("ts");
    System.out.println("ts 2: " + ts2.getTime());
    
    assertNotEquals(ts1.getTime(), ts2.getTime());
    assertEquals(dbTime, ts2.getTime());
  }
  
  private TimestampIncrementingTableQuerier newQuerier() {
    return new TimestampIncrementingTableQuerier(TableQuerier.QueryMode.TABLE, null, "", null, "id", Collections.<String, Object>emptyMap(), 0L, null, false, null, null);
  }
  
  private TimestampIncrementingTableQuerier newQuerier(TableQuerier.QueryMode mode, String name, String timestampColumn, String incrementingColumn, String timezone, String inlineViewDefinition) {
    return new TimestampIncrementingTableQuerier(mode, name, "", timestampColumn, incrementingColumn, Collections.<String, Object>emptyMap(), 0L, null, false, timezone, inlineViewDefinition);
  }

}
