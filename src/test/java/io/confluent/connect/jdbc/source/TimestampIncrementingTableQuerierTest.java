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
    TimeZone.setDefault(null);
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
    derbySdf.setTimeZone(TimeZone.getTimeZone("UTC"));
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
    db.createTable("parent", "id", "INT NOT NULL", "parent_val", "VARCHAR(20)", "creation_ts", "TIMESTAMP NOT NULL", "update_ts", "TIMESTAMP");
    db.createTable("child", "id", "INT NOT NULL", "child_val", "VARCHAR(20)");
    SimpleDateFormat derbySdf = new SimpleDateFormat("yyyy-MM-dd-HH.mm.ss.SSS");
    derbySdf.setTimeZone(TimeZone.getTimeZone("UTC"));
    long now = new Date().getTime();
    String creationTs = derbySdf.format(now - (60000*30));
    String updateTs = derbySdf.format(now - (60000*5));
    db.insert("parent", "id", 1, "parent_val", "val1", "creation_ts", creationTs);
    db.insert("parent", "id", 2, "parent_val", "val2",
        "creation_ts", creationTs, "update_ts", updateTs);
    db.insert("child", "id", 1, "child_val", "val3");
    db.insert("child", "id", 2, "child_val", "val4");
    final String inlineViewName= "V_test";
    final String inlineViewDefinition = "(SELECT \"parent\".\"id\", \"parent\".\"parent_val\", \"child\".\"child_val\","
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
    int id = struct.getInt32("id");
    String parentVal = struct.getString("parent_val");
    String childVal = struct.getString("child_val");
    String ts = derbySdf.format(((java.sql.Timestamp)struct.get("ts")).getTime());
    assertEquals(1, id);
    assertEquals("val1", parentVal);
    assertEquals("val3", childVal);
    assertEquals(ts, creationTs);
    
    rs.next();
    struct = DataConverter.convertRecord(DataConverter.convertSchema(inlineViewName, rs.getMetaData(),
        false), rs, false, null);
    id = struct.getInt32("id");
    parentVal = struct.getString("parent_val");
    childVal = struct.getString("child_val");
    ts = derbySdf.format(((java.sql.Timestamp)struct.get("ts")).getTime());
    assertEquals(2, id);
    assertEquals("val2", parentVal);
    assertEquals("val4", childVal);
    assertEquals(ts, updateTs);
  }
  
  @Test
  public void testConvertValuesFromDbColumnTypeTimeStampWithoutTimezone() throws Exception {
    // This test is an attempt to show that when dealing with SQL types similar to
    // 'timestamp without timezone', in a DB running with a timezone different than UTC,
    // a Calendar may be necessary to retrieve and store the correct value in Kafka.
    String tableName = "test1";
    int timeShift = 5;
    String dbTimezone = "GMT-" + timeShift;
    // We simulate a database clock running in a timezone different than UTC.
    TimeZone.setDefault(TimeZone.getTimeZone(dbTimezone));
    SimpleDateFormat derbySdf = new SimpleDateFormat("yyyy-MM-dd-HH.mm.ss.SSS");
    long dbTime = new Date().getTime();
    String dbTimestampWithoutTimezone = derbySdf.format(dbTime);
    db.createTable(tableName, "id", "INT NOT NULL", "ts", "TIMESTAMP NOT NULL");
    db.insert(tableName, "id", 1, "ts", dbTimestampWithoutTimezone);
    
    ResultSet rs = db.getConnection().createStatement().executeQuery("SELECT * FROM \"" + tableName + "\"");
    rs.next();
    long timeWrittenInTable = rs.getTimestamp("ts").getTime();
    System.out.println("timeWrittenInTable: " + timeWrittenInTable);
    assertEquals(dbTime, timeWrittenInTable);
    
    // The Calendar used to retrieve the time is not the same than the one used to store the value.
    Timestamp dbTimeUTC = JdbcUtils.getCurrentTimeOnDB(cachedConnectionProvider.getValidConnection(),
        DateTimeUtils.UTC_CALENDAR.get());
    // Here it is the same.
    Timestamp dbTimeDbTimeZone = JdbcUtils.getCurrentTimeOnDB(cachedConnectionProvider.getValidConnection(),
        DateTimeUtils.getCalendarWithTimeZone(dbTimezone));
    Timestamp dbTimeJvmTimeZone = JdbcUtils.getCurrentTimeOnDB(cachedConnectionProvider.getValidConnection(),
        null);
    
    assertTrue(dbTimeUTC.getTime() < dbTimeJvmTimeZone.getTime());
    SimpleDateFormat compareHourSdf = new SimpleDateFormat("yyyy-MM-dd-HH.mm");
    assertEquals(compareHourSdf.format(dbTimeDbTimeZone), compareHourSdf.format(dbTimeJvmTimeZone));
    
    // Here the querier will use UTC calendar to get current time on db and determine query window end time
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
    
    rs = querier.executeQuery();
    rs.next();
    struct = DataConverter.convertRecord(DataConverter.convertSchema(tableName, rs.getMetaData(),
        false), rs, false, dbTimezone);
    Timestamp ts2 = (Timestamp)struct.get("ts");
    
    // By transitivity, data converted with default Calendar (UTC) gives incorrect time values
    // relative to database timezone
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
