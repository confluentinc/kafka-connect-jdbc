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
import org.junit.Before;
import org.junit.Test;

import io.confluent.connect.jdbc.util.CachedConnectionProvider;
import io.confluent.connect.jdbc.util.DateTimeUtils;

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

public class TimestampIncrementingTableQuerierTest {
  
  private EmbeddedDerby db;
  private CachedConnectionProvider cachedConnectionProvider;
  
  @Before
  public void setup() {
    db = new EmbeddedDerby();
    cachedConnectionProvider = new CachedConnectionProvider(db.getUrl());
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
  public void tesCreatePreparedStatemenInTimeStampMode() throws Exception {
    String tableName = "test1";
    TimeZone.setDefault(TimeZone.getTimeZone("UTC"));
    SimpleDateFormat derbySdf = new SimpleDateFormat("yyyy-MM-dd-HH.mm.ss");
    derbySdf.setTimeZone(TimeZone.getTimeZone("UTC"));
    int pastOffsetInMinutes = 10;
    String dbDateUTC = derbySdf.format(new Date().getTime() - (60000*pastOffsetInMinutes));
    db.createTable(tableName, "id", "INT NOT NULL", "ts", "TIMESTAMP NOT NULL");
    db.insert(tableName, "id", 1, "ts", dbDateUTC);
    TimestampIncrementingTableQuerier querier =
        newQuerier(TableQuerier.QueryMode.TABLE, tableName, "ts", null, "UTC");
    querier.createPreparedStatement(cachedConnectionProvider.getValidConnection());
    ResultSet rs = querier.executeQuery();
    
    rs.next();
    int id = rs.getInt("id");
    String ts = derbySdf.format(rs.getTimestamp("ts", DateTimeUtils.UTC_CALENDAR.get()));

    assertEquals(1, id);
    assertEquals(ts, dbDateUTC);
    
  }
  
  private TimestampIncrementingTableQuerier newQuerier() {
    return new TimestampIncrementingTableQuerier(TableQuerier.QueryMode.TABLE, null, "", null, "id", Collections.<String, Object>emptyMap(), 0L, null, false, null, null);
  }
  
  private TimestampIncrementingTableQuerier newQuerier(TableQuerier.QueryMode mode, String name, String timestampColumn, String incrementingColumn, String timezone) {
    return new TimestampIncrementingTableQuerier(mode, name, "", timestampColumn, incrementingColumn, Collections.<String, Object>emptyMap(), 0L, null, false, timezone, null);
  }

}
