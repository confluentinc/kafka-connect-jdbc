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
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnit;
import org.mockito.junit.MockitoRule;

import javax.sql.rowset.RowSetMetaDataImpl;
import java.math.BigDecimal;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Collections;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.when;

public class TimestampIncrementingTableQuerierTest {
  public static final String TABLE_NAME = "myname";
  public static final String TOPIC_PREFIX = "topic-prefix-";
  public static final String PARTITION_NAME = "partition";
  public static final int PARTITION_NUMBER_VALUE = 12;
  @Rule
  public MockitoRule rule = MockitoJUnit.rule();
  @Mock
  private ResultSet resultSet;

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
  public void getSourceOffsetNominalCase() {
    final Schema decimalSchema = Decimal.schema(0);
    final Schema schema = SchemaBuilder.struct().field("id", decimalSchema).build();
    final Struct record = new Struct(schema).put("id", new BigDecimal(42));
    Map<String, ?> sourceOffset = newQuerier().getSourceOffset(schema, record);
    TimestampIncrementingOffset expected = new TimestampIncrementingOffset(null, 42L);
    assertEquals(expected.toMap(), sourceOffset);
  }

  @Test
  public void getSourceOffsetNullValues() {
    Map<String, ?> sourceOffset = newQuerier().getSourceOffset(null, null);
    TimestampIncrementingOffset offset = new TimestampIncrementingOffset(null, null);
    assertEquals(offset.toMap(), sourceOffset);
  }

  @Test
  public void extractRecordsNominalCase() throws SQLException {
    TableQuerier querier = newQuerierWithPartitionValue(PARTITION_NUMBER_VALUE);

    SourceRecord sourceRecord = querier.extractRecord();
    Schema schema = SchemaBuilder.struct().build();
    Struct struct = new Struct(schema);
    SourceRecord expected = new SourceRecord(Collections.singletonMap(JdbcSourceConnectorConstants.TABLE_NAME_KEY, TABLE_NAME), Collections.EMPTY_MAP, TOPIC_PREFIX + TABLE_NAME, PARTITION_NUMBER_VALUE, schema, struct);

    assertEquals(expected, sourceRecord);

  }

  @Test(expected = ConnectException.class)
  public void extractRecordsWithNegativeValue() throws SQLException {
    TableQuerier querier = newQuerierWithPartitionValue(-1);
    querier.extractRecord();

  }

  private TableQuerier newQuerierWithPartitionValue(int partitionNumberValue) throws SQLException {
    TableQuerier querier = new TimestampIncrementingTableQuerier(TableQuerier.QueryMode.TABLE, TABLE_NAME, TOPIC_PREFIX, null, "id", Collections.<String, Object>emptyMap(), 0L, null, partitionNumberValue);
    querier.resultSet = this.resultSet;
    querier.schema =  SchemaBuilder.struct().build();
    RowSetMetaDataImpl resultSetMetaData = new RowSetMetaDataImpl();
    when(resultSet.getMetaData()).thenReturn(resultSetMetaData);
    return querier;
  }


  private TimestampIncrementingTableQuerier newQuerier() {
    return new TimestampIncrementingTableQuerier(TableQuerier.QueryMode.TABLE, null, "", null, "id", Collections.<String, Object>emptyMap(), 0L, null, null);
  }

}
