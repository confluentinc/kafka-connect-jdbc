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

import io.confluent.connect.jdbc.util.JdbcUtils;
import org.apache.kafka.connect.data.Decimal;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.ConnectException;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.api.easymock.PowerMock;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Collections;

import static org.easymock.EasyMock.expect;
import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

@RunWith(PowerMockRunner.class)
@PrepareForTest(JdbcUtils.class)
public class TimestampIncrementingTableQuerierTest {

  private Connection connection;

  @Before
  public void setUp() throws SQLException {
    connection = mock(Connection.class);

    PowerMock.mockStatic(JdbcUtils.class);

    expect(JdbcUtils.getIdentifierQuoteString(connection)).andReturn("");
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
  public void createAutoIncrementingPreparedStatement() throws SQLException {
    final TimestampIncrementingTableQuerier querier = new TimestampIncrementingTableQuerier(TableQuerier.QueryMode.TABLE, "table", "", null, null, "", "", Collections.<String, Object>emptyMap(), 0L, null, false);

    expect(JdbcUtils.getAutoincrementColumn(connection, null, "table")).andReturn("id");

    expect(JdbcUtils.quoteString("table", "")).andReturn("table");
    expect(JdbcUtils.quoteString("id", "")).andReturn("id");

    PowerMock.replay(JdbcUtils.class);

    querier.getOrCreatePreparedStatement(connection);

    verify(connection).prepareStatement("SELECT * FROM table WHERE id > ? ORDER BY id ASC");

    PowerMock.verifyAll();
  }

  @Test
  public void createIncrementingAliasPreparedStatement() throws SQLException {
    final TimestampIncrementingTableQuerier querier = new TimestampIncrementingTableQuerier(TableQuerier.QueryMode.QUERY, "query", "", null, null, "id", "t", Collections.<String, Object>emptyMap(), 0L, null, false);

    expect(JdbcUtils.quoteString("t", "")).andReturn("t");
    expect(JdbcUtils.quoteString("id", "")).andReturn("id");

    PowerMock.replayAll();

    querier.getOrCreatePreparedStatement(connection);

    verify(connection).prepareStatement("query WHERE t.id > ? ORDER BY t.id ASC");

    PowerMock.verifyAll();
  }

  @Test
  public void createTimestampPreparedStatement() throws SQLException {
    final TimestampIncrementingTableQuerier querier = new TimestampIncrementingTableQuerier(TableQuerier.QueryMode.TABLE, "table", "", "updated_at", "", null, null, Collections.<String, Object>emptyMap(), 0L, null, false);

    expect(JdbcUtils.quoteString("table", "")).andReturn("table");
    expect(JdbcUtils.quoteString("updated_at", "")).andReturn("updated_at");

    PowerMock.replay(JdbcUtils.class);

    querier.getOrCreatePreparedStatement(connection);

    verify(connection).prepareStatement("SELECT * FROM table WHERE updated_at > ? AND updated_at < ? ORDER BY updated_at ASC");

    PowerMock.verifyAll();
  }

  @Test
  public void createTimestampAliasPreparedStatement() throws SQLException {
    final TimestampIncrementingTableQuerier querier = new TimestampIncrementingTableQuerier(TableQuerier.QueryMode.QUERY, "query", "", "updated_at", "t", null, null, Collections.<String, Object>emptyMap(), 0L, null, false);

    expect(JdbcUtils.quoteString("t", "")).andReturn("t");
    expect(JdbcUtils.quoteString("updated_at", "")).andReturn("updated_at");

    PowerMock.replay(JdbcUtils.class);

    querier.getOrCreatePreparedStatement(connection);

    verify(connection).prepareStatement("query WHERE t.updated_at > ? AND t.updated_at < ? ORDER BY t.updated_at ASC");

    PowerMock.verifyAll();
  }

  @Test
  public void createTimestampIncrementingPreparedStatement() throws SQLException {
    final TimestampIncrementingTableQuerier querier = new TimestampIncrementingTableQuerier(TableQuerier.QueryMode.TABLE, "table", "", "updated_at", "", "id", "", Collections.<String, Object>emptyMap(), 0L, null, false);

    expect(JdbcUtils.quoteString("table", "")).andReturn("table");
    expect(JdbcUtils.quoteString("id", "")).andReturn("id");
    expect(JdbcUtils.quoteString("updated_at", "")).andReturn("updated_at");

    PowerMock.replay(JdbcUtils.class);

    querier.getOrCreatePreparedStatement(connection);

    verify(connection).prepareStatement("SELECT * FROM table WHERE updated_at < ? AND ((updated_at = ? AND id > ?) OR updated_at > ?) ORDER BY updated_at,id ASC");

    PowerMock.verifyAll();
  }

  @Test
  public void createTimestampAliasIncrementingAliasPreparedStatement() throws SQLException {
    final TimestampIncrementingTableQuerier querier = new TimestampIncrementingTableQuerier(TableQuerier.QueryMode.QUERY, "query", "", "updated_at", "t", "id", "i", Collections.<String, Object>emptyMap(), 0L, null, false);

    expect(JdbcUtils.quoteString("t", "")).andReturn("t");
    expect(JdbcUtils.quoteString("i", "")).andReturn("i");
    expect(JdbcUtils.quoteString("id", "")).andReturn("id");
    expect(JdbcUtils.quoteString("updated_at", "")).andReturn("updated_at");

    PowerMock.replay(JdbcUtils.class);

    querier.getOrCreatePreparedStatement(connection);

    verify(connection).prepareStatement("query WHERE t.updated_at < ? AND ((t.updated_at = ? AND i.id > ?) OR t.updated_at > ?) ORDER BY t.updated_at,i.id ASC");

    PowerMock.verifyAll();
  }

  private TimestampIncrementingTableQuerier newQuerier() {
    return new TimestampIncrementingTableQuerier(TableQuerier.QueryMode.TABLE, null, "", null, null, "id", "", Collections.<String, Object>emptyMap(), 0L, null, false);
  }

}
