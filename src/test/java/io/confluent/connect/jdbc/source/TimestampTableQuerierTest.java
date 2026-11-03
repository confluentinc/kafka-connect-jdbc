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

import io.confluent.connect.jdbc.dialect.DatabaseDialect;
import io.confluent.connect.jdbc.util.ExpressionBuilder;
import io.confluent.connect.jdbc.util.TableId;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.junit.MockitoJUnitRunner;
import org.mockito.stubbing.OngoingStubbing;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.time.ZoneId;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class TimestampTableQuerierTest {

  private static final Timestamp INITIAL_TS = new Timestamp(71);
  private static final long INITIAL_INC = 4761;
  private static final List<String> TIMESTAMP_COLUMNS = Arrays.asList("ts1", "ts2");
  private static final String INCREMENTING_COLUMN = "inc";

  @Mock
  private PreparedStatement stmt;
  @Mock
  private ResultSet resultSet;
  @Mock
  private Connection db;
  @Mock
  private ExpressionBuilder expressionBuilder;
  @Mock
  private TimestampIncrementingCriteria criteria;
  @Mock
  private SchemaMapping schemaMapping;
  private DatabaseDialect dialect;

  private MockedStatic<SchemaMapping> schemaMappingStatic;
  private OngoingStubbing<Boolean> resultSetNextStubbing;
  private OngoingStubbing<TimestampIncrementingOffset> extractValuesStubbing;

  @Before
  public void setUp() {
    dialect = mock(DatabaseDialect.class);
    schemaMappingStatic = mockStatic(SchemaMapping.class);
  }

  @After
  public void tearDown() {
    schemaMappingStatic.close();
  }

  private TimestampIncrementingTableQuerier querier(Timestamp initialTimestampOffset) {
    final String tableName = "table";
    when(dialect.parseTableIdentifier(tableName)).thenReturn(new TableId("", "", tableName));

    return new TimestampTableQuerier(
        dialect,
        TableQuerier.QueryMode.TABLE,
        tableName,
        "",
        TIMESTAMP_COLUMNS,
        new TimestampIncrementingOffset(initialTimestampOffset, null).toMap(),
        10211197100L, // Timestamp delay
        ZoneId.of("UTC"),
        "",
        JdbcSourceConnectorConfig.TimestampGranularity.CONNECT_LOGICAL,
        false
    );
  }

  private Schema schema() {
    SchemaBuilder result =SchemaBuilder.struct();
    result.field(INCREMENTING_COLUMN, Schema.INT64_SCHEMA);
    TIMESTAMP_COLUMNS.forEach(
        col -> result.field(col, org.apache.kafka.connect.data.Timestamp.builder().build())
    );
    return result.build();
  }

  private void expectNewQuery() throws Exception {
    when(dialect.createPreparedStatement(eq(db), any())).thenReturn(stmt);
    when(dialect.expressionBuilder()).thenReturn(expressionBuilder);
    when(dialect.criteriaFor(any(), any())).thenReturn(criteria);
    when(stmt.executeQuery()).thenReturn(resultSet);
    when(resultSet.getMetaData()).thenReturn(null);
    when(schemaMapping.schema()).thenReturn(schema());
    when(schemaMapping.fieldSetters()).thenReturn(Collections.emptyList());
    schemaMappingStatic.when(() -> SchemaMapping.create(any(), any(), any())).thenReturn(schemaMapping);
  }

  private void stubResultSetNext(boolean hasNext) throws SQLException {
    if (resultSetNextStubbing == null) {
      resultSetNextStubbing = when(resultSet.next()).thenReturn(hasNext);
    } else {
      resultSetNextStubbing = resultSetNextStubbing.thenReturn(hasNext);
    }
  }

  private void stubExtractValues(TimestampIncrementingOffset offset) throws Exception {
    if (extractValuesStubbing == null) {
      extractValuesStubbing = when(criteria.extractValues(any(), any(), any(), any())).thenReturn(offset);
    } else {
      extractValuesStubbing = extractValuesStubbing.thenReturn(offset);
    }
  }

  @Test
  public void testEmptyResultSet() throws Exception {
    expectNewQuery();
    TimestampIncrementingTableQuerier querier = querier(INITIAL_TS);
    stubResultSetNext(false);

    querier.maybeStartQuery(db);

    assertFalse(querier.next());
  }

  @Test
  public void testSingleRecordInResultSet() throws Exception {
    Timestamp newTimestamp = new Timestamp(INITIAL_TS.getTime() + 1);
    expectNewQuery();
    TimestampIncrementingTableQuerier querier = querier(INITIAL_TS);
    expectRecord(newTimestamp);
    stubResultSetNext(false);

    querier.maybeStartQuery(db);

    assertNextRecord(querier, newTimestamp);

    assertFalse(querier.next());
  }

  @Test
  public void testTwoRecordsWithSameTimestampInResultSet() throws Exception {
    Timestamp newTimestamp = new Timestamp(INITIAL_TS.getTime() + 1);
    expectNewQuery();
    TimestampIncrementingTableQuerier querier = querier(INITIAL_TS);
    expectRecord(newTimestamp);
    expectRecord(newTimestamp);
    stubResultSetNext(false);

    querier.maybeStartQuery(db);

    // This isn't the last record in the result set with the new timestamp, so we can't commit an
    // offset that includes that timestamp yet
    assertNextRecord(querier, INITIAL_TS);

    // Now we can commit an offset with that timestamp, since this is the last record in the result
    // set
    assertNextRecord(querier, newTimestamp);

    assertFalse(querier.next());
  }

  @Test
  public void testTwoRecordsWithSameTimestampFollowedByRecordWithNewTimestampInResultSet() throws Exception {
    Timestamp firstNewTimestamp = new Timestamp(INITIAL_TS.getTime() + 1);
    Timestamp secondNewTimestamp = new Timestamp(INITIAL_TS.getTime() + 2);
    expectNewQuery();
    TimestampIncrementingTableQuerier querier = querier(INITIAL_TS);
    expectRecord(firstNewTimestamp);
    expectRecord(firstNewTimestamp);
    expectRecord(secondNewTimestamp);
    stubResultSetNext(false);

    querier.maybeStartQuery(db);

    // This isn't the last record in the result set with the new timestamp, so we can't commit an
    // offset that includes that timestamp yet
    assertNextRecord(querier, INITIAL_TS);

    // Now we can commit an offset with that timestamp, since this is the last record in the result
    // set
    assertNextRecord(querier, firstNewTimestamp);

    // And again, now we can commit an offset with the second new timestamp as we've exhausted the
    // batch
    assertNextRecord(querier, secondNewTimestamp);

    assertFalse(querier.next());
  }

  @Test
  public void testTwoRecordsWithSameTimestampFollowedByTwoRecordsWithNewTimestampInResultSet() throws Exception {
    Timestamp firstNewTimestamp = new Timestamp(INITIAL_TS.getTime() + 1);
    Timestamp secondNewTimestamp = new Timestamp(INITIAL_TS.getTime() + 2);
    expectNewQuery();
    TimestampIncrementingTableQuerier querier = querier(INITIAL_TS);
    expectRecord(firstNewTimestamp);
    expectRecord(firstNewTimestamp);
    expectRecord(secondNewTimestamp);
    expectRecord(secondNewTimestamp);
    stubResultSetNext(false);

    querier.maybeStartQuery(db);

    // This isn't the last record in the result set with the new timestamp, so we can't commit an
    // offset that includes that timestamp yet
    assertNextRecord(querier, INITIAL_TS);

    // Now we can commit an offset with that timestamp, since this is the last record in the result
    // set
    assertNextRecord(querier, firstNewTimestamp);

    // Again, have to reuse the timestamp since there's another record waiting that has the same
    // timestamp as the one we're about to query
    assertNextRecord(querier, firstNewTimestamp);

    // And again, now we can commit an offset with the second new timestamp as we've exhausted the
    // batch
    assertNextRecord(querier, secondNewTimestamp);

    assertFalse(querier.next());
  }

  @Test
  public void testMultipleSingleRecordResultSets() throws Exception {
    expectNewQuery();
    expectNewQuery();
    TimestampIncrementingTableQuerier querier = querier(INITIAL_TS);
    expectRecord(INITIAL_TS);
    stubResultSetNext(false);
    expectRecord(INITIAL_TS);
    stubResultSetNext(false);

    querier.maybeStartQuery(db);

    // We have to commit for the last record in the batch
    assertNextRecord(querier, INITIAL_TS);

    assertFalse(querier.next());

    querier.reset(0, true);
    querier.maybeStartQuery(db);

    // We have to commit for the last record in the batch
    assertNextRecord(querier, INITIAL_TS);

    assertFalse(querier.next());
  }

  @Test
  public void testMultipleDoubleRecordResultSetsOffsetReset() throws Exception {
    Timestamp firstNewTimestamp = new Timestamp(INITIAL_TS.getTime() + 1);
    expectNewQuery();
    expectNewQuery();
    TimestampIncrementingTableQuerier querier = querier(INITIAL_TS);
    expectRecord(INITIAL_TS);
    expectRecord(firstNewTimestamp);
    stubResultSetNext(false);

    querier.maybeStartQuery(db);

    // We have to commit for the last record in the batch
    assertNextRecord(querier, INITIAL_TS);
    assertNextRecord(querier, firstNewTimestamp);

    assertFalse(querier.next());

    querier.reset(0, true);
    querier.maybeStartQuery(db);

    assertEquals(querier.offset.getTimestampOffset(), INITIAL_TS);
  }

  @Test
  public void testMultipleDoubleRecordResultSetsNoOffsetReset() throws Exception {
    Timestamp firstNewTimestamp = new Timestamp(INITIAL_TS.getTime() + 1);
    expectNewQuery();
    expectNewQuery();
    TimestampIncrementingTableQuerier querier = querier(INITIAL_TS);
    expectRecord(INITIAL_TS);
    expectRecord(firstNewTimestamp);
    stubResultSetNext(false);

    querier.maybeStartQuery(db);

    // We have to commit for the last record in the batch
    assertNextRecord(querier, INITIAL_TS);
    assertNextRecord(querier, firstNewTimestamp);

    assertFalse(querier.next());

    querier.reset(0, false);
    querier.maybeStartQuery(db);

    assertEquals(querier.offset.getTimestampOffset(), firstNewTimestamp);
  }

  private void assertNextRecord(
      TimestampIncrementingTableQuerier querier, Timestamp expectedTimestampOffset
  ) throws Exception {
    assertTrue(querier.next());
    SourceRecord record = querier.extractRecord();
    TimestampIncrementingOffset actualOffset =TimestampIncrementingOffset.fromMap(record.sourceOffset());
    assertEquals(expectedTimestampOffset, actualOffset.getTimestampOffset());
  }

  private void expectRecord(Timestamp timestamp) throws Exception {
    stubResultSetNext(true);
    TimestampIncrementingOffset offset = new TimestampIncrementingOffset(timestamp, null);
    stubExtractValues(offset);
  }

  private static TimestampIncrementingOffset offset(Timestamp ts) {
    return offset(ts, null);
  }

  private static TimestampIncrementingOffset offset(Timestamp ts, Long inc) {
    return new TimestampIncrementingOffset(ts, inc);
  }
}
