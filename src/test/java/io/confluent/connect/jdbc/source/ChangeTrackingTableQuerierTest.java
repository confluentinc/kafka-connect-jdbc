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
import io.confluent.connect.jdbc.util.ColumnDefinition;
import io.confluent.connect.jdbc.util.ColumnId;
import io.confluent.connect.jdbc.util.ExpressionBuilder;
import io.confluent.connect.jdbc.util.TableId;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.api.easymock.annotation.Mock;
import org.powermock.api.easymock.annotation.MockNice;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Timestamp;
import java.util.*;

import static org.easymock.EasyMock.*;
import static org.easymock.EasyMock.expect;
import static org.junit.Assert.*;
import static org.powermock.api.easymock.PowerMock.expectLastCall;
import static org.powermock.api.easymock.PowerMock.replay;
import static org.powermock.api.easymock.PowerMock.*;

@RunWith(PowerMockRunner.class)
@PrepareForTest(SchemaMapping.class)
public class ChangeTrackingTableQuerierTest {

  private static final long INITIAL_CHANGE_VERSION_OFFSET = 1000;
  private static final String PK_COLUMN = "col1";
  private static final String tableName = "table";
  private static final TableId tableId = new TableId("", "", tableName);
  private static final ColumnId columnId = new ColumnId(tableId, PK_COLUMN);

  @Mock
  private PreparedStatement stmt;
  @Mock
  private ResultSet resultSet;
  @Mock
  private Connection db;
  @MockNice
  private ExpressionBuilder expressionBuilder;
  @Mock
  private SchemaMapping schemaMapping;
  private DatabaseDialect dialect;

  @Mock
  ColumnDefinition columnDefinition;

  @Before
  public void setUp() {
    dialect = mock(DatabaseDialect.class);
    mockStatic(SchemaMapping.class);
  }
  private Schema schema() {
    SchemaBuilder result = SchemaBuilder.struct();
    result.field(PK_COLUMN, Schema.INT64_SCHEMA);
    return result.build();
  }

  private ChangeTrackingTableQuerier querier(long changeVersionOffset) {
    expect(dialect.parseTableIdentifier(tableName)).andReturn(tableId);
    replay(dialect);
    // Have to replay the dialect here since it's used to the table ID in the querier's constructor
    return new ChangeTrackingTableQuerier(
            dialect,
            TableQuerier.QueryMode.TABLE,
            tableName,
            "",
            new ChangeTrackingOffset(changeVersionOffset).toMap(),
            ""
    );
  }


  private void expectNewQuery() throws Exception {
    expect(columnDefinition.isPrimaryKey()).andStubReturn(true);
    expect(columnDefinition.id()).andStubReturn(columnId);
    expect(dialect.createPreparedStatement(eq(db), anyObject())).andReturn(stmt);
    expect(dialect.describeColumns(eq(db), anyString(), anyString(), anyString(), anyString())).andStubReturn(Collections.singletonMap(columnId,columnDefinition));
    expect(dialect.expressionBuilder()).andReturn(expressionBuilder);
    dialect.validateSpecificColumnTypes(anyObject(), anyObject());
    expectLastCall();
    expect(stmt.executeQuery()).andReturn(resultSet);
    expect(resultSet.getMetaData()).andReturn(null);
    expect(SchemaMapping.create(anyObject(), anyObject(), anyObject())).andReturn(schemaMapping);
  }

  @Test
  public void testEmptyResultSet() throws Exception {
    expectNewQuery();
    ChangeTrackingTableQuerier querier = querier(INITIAL_CHANGE_VERSION_OFFSET);
    expect(resultSet.next()).andReturn(false);

    replayAll();

    querier.maybeStartQuery(db);

    assertFalse(querier.next());
  }

  @Test
  public void testSingleRecordInResultSet() throws Exception {
    long newChangeVersionOffset = INITIAL_CHANGE_VERSION_OFFSET + 1;
    expectNewQuery();
    ChangeTrackingTableQuerier querier = querier(INITIAL_CHANGE_VERSION_OFFSET);
    expectRecord(newChangeVersionOffset);
    expect(resultSet.next()).andReturn(false);

    replayAll();

    querier.maybeStartQuery(db);

    assertNextRecord(querier, newChangeVersionOffset);

    assertFalse(querier.next());
  }

  @Test
  public void testChangeTrackingMode() throws Exception {
    long firstNewChangeVersionOffset = INITIAL_CHANGE_VERSION_OFFSET + 1;
    long secondNewChangeVersionOffset = INITIAL_CHANGE_VERSION_OFFSET + 2;
    expectNewQuery();
    ChangeTrackingTableQuerier querier = querier(INITIAL_CHANGE_VERSION_OFFSET);
    expectRecord(firstNewChangeVersionOffset);
    expectRecord(secondNewChangeVersionOffset);
    expect(resultSet.next()).andReturn(false);

    replayAll();

    querier.maybeStartQuery(db);

    // We commit offsets immediately in this mode
    assertNextRecord(querier, firstNewChangeVersionOffset);
    assertNextRecord(querier, secondNewChangeVersionOffset);

    assertFalse(querier.next());
  }

  @Test
  public void testMultipleSingleRecordResultSets() throws Exception {
    expectNewQuery();
    expectNewQuery();
    ChangeTrackingTableQuerier querier = querier(INITIAL_CHANGE_VERSION_OFFSET);
    expectRecord(INITIAL_CHANGE_VERSION_OFFSET);
    expect(resultSet.next()).andReturn(false);
    expectReset();
    expectRecord(INITIAL_CHANGE_VERSION_OFFSET);
    expect(resultSet.next()).andReturn(false);

    replayAll();

    querier.maybeStartQuery(db);

    assertNextRecord(querier, INITIAL_CHANGE_VERSION_OFFSET);

    assertFalse(querier.next());

    querier.reset(0, true);
    querier.maybeStartQuery(db);

    assertNextRecord(querier, INITIAL_CHANGE_VERSION_OFFSET);

    assertFalse(querier.next());
  }


  private void assertNextRecord(
          ChangeTrackingTableQuerier querier, long expectedChangeVersionOffset
  ) throws Exception {
    assertTrue(querier.next());
    SourceRecord record = querier.extractRecord();
    ChangeTrackingOffset actualOffset = ChangeTrackingOffset.fromMap(record.sourceOffset());
    assertEquals(expectedChangeVersionOffset, actualOffset.getChangeVersionOffset());
  }

  private void expectRecord(long changeVersionOffset) throws Exception {
    expect(schemaMapping.schema()).andReturn(schema()).times(2);
    expect(resultSet.next()).andReturn(true);
    expect(schemaMapping.fieldSetters()).andReturn(Collections.emptyList());
    ChangeTrackingOffset offset = new ChangeTrackingOffset(changeVersionOffset);
    expect(resultSet.getLong(anyString())).andReturn(changeVersionOffset);
  }

  private void expectReset() throws Exception {
    resultSet.close();
    expectLastCall();
    stmt.close();
    expectLastCall();
    db.commit();
    expectLastCall();
  }

}
