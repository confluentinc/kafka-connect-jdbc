/*
 * Copyright 2026 Confluent Inc.
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

package io.confluent.connect.jdbc.sink;

import static org.easymock.EasyMock.anyObject;
import static org.easymock.EasyMock.eq;
import static org.easymock.EasyMock.expect;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.util.Collection;
import java.util.Collections;

import org.easymock.EasyMockSupport;
import org.junit.Test;

import io.confluent.connect.jdbc.dialect.DatabaseDialect;
import io.confluent.connect.jdbc.util.ColumnId;
import io.confluent.connect.jdbc.util.TableId;

public class SafeSqlContextTest extends EasyMockSupport {

  @Test
  public void shouldRedactInsertStatementGeneratedFromStructuredInputs() {
    TableId tableId = new TableId(null, null, "t");
    Collection<ColumnId> keyColumns =
        Collections.singletonList(new ColumnId(tableId, "id"));
    Collection<ColumnId> nonKeyColumns =
        Collections.singletonList(new ColumnId(tableId, "email"));
    DatabaseDialect dialect = createMock(DatabaseDialect.class);
    expect(dialect.buildInsertStatement(eq(tableId), anyObject(), anyObject()))
        .andReturn("INSERT INTO \"t\" (\"id\",\"email\") VALUES (?, ?)");
    replayAll();

    SafeSqlContext context = SafeSqlContext.create(
        dialect,
        tableId,
        keyColumns,
        nonKeyColumns,
        SafeSqlContext.Operation.INSERT,
        null
    ).get();

    assertEquals(
        "INSERT INTO \"t\" (\"id\",\"email\") VALUES (<redacted>)",
        context.safeStatement()
    );
    assertNull(context.vendorPrefix());
    verifyAll();
  }

  @Test
  public void shouldRejectGeneratedStatementContainingNewline() {
    TableId tableId = new TableId(null, null, "t");
    Collection<ColumnId> keyColumns =
        Collections.singletonList(new ColumnId(tableId, "id"));
    Collection<ColumnId> nonKeyColumns = Collections.emptyList();
    DatabaseDialect dialect = createMock(DatabaseDialect.class);
    expect(dialect.buildInsertStatement(eq(tableId), anyObject(), anyObject()))
        .andReturn("INSERT INTO \"t\"\n(\"id\") VALUES (?)");
    replayAll();

    assertFalse(SafeSqlContext.create(
        dialect,
        tableId,
        keyColumns,
        nonKeyColumns,
        SafeSqlContext.Operation.INSERT,
        null
    ).isPresent());
    verifyAll();
  }

  @Test
  public void shouldRejectUnsupportedDeleteStatement() {
    TableId tableId = new TableId(null, null, "t");
    Collection<ColumnId> keyColumns =
        Collections.singletonList(new ColumnId(tableId, "id"));
    Collection<ColumnId> nonKeyColumns = Collections.emptyList();
    DatabaseDialect dialect = createMock(DatabaseDialect.class);
    expect(dialect.buildDeleteStatement(eq(tableId), anyObject()))
        .andThrow(new UnsupportedOperationException());
    replayAll();

    assertFalse(SafeSqlContext.create(
        dialect,
        tableId,
        keyColumns,
        nonKeyColumns,
        SafeSqlContext.Operation.DELETE,
        null
    ).isPresent());
    verifyAll();
  }

  @Test
  public void shouldRedactUpsertStatementGeneratedFromStructuredInputs() {
    TableId tableId = new TableId(null, null, "t");
    Collection<ColumnId> keyColumns =
        Collections.singletonList(new ColumnId(tableId, "id"));
    Collection<ColumnId> nonKeyColumns =
        Collections.singletonList(new ColumnId(tableId, "email"));
    DatabaseDialect dialect = createMock(DatabaseDialect.class);
    expect(dialect.buildUpsertQueryStatement(eq(tableId), anyObject(), anyObject()))
        .andReturn("INSERT INTO \"t\" (\"id\", \"email\") VALUES (?, ?)");
    replayAll();

    SafeSqlContext context = SafeSqlContext.create(
        dialect,
        tableId,
        keyColumns,
        nonKeyColumns,
        SafeSqlContext.Operation.UPSERT,
        null
    ).get();

    assertEquals(
        "INSERT INTO \"t\" (\"id\", \"email\") VALUES (<redacted>)",
        context.safeStatement()
    );
    verifyAll();
  }

  @Test
  public void shouldRedactUpdateStatementGeneratedFromStructuredInputs() {
    TableId tableId = new TableId(null, null, "t");
    Collection<ColumnId> keyColumns =
        Collections.singletonList(new ColumnId(tableId, "id"));
    Collection<ColumnId> nonKeyColumns =
        Collections.singletonList(new ColumnId(tableId, "email"));
    DatabaseDialect dialect = createMock(DatabaseDialect.class);
    expect(dialect.buildUpdateStatement(eq(tableId), anyObject(), anyObject()))
        .andReturn("UPDATE \"t\" SET \"email\" = ? WHERE \"id\" = ?");
    replayAll();

    SafeSqlContext context = SafeSqlContext.create(
        dialect,
        tableId,
        keyColumns,
        nonKeyColumns,
        SafeSqlContext.Operation.UPDATE,
        null
    ).get();

    assertEquals(
        "UPDATE \"t\" SET \"email\" = <redacted> WHERE \"id\" = <redacted>",
        context.safeStatement()
    );
    verifyAll();
  }

  @Test
  public void shouldRedactDeleteStatementAndExposeQualifiedTableAndVendorPrefix() {
    TableId tableId = new TableId(null, null, "t");
    Collection<ColumnId> keyColumns =
        Collections.singletonList(new ColumnId(tableId, "id"));
    Collection<ColumnId> nonKeyColumns = Collections.emptyList();
    DatabaseDialect dialect = createMock(DatabaseDialect.class);
    expect(dialect.buildDeleteStatement(eq(tableId), anyObject()))
        .andReturn("DELETE FROM \"t\" WHERE \"id\" = ?");
    replayAll();

    SafeSqlContext context = SafeSqlContext.create(
        dialect,
        tableId,
        keyColumns,
        nonKeyColumns,
        SafeSqlContext.Operation.DELETE,
        "ORA"
    ).get();

    assertEquals("DELETE FROM \"t\" WHERE \"id\" = <redacted>", context.safeStatement());
    assertEquals("ORA", context.vendorPrefix());
    assertTrue(context.qualifiedTable().contains("t"));
    verifyAll();
  }

  @Test
  public void shouldReturnEmptyWhenDialectReturnsNullStatement() {
    TableId tableId = new TableId(null, null, "t");
    Collection<ColumnId> keyColumns =
        Collections.singletonList(new ColumnId(tableId, "id"));
    Collection<ColumnId> nonKeyColumns = Collections.emptyList();
    DatabaseDialect dialect = createMock(DatabaseDialect.class);
    expect(dialect.buildInsertStatement(eq(tableId), anyObject(), anyObject()))
        .andReturn(null);
    replayAll();

    assertFalse(SafeSqlContext.create(
        dialect,
        tableId,
        keyColumns,
        nonKeyColumns,
        SafeSqlContext.Operation.INSERT,
        null
    ).isPresent());
    verifyAll();
  }
}
