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
}
