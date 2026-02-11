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

package io.confluent.connect.jdbc.source;

import io.confluent.connect.jdbc.dialect.DatabaseDialect;
import io.confluent.connect.jdbc.util.TableId;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.api.easymock.annotation.Mock;
import org.powermock.modules.junit4.PowerMockRunner;

import java.sql.*;
import java.util.Collections;

import static org.easymock.EasyMock.*;
import static org.easymock.EasyMock.anyObject;
import static org.junit.Assert.*;
import static org.powermock.api.easymock.PowerMock.expectLastCall;
import static org.powermock.api.easymock.PowerMock.replayAll;

@RunWith(PowerMockRunner.class)
public class ChangeTrackingOffsetTest {
  private final long expectedOffset = 1001L;
  private final long MIN_CHANGE_TRACKING_OFFSET = 1000L;
  private final String MIN_CHANGE_TRACKING_OFFSET_FIELD = "min_valid_version";
  private static final TableId tableId = new TableId("", "", "table");
  private final ChangeTrackingOffset unset = new ChangeTrackingOffset(null);
  private final ChangeTrackingOffset set = new ChangeTrackingOffset(expectedOffset);
  @Mock
  private PreparedStatement stmt;
  @Mock
  private Connection db;
  @Mock
  private ResultSet resultSet;
  private DatabaseDialect dialect;

  @Before
  public void setUp() throws SQLException {
    dialect = mock(DatabaseDialect.class);
    expect(dialect.createPreparedStatement(eq(db), anyString())).andReturn(stmt);
    replay(dialect);
  }

  @Test
  public void testDefaults() throws Exception {
    assertEquals(0, unset.getChangeVersionOffset());
    expectNewQuery();
    assertEquals(MIN_CHANGE_TRACKING_OFFSET,unset.getChangeVersionOffset(dialect,db,tableId));
  }

  private void expectNewQuery() throws Exception {
    expect(stmt.executeQuery()).andReturn(resultSet);
    expect(resultSet.next()).andReturn(true);
    expect(resultSet.getLong(MIN_CHANGE_TRACKING_OFFSET_FIELD)).andReturn(MIN_CHANGE_TRACKING_OFFSET);
    resultSet.close();
    stmt.close();
    replayAll();
  }

  @Test
  public void testToMap() {
    assertEquals(0, unset.toMap().size());
    assertEquals(1, set.toMap().size());
  }

  @Test
  public void testGetChangeVersionOffset() throws Exception {
    assertEquals(0, unset.getChangeVersionOffset());
    assertEquals(expectedOffset, set.getChangeVersionOffset());
    expectNewQuery();
    assertEquals(MIN_CHANGE_TRACKING_OFFSET, unset.getChangeVersionOffset(dialect,db,tableId));
    assertEquals(expectedOffset, set.getChangeVersionOffset(dialect,db,tableId));
  }

  @Test
  public void testFromMap() {
    assertEquals(unset, ChangeTrackingOffset.fromMap(unset.toMap()));
    assertEquals(set, ChangeTrackingOffset.fromMap(set.toMap()));
  }

  @Test
  public void testEquals() {
    assertEquals(unset, new ChangeTrackingOffset(null));
    assertEquals(set,new ChangeTrackingOffset(expectedOffset));

  }

}
