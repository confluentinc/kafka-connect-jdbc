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

package io.confluent.connect.jdbc.source;

import java.util.HashMap;
import java.util.Map;

import org.junit.Test;

import io.confluent.connect.jdbc.util.TableId;

import static org.junit.Assert.assertEquals;

public class OffsetProtocolsTest {

  @Test
  public void v1PartitionUsesDotSeparatedName() {
    assertEquals(v1Partition("db.s.t"),
        OffsetProtocols.sourcePartitionForProtocolV1(new TableId("db", "s", "t")));
  }

  @Test
  public void v1PartitionAcceptsTableNamesWithSpacesAndParentheses() {
    // The partition key is an offset storage key, never executed as SQL, so legitimate table
    // names that are not valid bare SQL identifiers must still produce the same key as before.
    assertEquals(v1Partition("db.s(1).my table"),
        OffsetProtocols.sourcePartitionForProtocolV1(new TableId("db", "s(1)", "my table")));
  }

  private static Map<String, String> v1Partition(String fqn) {
    Map<String, String> expected = new HashMap<>();
    expected.put(JdbcSourceConnectorConstants.TABLE_NAME_KEY, fqn);
    expected.put(JdbcSourceConnectorConstants.OFFSET_PROTOCOL_VERSION_KEY,
        JdbcSourceConnectorConstants.PROTOCOL_VERSION_ONE);
    return expected;
  }
}
