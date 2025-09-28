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

package io.confluent.connect.jdbc.util;

import org.junit.Test;

import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class TableCollectionUtilsTest {

  private static final String DB1 = "db1";
  private static final String TABLE1 = "table1";
  private static final String TABLE2 = "table2";
  private static final String TABLE3 = "table3";
  private static final String TABLE_REGEX1 = ".*table1";
  private static final String TABLE_REGEX2 = ".*table2";
  private static final String TABLE_REGEX_ALL = ".*table.*";


  @Test
  public void validateEachTableMatchesExactlyOneRegexWithValidTables() {
    List<String> regexes = Arrays.asList(TABLE_REGEX1, TABLE_REGEX2);
    List<TableId> tableIds = Arrays.asList(new TableId(null, DB1, TABLE1),
                                           new TableId(null, DB1, TABLE2));
    boolean result = TableCollectionUtils.validateEachTableMatchesExactlyOneRegex(
        regexes, tableIds, TableId::toUnquotedString, problem -> {});

    assertTrue(result);
  }

  @Test
  public void validateEachTableMatchesExactlyOneRegexWithNoMatch() {
    List<String> regexes = Arrays.asList(TABLE_REGEX1, TABLE_REGEX2);
    List<TableId> tableIds = Arrays.asList(new TableId(null, DB1, TABLE3));
    boolean result = TableCollectionUtils.validateEachTableMatchesExactlyOneRegex(
        regexes, tableIds, TableId::toUnquotedString, problem -> {});

    assertFalse(result);
  }

  @Test
  public void validateEachTableMatchesExactlyOneRegexWithMultipleMatches() {
    List<String> regexes = Arrays.asList(TABLE_REGEX1, TABLE_REGEX_ALL);
    List<TableId> tableIds = Arrays.asList(new TableId(null, DB1, TABLE1));
    boolean result = TableCollectionUtils.validateEachTableMatchesExactlyOneRegex(
        regexes, tableIds, TableId::toUnquotedString, problem -> {});

    assertFalse(result);
  }

  @Test
  public void validateEachTableMatchesExactlyOneRegexWithEmptyRegexList() {
    List<String> regexes = Arrays.asList();
    List<TableId> tableIds = Arrays.asList(new TableId(null, DB1, TABLE1));
    boolean result = TableCollectionUtils.validateEachTableMatchesExactlyOneRegex(
        regexes, tableIds, TableId::toUnquotedString, problem -> {});

    assertFalse(result);
  }
}
