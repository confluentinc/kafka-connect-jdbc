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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class TableCollectionUtilsTest {

  private static final String CATALOG = "catalog";
  private static final String SCHEMA1 = "schema1";
  private static final String SCHEMA2 = "schema2";
  private static final String TABLE1 = "users";
  private static final String TABLE2 = "orders";
  private static final String TABLE3 = "products";
  private static final String TABLE4 = "temp_users";
  private static final String TABLE5 = "staging_orders";

  @Test
  public void testFilterTablesWithIncludeOnly() {
    List<TableId> tables = Arrays.asList(
        new TableId(CATALOG, SCHEMA1, TABLE1),
        new TableId(CATALOG, SCHEMA1, TABLE2),
        new TableId(CATALOG, SCHEMA2, TABLE3)
    );
    
    Set<String> includeRegex = new HashSet<>(Arrays.asList(".*\"schema1\".*", ".*\"products\""));
    Set<String> excludeRegex = new HashSet<>();
    
    List<TableId> result = TableCollectionUtils.filterTables(tables, includeRegex, excludeRegex);
    
    assertEquals(3, result.size());
    assertTrue(result.stream().anyMatch(t -> TABLE1.equals(t.tableName())));
    assertTrue(result.stream().anyMatch(t -> TABLE2.equals(t.tableName())));
    assertTrue(result.stream().anyMatch(t -> TABLE3.equals(t.tableName())));
  }

  @Test
  public void testFilterTablesWithIncludeAndExclude() {
    List<TableId> tables = Arrays.asList(
        new TableId(CATALOG, SCHEMA1, TABLE1),
        new TableId(CATALOG, SCHEMA1, TABLE2),
        new TableId(CATALOG, SCHEMA2, TABLE3),
        new TableId(CATALOG, SCHEMA1, TABLE4),
        new TableId(CATALOG, SCHEMA2, TABLE5)
    );
    
    Set<String> includeRegex = new HashSet<>(Arrays.asList(".*\"schema1\".*", ".*\"schema2\".*"));
    Set<String> excludeRegex = new HashSet<>(Arrays.asList(".*\"temp.*", ".*\"staging.*"));
    
    List<TableId> result = TableCollectionUtils.filterTables(tables, includeRegex, excludeRegex);
    
    assertEquals(3, result.size());
    assertTrue(result.stream().anyMatch(t -> TABLE1.equals(t.tableName())));
    assertTrue(result.stream().anyMatch(t -> TABLE2.equals(t.tableName())));
    assertTrue(result.stream().anyMatch(t -> TABLE3.equals(t.tableName())));
    assertFalse(result.stream().anyMatch(t -> TABLE4.equals(t.tableName())));
    assertFalse(result.stream().anyMatch(t -> TABLE5.equals(t.tableName())));
  }

  @Test
  public void testFilterTablesWithExcludeOnly() {
    List<TableId> tables = Arrays.asList(
        new TableId(CATALOG, SCHEMA1, TABLE1),
        new TableId(CATALOG, SCHEMA1, TABLE4),
        new TableId(CATALOG, SCHEMA2, TABLE5)
    );
    
    // Empty include set means no tables will be included
    Set<String> includeRegex = new HashSet<>();
    Set<String> excludeRegex = new HashSet<>(Arrays.asList(".*temp.*"));
    
    List<TableId> result = TableCollectionUtils.filterTables(tables, includeRegex, excludeRegex);
    
    assertEquals(0, result.size());
  }

  @Test
  public void testFilterTablesWithEmptyIncludeList() {
    List<TableId> tables = Arrays.asList(
        new TableId(CATALOG, SCHEMA1, TABLE1),
        new TableId(CATALOG, SCHEMA1, TABLE2)
    );
    
    Set<String> includeRegex = new HashSet<>();
    Set<String> excludeRegex = new HashSet<>();
    
    List<TableId> result = TableCollectionUtils.filterTables(tables, includeRegex, excludeRegex);
    
    assertEquals(0, result.size());
  }

  @Test
  public void testFilterTablesRemovesDuplicates() {
    List<TableId> tables = Arrays.asList(
        new TableId(CATALOG, SCHEMA1, TABLE1),
        new TableId(CATALOG, SCHEMA1, TABLE1), // Duplicate
        new TableId(CATALOG, SCHEMA1, TABLE2)
    );
    
    Set<String> includeRegex = new HashSet<>(Arrays.asList(".*\"schema1\".*"));
    Set<String> excludeRegex = new HashSet<>();
    
    List<TableId> result = TableCollectionUtils.filterTables(tables, includeRegex, excludeRegex);
    
    assertEquals(2, result.size());
    assertTrue(result.stream().anyMatch(t -> TABLE1.equals(t.tableName())));
    assertTrue(result.stream().anyMatch(t -> TABLE2.equals(t.tableName())));
  }

  @Test
  public void testFilterTablesWithComplexRegex() {
    List<TableId> tables = Arrays.asList(
        new TableId(CATALOG, "prod_schema", "customer_data"),
        new TableId(CATALOG, "test_schema", "customer_data"),
        new TableId(CATALOG, "prod_schema", "order_history"),
        new TableId(CATALOG, "staging_schema", "temp_data")
    );
    
    Set<String> includeRegex = new HashSet<>(
            Arrays.asList(".*\"prod_schema\".*\"customer.*", ".*\"prod_schema\".*\"order.*"));
    Set<String> excludeRegex = new HashSet<>();
    
    List<TableId> result = TableCollectionUtils.filterTables(tables, includeRegex, excludeRegex);
    
    assertEquals(2, result.size());
    assertTrue(result.stream().anyMatch(t -> "customer_data".equals(t.tableName())
            && "prod_schema".equals(t.schemaName())));
    assertTrue(result.stream().anyMatch(t -> "order_history".equals(t.tableName())
            && "prod_schema".equals(t.schemaName())));
  }

  @Test
  public void testValidateEachTableMatchesExactlyOneRegexWithValidTables() {
    List<String> regexes = Arrays.asList(".*\"schema1\".*\"users\"", ".*\"schema1\".*\"orders\"");
    List<TableId> tableIds = Arrays.asList(
        new TableId(CATALOG, SCHEMA1, TABLE1),
        new TableId(CATALOG, SCHEMA1, TABLE2)
    );
    
    List<String> problems = new ArrayList<>();
    boolean result = TableCollectionUtils.validateEachTableMatchesExactlyOneRegex(
        regexes, tableIds, TableId::toString, problems::add
    );
    
    assertTrue(result);
    assertTrue(problems.isEmpty());
  }

  @Test
  public void testValidateEachTableMatchesExactlyOneRegexWithNoMatch() {
    List<String> regexes = Arrays.asList(".*\"schema1\".*\"users\"", ".*\"schema1\".*\"orders\"");
    List<TableId> tableIds = Arrays.asList(
        new TableId(CATALOG, SCHEMA2, TABLE3) // Won't match any regex
    );
    
    List<String> problems = new ArrayList<>();
    boolean result = TableCollectionUtils.validateEachTableMatchesExactlyOneRegex(
        regexes, tableIds, TableId::toString, problems::add
    );
    
    assertFalse(result);
    assertEquals(1, problems.size());
    assertTrue(problems.get(0).contains("does not match any of the provided regexes"));
  }

  @Test
  public void testValidateEachTableMatchesExactlyOneRegexWithMultipleMatches() {
    List<String> regexes = Arrays.asList(".*\"schema1\".*\"users\"", ".*\"users\""); // Both will match
    List<TableId> tableIds = Arrays.asList(
        new TableId(CATALOG, SCHEMA1, TABLE1)
    );
    
    List<String> problems = new ArrayList<>();
    boolean result = TableCollectionUtils.validateEachTableMatchesExactlyOneRegex(
        regexes, tableIds, TableId::toString, problems::add
    );
    
    assertFalse(result);
    assertEquals(1, problems.size());
    assertTrue(problems.get(0).contains("matches more than one of the provided regexes"));
  }

  @Test
  public void testValidateEachTableMatchesExactlyOneRegexWithEmptyRegexList() {
    List<String> regexes = new ArrayList<>();
    List<TableId> tableIds = Arrays.asList(
        new TableId(CATALOG, SCHEMA1, TABLE1)
    );
    
    List<String> problems = new ArrayList<>();
    boolean result = TableCollectionUtils.validateEachTableMatchesExactlyOneRegex(
        regexes, tableIds, TableId::toString, problems::add
    );
    
    assertFalse(result);
    assertEquals(1, problems.size());
    assertTrue(problems.get(0).contains("does not match any of the provided regexes"));
  }

  @Test
  public void testFilterTablesWithNullSets() {
    List<TableId> tables = Arrays.asList(
        new TableId(CATALOG, SCHEMA1, TABLE1)
    );
    
    List<TableId> result = TableCollectionUtils.filterTables(tables,
            null, null);
    
    assertEquals(0, result.size());
  }

  @Test
  public void testFilterTablesPreservesOrder() {
    List<TableId> tables = Arrays.asList(
        new TableId(CATALOG, SCHEMA1, "zebra"),
        new TableId(CATALOG, SCHEMA1, "apple"),
        new TableId(CATALOG, SCHEMA1, "banana")
    );
    
    Set<String> includeRegex = new HashSet<>(Arrays.asList(".*\"schema1\".*"));
    Set<String> excludeRegex = new HashSet<>();
    
    List<TableId> result = TableCollectionUtils.filterTables(tables, includeRegex, excludeRegex);
    
    assertEquals(3, result.size());
    assertEquals("zebra", result.get(0).tableName());
    assertEquals("apple", result.get(1).tableName());
    assertEquals("banana", result.get(2).tableName());
  }
}
