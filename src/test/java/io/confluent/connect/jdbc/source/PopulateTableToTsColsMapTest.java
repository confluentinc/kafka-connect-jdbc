/*
 * Copyright 2024 Confluent Inc.
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

/**
 * Tests for the {@link JdbcSourceTask#populateTableToTsColsMap()} method.
 * 
 * This test class specifically focuses on testing the timestamp column mapping functionality,
 * including regex matching using {@link TableId#toUnquotedString()} for proper table name
 * comparison against regex patterns in the timestamp.columns.mapping configuration.
 */

package io.confluent.connect.jdbc.source;

import io.confluent.connect.jdbc.dialect.GenericDatabaseDialect;
import io.confluent.connect.jdbc.util.TableId;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class PopulateTableToTsColsMapTest {

  private JdbcSourceTaskConfig config;
  private GenericDatabaseDialect dialect;
  private JdbcSourceTask task;
  private Map<String, List<String>> tableToTsCols;

  @Before
  public void setUp() {
    task = new JdbcSourceTask();
    config = mock(JdbcSourceTaskConfig.class);
    dialect = mock(GenericDatabaseDialect.class);
    tableToTsCols = new HashMap<>();
    
    try {
      java.lang.reflect.Field configField = JdbcSourceTask.class.getDeclaredField("config");
      configField.setAccessible(true);
      configField.set(task, config);
      
      java.lang.reflect.Field dialectField = JdbcSourceTask.class.getDeclaredField("dialect");
      dialectField.setAccessible(true);
      dialectField.set(task, dialect);
      
      java.lang.reflect.Field tableToTsColsField = JdbcSourceTask.class.getDeclaredField("tableToTsCols");
      tableToTsColsField.setAccessible(true);
      tableToTsColsField.set(task, tableToTsCols);
    } catch (Exception e) {
      throw new RuntimeException("Failed to set up test fields", e);
    }
  }

  @Test
  public void testPopulateTableToTsColsMapWithSingleColumn() throws Exception {
    List<String> tables = Arrays.asList("mydb.myschema.users", "mydb.myschema.orders");
    List<String> timestampMappings = Arrays.asList(".*users.*:[created_at]", ".*orders.*:[updated_at]");
    
    when(config.getList(JdbcSourceTaskConfig.TABLES_CONFIG)).thenReturn(tables);
    when(config.timestampColumnMapping()).thenReturn(timestampMappings);
    
    when(dialect.parseTableIdentifier("mydb.myschema.users"))
        .thenReturn(new TableId("mydb", "myschema", "users"));
    when(dialect.parseTableIdentifier("mydb.myschema.orders"))
        .thenReturn(new TableId("mydb", "myschema", "orders"));

    task.populateTableToTsColsMap();

    assertEquals(2, tableToTsCols.size());
    assertEquals(Arrays.asList("created_at"), tableToTsCols.get("mydb.myschema.users"));
    assertEquals(Arrays.asList("updated_at"), tableToTsCols.get("mydb.myschema.orders"));
  }

  @Test
  public void testPopulateTableToTsColsMapWithMultipleColumns() throws Exception {
    List<String> tables = Arrays.asList("mydb.myschema.users", "mydb.myschema.orders");
    List<String> timestampMappings = Arrays.asList(
        ".*users.*:[created_at|updated_at|modified_at]",
        ".*orders.*:[order_created|order_updated]"
    );
    
    when(config.getList(JdbcSourceTaskConfig.TABLES_CONFIG)).thenReturn(tables);
    when(config.timestampColumnMapping()).thenReturn(timestampMappings);
    
    when(dialect.parseTableIdentifier("mydb.myschema.users"))
        .thenReturn(new TableId("mydb", "myschema", "users"));
    when(dialect.parseTableIdentifier("mydb.myschema.orders"))
        .thenReturn(new TableId("mydb", "myschema", "orders"));

    task.populateTableToTsColsMap();

    assertEquals(2, tableToTsCols.size());
    assertEquals(Arrays.asList("created_at", "updated_at", "modified_at"), 
                 tableToTsCols.get("mydb.myschema.users"));
    assertEquals(Arrays.asList("order_created", "order_updated"), 
                 tableToTsCols.get("mydb.myschema.orders"));
  }

  @Test
  public void testPopulateTableToTsColsMapWithQuotedTableNames() throws Exception {
    List<String> tables = Arrays.asList("\"mydb\".\"myschema\".\"users\"", "\"mydb\".\"myschema\".\"orders\"");
    List<String> timestampMappings = Arrays.asList(".*users.*:[created_at]", ".*orders.*:[updated_at]");
    
    when(config.getList(JdbcSourceTaskConfig.TABLES_CONFIG)).thenReturn(tables);
    when(config.timestampColumnMapping()).thenReturn(timestampMappings);
    
    // Mock dialect to return proper TableId objects (quoted)
    when(dialect.parseTableIdentifier("\"mydb\".\"myschema\".\"users\""))
        .thenReturn(new TableId("mydb", "myschema", "users"));
    when(dialect.parseTableIdentifier("\"mydb\".\"myschema\".\"orders\""))
        .thenReturn(new TableId("mydb", "myschema", "orders"));

    task.populateTableToTsColsMap();

    assertEquals(2, tableToTsCols.size());
    assertEquals(Arrays.asList("created_at"), tableToTsCols.get("\"mydb\".\"myschema\".\"users\""));
    assertEquals(Arrays.asList("updated_at"), tableToTsCols.get("\"mydb\".\"myschema\".\"orders\""));
  }

  @Test
  public void testPopulateTableToTsColsMapWithNoMatches() throws Exception {
    List<String> tables = Arrays.asList("mydb.myschema.products");
    List<String> timestampMappings = Arrays.asList(".*users.*:[created_at]", ".*orders.*:[updated_at]");
    
    when(config.getList(JdbcSourceTaskConfig.TABLES_CONFIG)).thenReturn(tables);
    when(config.timestampColumnMapping()).thenReturn(timestampMappings);
    
    when(dialect.parseTableIdentifier("mydb.myschema.products"))
        .thenReturn(new TableId("mydb", "myschema", "products"));

    task.populateTableToTsColsMap();

    assertEquals(0, tableToTsCols.size());
  }

  @Test
  public void testPopulateTableToTsColsMapWithMultipleMatches() throws Exception {
    List<String> tables = Arrays.asList("mydb.myschema.users");
    List<String> timestampMappings = Arrays.asList(
        ".*users.*:[created_at|updated_at]",
        ".*myschema.*:[modified_at]",
        ".*mydb.*:[deleted_at]"
    );
    
    when(config.getList(JdbcSourceTaskConfig.TABLES_CONFIG)).thenReturn(tables);
    when(config.timestampColumnMapping()).thenReturn(timestampMappings);
    
    when(dialect.parseTableIdentifier("mydb.myschema.users"))
        .thenReturn(new TableId("mydb", "myschema", "users"));

    task.populateTableToTsColsMap();

    assertEquals(1, tableToTsCols.size());
    assertEquals(Arrays.asList("created_at", "updated_at"), 
                 tableToTsCols.get("mydb.myschema.users"));
  }

  @Test
  public void testPopulateTableToTsColsMapWithEmptyMappings() throws Exception {
    List<String> tables = Arrays.asList("mydb.myschema.users");
    List<String> timestampMappings = Collections.emptyList();
    
    when(config.getList(JdbcSourceTaskConfig.TABLES_CONFIG)).thenReturn(tables);
    when(config.timestampColumnMapping()).thenReturn(timestampMappings);

    task.populateTableToTsColsMap();

    assertEquals(0, tableToTsCols.size());
  }

  @Test
  public void testPopulateTableToTsColsMapWithEmptyTables() throws Exception {
    List<String> tables = Collections.emptyList();
    List<String> timestampMappings = Arrays.asList(".*users.*:[created_at]");
    
    when(config.getList(JdbcSourceTaskConfig.TABLES_CONFIG)).thenReturn(tables);
    when(config.timestampColumnMapping()).thenReturn(timestampMappings);

    task.populateTableToTsColsMap();

    assertEquals(0, tableToTsCols.size());
  }

  @Test
  public void testPopulateTableToTsColsMapWithComplexRegex() throws Exception {
    List<String> tables = Arrays.asList("mydb.analytics.user_events", "mydb.analytics.order_events");
    List<String> timestampMappings = Arrays.asList(
        ".*analytics\\.user_.*:[event_timestamp|created_at]",
        ".*analytics\\.order_.*:[order_timestamp|updated_at]"
    );
    
    when(config.getList(JdbcSourceTaskConfig.TABLES_CONFIG)).thenReturn(tables);
    when(config.timestampColumnMapping()).thenReturn(timestampMappings);
    
    when(dialect.parseTableIdentifier("mydb.analytics.user_events"))
        .thenReturn(new TableId("mydb", "analytics", "user_events"));
    when(dialect.parseTableIdentifier("mydb.analytics.order_events"))
        .thenReturn(new TableId("mydb", "analytics", "order_events"));

    task.populateTableToTsColsMap();

    assertEquals(2, tableToTsCols.size());
    assertEquals(Arrays.asList("event_timestamp", "created_at"), 
                 tableToTsCols.get("mydb.analytics.user_events"));
    assertEquals(Arrays.asList("order_timestamp", "updated_at"), 
                 tableToTsCols.get("mydb.analytics.order_events"));
  }

  @Test
  public void testPopulateTableToTsColsMapWithSchemaOnlyTable() throws Exception {
    List<String> tables = Arrays.asList("myschema.users");
    List<String> timestampMappings = Arrays.asList(".*users.*:[created_at]");
    
    when(config.getList(JdbcSourceTaskConfig.TABLES_CONFIG)).thenReturn(tables);
    when(config.timestampColumnMapping()).thenReturn(timestampMappings);
    
    // Mock dialect to return proper TableId objects (no catalog)
    when(dialect.parseTableIdentifier("myschema.users"))
        .thenReturn(new TableId(null, "myschema", "users"));

    task.populateTableToTsColsMap();

    assertEquals(1, tableToTsCols.size());
    assertEquals(Arrays.asList("created_at"), tableToTsCols.get("myschema.users"));
  }

  @Test
  public void testPopulateTableToTsColsMapWithTableOnly() throws Exception {
    List<String> tables = Arrays.asList("users");
    List<String> timestampMappings = Arrays.asList("users:[created_at]");
    
    when(config.getList(JdbcSourceTaskConfig.TABLES_CONFIG)).thenReturn(tables);
    when(config.timestampColumnMapping()).thenReturn(timestampMappings);
    
    // Mock dialect to return proper TableId objects (no catalog, no schema)
    when(dialect.parseTableIdentifier("users"))
        .thenReturn(new TableId(null, null, "users"));

    task.populateTableToTsColsMap();

    assertEquals(1, tableToTsCols.size());
    assertEquals(Arrays.asList("created_at"), tableToTsCols.get("users"));
  }
}
