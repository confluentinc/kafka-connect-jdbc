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

import org.apache.kafka.common.config.ConfigValue;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class JdbcSourceConnectorConfigMappingTest {

  @Test
  public void testTimestampColumnMappingValidation() {
    Map<String, String> props = new HashMap<>();
    props.put(JdbcSourceConnectorConfig.CONNECTION_URL_CONFIG, "jdbc:test://localhost");
    props.put(JdbcSourceConnectorConfig.TIMESTAMP_COLUMN_MAPPING_CONFIG, "something garbage");
    
    Map<String, ConfigValue> validatedConfig =
        JdbcSourceConnectorConfig.CONFIG_DEF.validateAll(props);
    ConfigValue validatedValue =
        validatedConfig.get(JdbcSourceConnectorConfig.TIMESTAMP_COLUMN_MAPPING_CONFIG);
    assertTrue(validatedValue.errorMessages().get(0).contains("Invalid format. Expected 'regex:[col1|col2|...]'"));

    props.put(JdbcSourceConnectorConfig.TIMESTAMP_COLUMN_MAPPING_CONFIG, "something:garbage:something");
    validatedConfig =
        JdbcSourceConnectorConfig.CONFIG_DEF.validateAll(props);
    validatedValue =
        validatedConfig.get(JdbcSourceConnectorConfig.TIMESTAMP_COLUMN_MAPPING_CONFIG);
    assertTrue(validatedValue.errorMessages().get(0).contains("Invalid format. Expected 'regex:[col1|col2|...]'"));

    props.put(JdbcSourceConnectorConfig.TIMESTAMP_COLUMN_MAPPING_CONFIG, ".*):[col1, col2]");
    validatedConfig =
        JdbcSourceConnectorConfig.CONFIG_DEF.validateAll(props);
    validatedValue =
        validatedConfig.get(JdbcSourceConnectorConfig.TIMESTAMP_COLUMN_MAPPING_CONFIG);
    assertTrue(validatedValue.errorMessages().get(0).contains("Invalid regular expression"));

    props.put(JdbcSourceConnectorConfig.TIMESTAMP_COLUMN_MAPPING_CONFIG, "abc.*:col1|col2],.*:col3|col4]");
    validatedConfig =
        JdbcSourceConnectorConfig.CONFIG_DEF.validateAll(props);
    validatedValue =
        validatedConfig.get(JdbcSourceConnectorConfig.TIMESTAMP_COLUMN_MAPPING_CONFIG);
    assertTrue(validatedValue.errorMessages().get(0).contains("Columns list must be enclosed in square brackets"));

    props.put(JdbcSourceConnectorConfig.TIMESTAMP_COLUMN_MAPPING_CONFIG, "abc.*:[|col2],.*:[col3|col4]");
    validatedConfig =
        JdbcSourceConnectorConfig.CONFIG_DEF.validateAll(props);
    validatedValue =
        validatedConfig.get(JdbcSourceConnectorConfig.TIMESTAMP_COLUMN_MAPPING_CONFIG);
    assertTrue(validatedValue.errorMessages().get(0).contains("Every column name should be non-empty string"));

    props.put(JdbcSourceConnectorConfig.TIMESTAMP_COLUMN_MAPPING_CONFIG, "abc.*:[]");
    validatedConfig =
        JdbcSourceConnectorConfig.CONFIG_DEF.validateAll(props);
    validatedValue =
        validatedConfig.get(JdbcSourceConnectorConfig.TIMESTAMP_COLUMN_MAPPING_CONFIG);
    assertTrue(validatedValue.errorMessages().get(0).contains("Columns list cannot be empty"));
  }

  @Test
  public void testIncrementingColumnMappingValidation() {
    Map<String, String> props = new HashMap<>();
    props.put(JdbcSourceConnectorConfig.CONNECTION_URL_CONFIG, "jdbc:test://localhost");
    props.put(JdbcSourceConnectorConfig.INCREMENTING_COLUMN_MAPPING_CONFIG, "something garbage");
    
    Map<String, ConfigValue> validatedConfig =
        JdbcSourceConnectorConfig.CONFIG_DEF.validateAll(props);
    ConfigValue validatedValue =
        validatedConfig.get(JdbcSourceConnectorConfig.INCREMENTING_COLUMN_MAPPING_CONFIG);
    assertTrue(validatedValue.errorMessages().get(0).contains("Invalid format. Expected 'regex:columnName'"));

    props.put(JdbcSourceConnectorConfig.INCREMENTING_COLUMN_MAPPING_CONFIG, "something:garbage:something");
    validatedConfig =
        JdbcSourceConnectorConfig.CONFIG_DEF.validateAll(props);
    validatedValue =
        validatedConfig.get(JdbcSourceConnectorConfig.INCREMENTING_COLUMN_MAPPING_CONFIG);
    assertTrue(validatedValue.errorMessages().get(0).contains("Invalid format. Expected 'regex:columnName'"));

    props.put(JdbcSourceConnectorConfig.INCREMENTING_COLUMN_MAPPING_CONFIG, ".*):col1");
    validatedConfig =
        JdbcSourceConnectorConfig.CONFIG_DEF.validateAll(props);
    validatedValue =
        validatedConfig.get(JdbcSourceConnectorConfig.INCREMENTING_COLUMN_MAPPING_CONFIG);
    assertTrue(validatedValue.errorMessages().get(0).contains("Invalid regular expression"));

    props.put(JdbcSourceConnectorConfig.INCREMENTING_COLUMN_MAPPING_CONFIG, "abc.*:ID,abc.*: ,abc.*:ID");
    validatedConfig =
        JdbcSourceConnectorConfig.CONFIG_DEF.validateAll(props);
    validatedValue =
        validatedConfig.get(JdbcSourceConnectorConfig.INCREMENTING_COLUMN_MAPPING_CONFIG);
    assertTrue(validatedValue.errorMessages().get(0).contains("Column name cannot be empty"));
  }

  @Test
  public void testModeUsesTimestampColumn() {
    Map<String, String> props = new HashMap<>();
    props.put(JdbcSourceConnectorConfig.CONNECTION_URL_CONFIG, "jdbc:test://localhost");
    props.put(JdbcSourceConnectorConfig.MODE_CONFIG, JdbcSourceConnectorConfig.MODE_TIMESTAMP);
    
    JdbcSourceConnectorConfig config = new JdbcSourceConnectorConfig(props);
    assertTrue(config.modeUsesTimestampColumn());
    assertFalse(config.modeUsesIncrementingColumn());

    props.put(JdbcSourceConnectorConfig.MODE_CONFIG, JdbcSourceConnectorConfig.MODE_INCREMENTING);
    config = new JdbcSourceConnectorConfig(props);
    assertFalse(config.modeUsesTimestampColumn());
    assertTrue(config.modeUsesIncrementingColumn());

    props.put(JdbcSourceConnectorConfig.MODE_CONFIG, JdbcSourceConnectorConfig.MODE_TIMESTAMP_INCREMENTING);
    config = new JdbcSourceConnectorConfig(props);
    assertTrue(config.modeUsesTimestampColumn());
    assertTrue(config.modeUsesIncrementingColumn());

    props.put(JdbcSourceConnectorConfig.MODE_CONFIG, JdbcSourceConnectorConfig.MODE_BULK);
    config = new JdbcSourceConnectorConfig(props);
    assertFalse(config.modeUsesTimestampColumn());
    assertFalse(config.modeUsesIncrementingColumn());
  }

  @Test
  public void testTimestampColumnMappingAccessors() {
    Map<String, String> props = new HashMap<>();
    props.put(JdbcSourceConnectorConfig.CONNECTION_URL_CONFIG, "jdbc:test://localhost");
    props.put(JdbcSourceConnectorConfig.TIMESTAMP_COLUMN_MAPPING_CONFIG, 
        "schema1.table1.*:[col1|col2],schema2.table2.*:[col3]");
    
    JdbcSourceConnectorConfig config = new JdbcSourceConnectorConfig(props);
    assertEquals(2, config.timestampColumnMapping().size());
    assertEquals(2, config.timestampColMappingRegexes().size());
    assertEquals("schema1.table1.*", config.timestampColMappingRegexes().get(0));
    assertEquals("schema2.table2.*", config.timestampColMappingRegexes().get(1));
  }

  @Test
  public void testIncrementingColumnMappingAccessors() {
    Map<String, String> props = new HashMap<>();
    props.put(JdbcSourceConnectorConfig.CONNECTION_URL_CONFIG, "jdbc:test://localhost");
    props.put(JdbcSourceConnectorConfig.INCREMENTING_COLUMN_MAPPING_CONFIG, 
        "schema1.table1.*:id1,schema2.table2.*:id2");
    
    JdbcSourceConnectorConfig config = new JdbcSourceConnectorConfig(props);
    assertEquals(2, config.incrementingColumnMapping().size());
    assertEquals(2, config.incrementingColMappingRegexes().size());
    assertEquals("schema1.table1.*", config.incrementingColMappingRegexes().get(0));
    assertEquals("schema2.table2.*", config.incrementingColMappingRegexes().get(1));
  }
}
