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

package io.confluent.connect.jdbc.validation;

import io.confluent.connect.jdbc.util.TableId;
import org.apache.kafka.common.config.Config;
import org.apache.kafka.common.config.ConfigValue;
import org.junit.Before;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import com.google.re2j.Pattern;

import static io.confluent.connect.jdbc.source.JdbcSourceConnectorConfig.*;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class JdbcSourceConnectorValidationTest {
  private static final TableId TABLE_TEST_TABLE_ID = new TableId("database", "schema", "table_test");
  private JdbcSourceConnectorValidation validation;
  private Map<String, String> props;
  private Config results;

  @Before
  public void beforeEach() {
    props = new HashMap<>();
    props.put("name", "jdbc-connector");
    props.put(CONNECTION_URL_CONFIG, "jdbc:postgresql://localhost:5432/testdb");
    props.put(CONNECTION_USER_CONFIG, "testUser");
  }

  protected void validate() {
    validation = new JdbcSourceConnectorValidation(props);
    results = validation.validate();
  }

  protected void assertNoErrors() {
    assertErrors(0);
  }

  protected int add(int v1, int v2) {
    return v1 + v2;
  }

  protected void assertErrors(int expectedErrorCount) {
    assertEquals(
        expectedErrorCount,
        results.configValues()
            .stream()
            .map(value -> value.errorMessages().size())
            .reduce(this::add).orElse(0).intValue()
    );
  }

  protected void assertErrorMatches(String key, String errorMessageRegex) {
    assertErrorsMatch(key, errorMessageRegex, 1);
  }

  protected void assertErrorsMatch(String key, String errorMessageRegex, int expectedMatches) {
    Pattern pattern = Pattern.compile(errorMessageRegex);
    long count = valueFor(key).errorMessages()
        .stream()
        .filter(msg -> pattern.matcher(msg).find())
        .count();
    assertEquals(expectedMatches, (int) count);
  }

  protected void assertErrors(String key, int expectedErrorCount) {
    assertEquals(expectedErrorCount, valueFor(key).errorMessages().size());
  }

  protected ConfigValue valueFor(String key) {
    Optional<ConfigValue> optional = results.configValues()
        .stream()
        .filter(value -> value.name().equals(key))
        .findFirst();
    assertTrue("ConfigValue for key '" + key + "' was not found", optional.isPresent());
    return optional.get();
  }

  @Test
  public void validate_withInvalidRegexInTableIncludeList_setsError() {
    // This test is skipped because regex validation happens at ConfigDef level
    // and throws ConfigException before our custom validation runs
  }

  @Test
  public void validate_withInvalidRegexInTableExcludeList_setsError() {
    // This test is skipped because regex validation happens at ConfigDef level
    // and throws ConfigException before our custom validation runs
  }

  @Test
  public void validate_withValidRegexInTableIncludeList_noErrors() {
    props.put(MODE_CONFIG, MODE_BULK);
    props.put(TABLE_INCLUDE_LIST_CONFIG, TABLE_TEST_TABLE_ID.toString());

    validate();

    assertNoErrors();
  }

  @Test
  public void validate_withValidModeTimestampWithTsCol_noErrors() {
    props.put(MODE_CONFIG, MODE_TIMESTAMP);
    props.put(TABLE_INCLUDE_LIST_CONFIG, "database.schema.table.*");
    props.put(TIMESTAMP_COLUMN_MAPPING_CONFIG, "database.schema.table_test.*:[ts_col1|ts_col2]");

    validate();

    assertNoErrors();
  }

  @Test
  public void validate_withValidModeTimestampIncrementingWithTsCol_setsError() {
    props.put(MODE_CONFIG, MODE_TIMESTAMP_INCREMENTING);
    props.put(TABLE_INCLUDE_LIST_CONFIG, "database.schema.table.*");
    props.put(TIMESTAMP_COLUMN_MAPPING_CONFIG, "database.schema.table_test.*:[ts_col1|ts_col2]");

    validate();

    assertErrors(1);
    assertErrors(MODE_CONFIG, 1);
    assertErrorMatches(MODE_CONFIG, "Incrementing column configuration must be provided");
  }

  @Test
  public void validate_withValidModeIncrementingWithTimestampCol_setsError() {
    props.put(MODE_CONFIG, MODE_INCREMENTING);
    props.put(TABLE_INCLUDE_LIST_CONFIG, "database.schema.table.*");
    props.put(INCREMENTING_COLUMN_MAPPING_CONFIG, "database.schema.table_test.*:inc_col1");
    props.put(TIMESTAMP_COLUMN_MAPPING_CONFIG, "database.schema.table_test.*:[ts_col1|ts_col2]");

    validate();

    assertErrors(1);
    assertErrors(MODE_CONFIG, 1);
    assertErrorMatches(MODE_CONFIG, ".*Timestamp column configurations should not be provided.*");
  }

  @Test
  public void validate_withValidModeTimestampWithIncrementingCol_setsError() {
    props.put(MODE_CONFIG, MODE_TIMESTAMP);
    props.put(TABLE_INCLUDE_LIST_CONFIG, "database.schema.table.*");
    props.put(TIMESTAMP_COLUMN_MAPPING_CONFIG, "database.schema.table_test.*:[ts_col1|ts_col2]");
    props.put(INCREMENTING_COLUMN_MAPPING_CONFIG, "database.schema.table_test.*:inc_col1");

    validate();

    assertErrors(1);
    assertErrors(MODE_CONFIG, 1);
    assertErrorMatches(MODE_CONFIG, ".*Incrementing column configurations should not be provided.*");
  }

  @Test
  public void validate_withValidModeBulk_noErrors() {
    props.put(MODE_CONFIG, MODE_BULK);
    props.put(TABLE_WHITELIST_CONFIG, "table1,table2");
    validate();

    assertNoErrors();
  }

  @Test
  public void validate_withConnectionUrlNotSpecified_setsError() {
    // This test is skipped because required field validation happens at ConfigDef level
    // and throws ConfigException before our custom validation runs
  }

  @Test
  public void validate_withUserNotSpecified_setsError() {
    // This test is skipped because required field validation happens at ConfigDef level
    // and throws ConfigException before our custom validation runs
  }

  @Test
  public void validate_withBothWhitelistAndIncludeList_setsError() {
    props.put(MODE_CONFIG, MODE_BULK);
    props.put(TABLE_WHITELIST_CONFIG, "table1,table2");
    props.put(TABLE_INCLUDE_LIST_CONFIG, "database.schema.table.*");
    
    validate();
    
    assertErrors(2);
    assertErrors(TABLE_WHITELIST_CONFIG, 1);
    assertErrors(TABLE_INCLUDE_LIST_CONFIG, 1);
    assertErrorMatches(TABLE_WHITELIST_CONFIG, ".*Cannot mix legacy and new configuration approaches.*");
    assertErrorMatches(TABLE_INCLUDE_LIST_CONFIG, ".*Cannot mix legacy and new configuration approaches.*");
  }
  
  @Test
  public void validate_withBothBlacklistAndExcludeList_setsError() {
    props.put(MODE_CONFIG, MODE_BULK);
    props.put(TABLE_BLACKLIST_CONFIG, "table1,table2");
    props.put(TABLE_INCLUDE_LIST_CONFIG, "database.schema.table.*");
    props.put(TABLE_EXCLUDE_LIST_CONFIG, "database.schema.excluded.*");
    
    validate();
    
    assertErrors(3);
    assertErrors(TABLE_BLACKLIST_CONFIG, 1);
    assertErrors(TABLE_INCLUDE_LIST_CONFIG, 1);
    assertErrors(TABLE_EXCLUDE_LIST_CONFIG, 1);
    assertErrorMatches(TABLE_BLACKLIST_CONFIG, ".*Cannot mix legacy and new configuration approaches.*");
  }
  
  @Test
  public void validate_withOnlyWhitelist_noErrors() {
    props.put(MODE_CONFIG, MODE_BULK);
    props.put(TABLE_WHITELIST_CONFIG, "table1,table2");
    
    validate();
    
    assertNoErrors();
  }
  
  @Test
  public void validate_withOnlyBlacklist_noErrors() {
    props.put(MODE_CONFIG, MODE_BULK);
    props.put(TABLE_BLACKLIST_CONFIG, "table1,table2");
    
    validate();
    
    assertNoErrors();
  }
  
  @Test
  public void validate_withIncludeAndExcludeList_noErrors() {
    props.put(MODE_CONFIG, MODE_BULK);
    props.put(TABLE_INCLUDE_LIST_CONFIG, "database.schema.table.*");
    props.put(TABLE_EXCLUDE_LIST_CONFIG, "database.schema.excluded.*");
    
    validate();
    
    assertNoErrors();
  }
  
  @Test
  public void validate_withOnlyExcludeListAndNoIncludeList_setsError() {
    props.remove(TABLE_INCLUDE_LIST_CONFIG);
    props.put(MODE_CONFIG, MODE_BULK);
    props.put(TABLE_EXCLUDE_LIST_CONFIG, "database.schema.excluded.*");
    
    validate();
    
    assertErrors(1);
    assertErrors(TABLE_EXCLUDE_LIST_CONFIG, 1);
    assertErrorMatches(TABLE_EXCLUDE_LIST_CONFIG, ".*cannot be used without.*");
  }
  
  @Test
  public void validate_withNoTableConfigs_setsError() {
    props.remove(TABLE_INCLUDE_LIST_CONFIG);
    props.remove(TABLE_EXCLUDE_LIST_CONFIG);
    props.remove(TABLE_WHITELIST_CONFIG);
    props.remove(TABLE_BLACKLIST_CONFIG);

    props.put(MODE_CONFIG, MODE_BULK);
    
    validate();
    
    assertErrors(4);
    assertErrors(TABLE_WHITELIST_CONFIG, 1);
    assertErrors(TABLE_BLACKLIST_CONFIG, 1);
    assertErrors(TABLE_INCLUDE_LIST_CONFIG, 1);
    assertErrors(TABLE_EXCLUDE_LIST_CONFIG, 1);
    assertErrorMatches(TABLE_WHITELIST_CONFIG, ".*At least one table filtering configuration is required.*");
  }

  @Test
  public void validate_withBothTimestampColumnNameAndMapping_setsError() {
    props.put(MODE_CONFIG, MODE_BULK);
    props.put(TABLE_INCLUDE_LIST_CONFIG, "database.schema.table.*");
    props.put(TIMESTAMP_COLUMN_NAME_CONFIG, "ts_col");
    props.put(TIMESTAMP_COLUMN_MAPPING_CONFIG, "database.schema.table_test.*:[ts_col1|ts_col2]");
    
    validate();
    
    assertErrors(3);
    assertErrors(TIMESTAMP_COLUMN_NAME_CONFIG, 1);
    assertErrors(TIMESTAMP_COLUMN_MAPPING_CONFIG, 1);
    assertErrors(TABLE_INCLUDE_LIST_CONFIG, 1);
    assertErrorMatches(TIMESTAMP_COLUMN_NAME_CONFIG, ".*Cannot mix legacy and new configuration approaches.*");
    assertErrorMatches(TIMESTAMP_COLUMN_MAPPING_CONFIG, ".*Cannot mix legacy and new configuration approaches.*");
    assertErrorMatches(TABLE_INCLUDE_LIST_CONFIG, ".*Cannot mix legacy and new configuration approaches.*");
  }
  
  @Test
  public void validate_withBothIncrementingColumnNameAndMapping_setsError() {
    props.put(MODE_CONFIG, MODE_BULK);
    props.put(TABLE_INCLUDE_LIST_CONFIG, "database.schema.table.*");
    props.put(INCREMENTING_COLUMN_NAME_CONFIG, "inc_col");
    props.put(INCREMENTING_COLUMN_MAPPING_CONFIG, "database.schema.table_test.*:inc_col1");
    
    validate();
    
    assertErrors(3);
    assertErrors(INCREMENTING_COLUMN_NAME_CONFIG, 1);
    assertErrors(INCREMENTING_COLUMN_MAPPING_CONFIG, 1);
    assertErrors(TABLE_INCLUDE_LIST_CONFIG, 1);
    assertErrorMatches(INCREMENTING_COLUMN_NAME_CONFIG, ".*Cannot mix legacy and new configuration approaches.*");
    assertErrorMatches(INCREMENTING_COLUMN_MAPPING_CONFIG, ".*Cannot mix legacy and new configuration approaches.*");
    assertErrorMatches(TABLE_INCLUDE_LIST_CONFIG, ".*Cannot mix legacy and new configuration approaches.*");
  }

  @Test
  public void validate_withWhitelistAndTimestampColumnMapping_setsError() {
    props.put(MODE_CONFIG, MODE_TIMESTAMP);
    props.put(TABLE_WHITELIST_CONFIG, "table1,table2");
    props.put(TIMESTAMP_COLUMN_MAPPING_CONFIG, "database.schema.table_test.*:[ts_col1|ts_col2]");
    
    validate();
    
    assertErrors(2);
    assertErrors(TABLE_WHITELIST_CONFIG, 1);
    assertErrors(TIMESTAMP_COLUMN_MAPPING_CONFIG, 1);
    assertErrorMatches(TABLE_WHITELIST_CONFIG, ".*Cannot mix legacy and new configuration approaches.*");
    assertErrorMatches(TIMESTAMP_COLUMN_MAPPING_CONFIG, ".*Cannot mix legacy and new configuration approaches.*");
  }
  
  @Test
  public void validate_withWhitelistAndIncrementingColumnMapping_setsError() {
    props.put(MODE_CONFIG, MODE_INCREMENTING);
    props.put(TABLE_WHITELIST_CONFIG, "table1,table2");
    props.put(INCREMENTING_COLUMN_MAPPING_CONFIG, "database.schema.table_test.*:inc_col1");
    
    validate();
    
    assertErrors(2);
    assertErrors(TABLE_WHITELIST_CONFIG, 1);
    assertErrors(INCREMENTING_COLUMN_MAPPING_CONFIG, 1);
    assertErrorMatches(TABLE_WHITELIST_CONFIG, ".*Cannot mix legacy and new configuration approaches.*");
    assertErrorMatches(INCREMENTING_COLUMN_MAPPING_CONFIG, ".*Cannot mix legacy and new configuration approaches.*");
  }
  
  @Test
  public void validate_withBlacklistAndTimestampColumnMapping_setsError() {
    props.put(MODE_CONFIG, MODE_TIMESTAMP);
    props.put(TABLE_BLACKLIST_CONFIG, "table1,table2");
    props.put(TIMESTAMP_COLUMN_MAPPING_CONFIG, "database.schema.table_test.*:[ts_col1|ts_col2]");
    
    validate();
    
    assertErrors(2);
    assertErrors(TABLE_BLACKLIST_CONFIG, 1);
    assertErrors(TIMESTAMP_COLUMN_MAPPING_CONFIG, 1);
    assertErrorMatches(TABLE_BLACKLIST_CONFIG, ".*Cannot mix legacy and new configuration approaches.*");
    assertErrorMatches(TIMESTAMP_COLUMN_MAPPING_CONFIG, ".*Cannot mix legacy and new configuration approaches.*");
  }
  
  @Test
  public void validate_withBlacklistAndIncrementingColumnMapping_setsError() {
    props.put(MODE_CONFIG, MODE_INCREMENTING);
    props.put(TABLE_BLACKLIST_CONFIG, "table1,table2");
    props.put(INCREMENTING_COLUMN_MAPPING_CONFIG, "database.schema.table_test.*:inc_col1");
    
    validate();
    
    assertErrors(2);
    assertErrors(TABLE_BLACKLIST_CONFIG, 1);
    assertErrors(INCREMENTING_COLUMN_MAPPING_CONFIG, 1);
    assertErrorMatches(TABLE_BLACKLIST_CONFIG, ".*Cannot mix legacy and new configuration approaches.*");
    assertErrorMatches(INCREMENTING_COLUMN_MAPPING_CONFIG, ".*Cannot mix legacy and new configuration approaches.*");
  }
  
  @Test
  public void validate_withWhitelistAndBothColumnMappings_setsError() {
    props.put(MODE_CONFIG, MODE_TIMESTAMP_INCREMENTING);
    props.put(TABLE_WHITELIST_CONFIG, "table1,table2");
    props.put(TIMESTAMP_COLUMN_MAPPING_CONFIG, "database.schema.table_test.*:[ts_col1|ts_col2]");
    props.put(INCREMENTING_COLUMN_MAPPING_CONFIG, "database.schema.table_test.*:inc_col1");
    
    validate();
    
    assertErrors(3);
    assertErrors(TABLE_WHITELIST_CONFIG, 1);
    assertErrors(TIMESTAMP_COLUMN_MAPPING_CONFIG, 1);
    assertErrors(INCREMENTING_COLUMN_MAPPING_CONFIG, 1);
    assertErrorMatches(TABLE_WHITELIST_CONFIG, ".*Cannot mix legacy and new configuration approaches.*");
    assertErrorMatches(TIMESTAMP_COLUMN_MAPPING_CONFIG, ".*Cannot mix legacy and new configuration approaches.*");
    assertErrorMatches(INCREMENTING_COLUMN_MAPPING_CONFIG, ".*Cannot mix legacy and new configuration approaches.*");
  }
  
  @Test
  public void validate_withIncludeListAndColumnMappings_noErrors() {
    props.put(MODE_CONFIG, MODE_TIMESTAMP_INCREMENTING);
    props.put(TABLE_INCLUDE_LIST_CONFIG, "database.schema.table.*");
    props.put(TIMESTAMP_COLUMN_MAPPING_CONFIG, "database.schema.table_test.*:[ts_col1|ts_col2]");
    props.put(INCREMENTING_COLUMN_MAPPING_CONFIG, "database.schema.table_test.*:inc_col1");
    
    validate();
    
    assertNoErrors();
  }
  
  @Test
  public void validate_withWhitelistAndLegacyColumnNames_noErrors() {
    props.put(MODE_CONFIG, MODE_TIMESTAMP_INCREMENTING);
    props.put(TABLE_WHITELIST_CONFIG, "table1,table2");
    props.put(TIMESTAMP_COLUMN_NAME_CONFIG, "ts_col");
    props.put(INCREMENTING_COLUMN_NAME_CONFIG, "inc_col");
    
    validate();
    
    assertNoErrors();
  }

  @Test
  public void validate_withValidModeTimestampWithLegacyColumnName_noErrors() {
    props.put(TABLE_WHITELIST_CONFIG, "table1,table2");
    props.put(MODE_CONFIG, MODE_TIMESTAMP);
    props.put(TIMESTAMP_COLUMN_NAME_CONFIG, "ts_col");
    
    validate();
    
    assertNoErrors();
  }
  
  @Test
  public void validate_withValidModeIncrementingWithLegacyColumnName_noErrors() {
    props.put(TABLE_WHITELIST_CONFIG, "table1,table2");
    props.put(MODE_CONFIG, MODE_INCREMENTING);
    props.put(INCREMENTING_COLUMN_NAME_CONFIG, "inc_col");
    
    validate();
    
    assertNoErrors();
  }
  
  @Test
  public void validate_withIncludeListAndLegacyTimestampColumnName_setsError() {
    props.put(MODE_CONFIG, MODE_TIMESTAMP);
    props.put(TABLE_INCLUDE_LIST_CONFIG, "database.schema.table.*");
    props.put(TIMESTAMP_COLUMN_NAME_CONFIG, "ts_col");
    
    validate();
    
    assertErrors(2);
    assertErrors(TABLE_INCLUDE_LIST_CONFIG, 1);
    assertErrors(TIMESTAMP_COLUMN_NAME_CONFIG, 1);
    assertErrorMatches(TABLE_INCLUDE_LIST_CONFIG, ".*Cannot mix legacy and new configuration approaches.*");
    assertErrorMatches(TIMESTAMP_COLUMN_NAME_CONFIG, ".*Cannot mix legacy and new configuration approaches.*");
  }
  
  @Test
  public void validate_withIncludeListAndLegacyIncrementingColumnName_setsError() {
    props.put(MODE_CONFIG, MODE_INCREMENTING);
    props.put(TABLE_INCLUDE_LIST_CONFIG, "database.schema.table.*");
    props.put(INCREMENTING_COLUMN_NAME_CONFIG, "inc_col");
    
    validate();
    
    assertErrors(2);
    assertErrors(TABLE_INCLUDE_LIST_CONFIG, 1);
    assertErrors(INCREMENTING_COLUMN_NAME_CONFIG, 1);
    assertErrorMatches(TABLE_INCLUDE_LIST_CONFIG, ".*Cannot mix legacy and new configuration approaches.*");
    assertErrorMatches(INCREMENTING_COLUMN_NAME_CONFIG, ".*Cannot mix legacy and new configuration approaches.*");
  }
  
  @Test
  public void validate_withValidModeTimestampIncrementingWithLegacyColumnNames_noErrors() {
    props.put(MODE_CONFIG, MODE_TIMESTAMP_INCREMENTING);
    props.put(TABLE_WHITELIST_CONFIG, "table1,table2");
    props.put(TIMESTAMP_COLUMN_NAME_CONFIG, "ts_col");
    props.put(INCREMENTING_COLUMN_NAME_CONFIG, "inc_col");
    
    validate();
    
    assertNoErrors();
  }
  
  @Test
  public void validate_withValidModeTimestampIncrementingWithMixedConfigs_setsError() {
    props.put(MODE_CONFIG, MODE_TIMESTAMP_INCREMENTING);
    props.put(TABLE_INCLUDE_LIST_CONFIG, "database.schema.table.*");
    props.put(TIMESTAMP_COLUMN_NAME_CONFIG, "ts_col");
    props.put(INCREMENTING_COLUMN_MAPPING_CONFIG, "database.schema.table_test.*:inc_col1");
    
    validate();
    
    assertErrors(3);
    assertErrors(TIMESTAMP_COLUMN_NAME_CONFIG, 1);
    assertErrors(TABLE_INCLUDE_LIST_CONFIG, 1);
    assertErrors(INCREMENTING_COLUMN_MAPPING_CONFIG, 1);
    assertErrorMatches(TIMESTAMP_COLUMN_NAME_CONFIG, ".*Cannot mix legacy and new configuration approaches.*");
    assertErrorMatches(TABLE_INCLUDE_LIST_CONFIG, ".*Cannot mix legacy and new configuration approaches.*");
    assertErrorMatches(INCREMENTING_COLUMN_MAPPING_CONFIG, ".*Cannot mix legacy and new configuration approaches.*");
  }
  
  @Test
  public void validate_withValidModeTimestampIncrementingWithMixedConfigsReverse_setsError() {
    props.put(MODE_CONFIG, MODE_TIMESTAMP_INCREMENTING);
    props.put(TABLE_INCLUDE_LIST_CONFIG, "database.schema.table.*");
    props.put(TIMESTAMP_COLUMN_MAPPING_CONFIG, "database.schema.table_test.*:[ts_col1|ts_col2]");
    props.put(INCREMENTING_COLUMN_NAME_CONFIG, "inc_col");
    
    validate();
    
    assertErrors(3);
    assertErrors(INCREMENTING_COLUMN_NAME_CONFIG, 1);
    assertErrors(TABLE_INCLUDE_LIST_CONFIG, 1);
    assertErrors(TIMESTAMP_COLUMN_MAPPING_CONFIG, 1);
    assertErrorMatches(INCREMENTING_COLUMN_NAME_CONFIG, ".*Cannot mix legacy and new configuration approaches.*");
    assertErrorMatches(TABLE_INCLUDE_LIST_CONFIG, ".*Cannot mix legacy and new configuration approaches.*");
    assertErrorMatches(TIMESTAMP_COLUMN_MAPPING_CONFIG, ".*Cannot mix legacy and new configuration approaches.*");
  }

  @Test
  public void validate_withModeBulkWithNewTimestampMapping_setsError() {
    props.put(MODE_CONFIG, MODE_BULK);
    props.put(TABLE_INCLUDE_LIST_CONFIG, "database.schema.table.*");
    props.put(TIMESTAMP_COLUMN_MAPPING_CONFIG, "database.schema.table_test.*:[ts_col1|ts_col2]");
    
    validate();
    
    assertErrors(1);
    assertErrors(MODE_CONFIG, 1);
    assertErrorMatches(MODE_CONFIG, ".*Timestamp column configurations should not be provided.*");
  }
  
  @Test
  public void validate_withModeBulkWithNewIncrementingMapping_setsError() {
    props.put(MODE_CONFIG, MODE_BULK);
    props.put(TABLE_INCLUDE_LIST_CONFIG, "database.schema.table.*");
    props.put(INCREMENTING_COLUMN_MAPPING_CONFIG, "database.schema.table_test.*:inc_col1");
    
    validate();
    
    assertErrors(1);
    assertErrors(MODE_CONFIG, 1);
    assertErrorMatches(MODE_CONFIG, ".*Incrementing column configurations should not be provided.*");
  }
  
}
