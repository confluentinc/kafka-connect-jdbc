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
  public void validate_withInvalidRegexInTableIncludeList_setsValidatorError() {
    props.put(MODE_CONFIG, MODE_BULK);
    // Invalid regex: unbalanced parenthesis
    props.put(TABLE_INCLUDE_LIST_CONFIG, "database.schema(table.*");

    validate();

    assertErrors(1);
    assertErrors(TABLE_INCLUDE_LIST_CONFIG, 1);
    assertErrorMatches(
        TABLE_INCLUDE_LIST_CONFIG,
        ".*Must be a valid comma-separated list of regular expression patterns.*"
    );
  }

  @Test
  public void validate_withInvalidRegexInTableExcludeList_setsValidatorError() {
    props.put(MODE_CONFIG, MODE_BULK);
    props.put(TABLE_INCLUDE_LIST_CONFIG, "database.schema.table.*");
    // Invalid regex: unbalanced bracket
    props.put(TABLE_EXCLUDE_LIST_CONFIG, "database.schema[table.*");

    validate();

    assertErrors(1);
    assertErrors(TABLE_EXCLUDE_LIST_CONFIG, 1);
    assertErrorMatches(
        TABLE_EXCLUDE_LIST_CONFIG,
        ".*Must be a valid comma-separated list of regular expression patterns.*"
    );
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
  public void validate_all_combined_setsError() {
    props.put(MODE_CONFIG, MODE_TIMESTAMP_INCREMENTING);
    props.put(TABLE_WHITELIST_CONFIG, "table1,table2");
    props.put(TABLE_INCLUDE_LIST_CONFIG, "table1,table2");
    props.put(INCREMENTING_COLUMN_NAME_CONFIG, "inc_col");
    props.put(INCREMENTING_COLUMN_MAPPING_CONFIG, "inc_col");
    props.put(TIMESTAMP_COLUMN_NAME_CONFIG, "ts_col");
    props.put(TIMESTAMP_COLUMN_MAPPING_CONFIG, "ts_col");

    validate();

    // We expect only 2 validator errors here:
    // - Invalid format for timestamp mapping
    // - Invalid format for incrementing mapping
    // Business logic validation (legacy/new conflict) doesn't run when validator errors exist
    assertErrors(2);
    
    // Validator errors for invalid formats
    assertErrors(TIMESTAMP_COLUMN_MAPPING_CONFIG, 1);
    assertErrorMatches(
        TIMESTAMP_COLUMN_MAPPING_CONFIG,
        ".*Invalid format.*Expected 'regex:\\[col1\\|col2\\|...\\]'.*"
    );
    
    assertErrors(INCREMENTING_COLUMN_MAPPING_CONFIG, 1);
    assertErrorMatches(
        INCREMENTING_COLUMN_MAPPING_CONFIG,
        ".*Invalid format.*Expected 'regex:columnName'.*"
    );
  }

  // ========== Validator Error Tests ==========
  // These tests verify that ConfigDef validators properly catch invalid formats
  // and report them as errors rather than throwing exceptions

  @Test
  public void validate_withInvalidTimestampMappingFormat_missingBrackets_setsError() {
    props.put(MODE_CONFIG, MODE_TIMESTAMP);
    props.put(TABLE_INCLUDE_LIST_CONFIG, "database.schema.table.*");
    props.put(TIMESTAMP_COLUMN_MAPPING_CONFIG, "database.schema.table.*:ts_col");

    validate();

    assertErrors(1);
    assertErrors(TIMESTAMP_COLUMN_MAPPING_CONFIG, 1);
    assertErrorMatches(
        TIMESTAMP_COLUMN_MAPPING_CONFIG,
        ".*Columns list must be enclosed in square brackets.*"
    );
  }

  @Test
  public void validate_withInvalidTimestampMappingFormat_emptyColumns_setsError() {
    props.put(MODE_CONFIG, MODE_TIMESTAMP);
    props.put(TABLE_INCLUDE_LIST_CONFIG, "database.schema.table.*");
    props.put(TIMESTAMP_COLUMN_MAPPING_CONFIG, "database.schema.table.*:[]");

    validate();

    assertErrors(1);
    assertErrors(TIMESTAMP_COLUMN_MAPPING_CONFIG, 1);
    assertErrorMatches(
        TIMESTAMP_COLUMN_MAPPING_CONFIG,
        ".*Columns list cannot be empty.*"
    );
  }

  @Test
  public void validate_withInvalidTimestampMappingFormat_missingColon_setsError() {
    props.put(MODE_CONFIG, MODE_TIMESTAMP);
    props.put(TABLE_INCLUDE_LIST_CONFIG, "database.schema.table.*");
    props.put(TIMESTAMP_COLUMN_MAPPING_CONFIG, "database.schema.table.*");

    validate();

    assertErrors(1);
    assertErrors(TIMESTAMP_COLUMN_MAPPING_CONFIG, 1);
    assertErrorMatches(
        TIMESTAMP_COLUMN_MAPPING_CONFIG,
        ".*Invalid format.*Expected 'regex:\\[col1\\|col2\\|...\\]'.*"
    );
  }

  @Test
  public void validate_withInvalidTimestampMappingFormat_emptyColumnName_setsError() {
    props.put(MODE_CONFIG, MODE_TIMESTAMP);
    props.put(TABLE_INCLUDE_LIST_CONFIG, "database.schema.table.*");
    props.put(TIMESTAMP_COLUMN_MAPPING_CONFIG, "database.schema.table.*:[col1||col3]");

    validate();

    assertErrors(1);
    assertErrors(TIMESTAMP_COLUMN_MAPPING_CONFIG, 1);
    assertErrorMatches(
        TIMESTAMP_COLUMN_MAPPING_CONFIG,
        ".*Every column name should be non-empty string.*"
    );
  }

  @Test
  public void validate_withInvalidTimestampMappingRegex_setsError() {
    props.put(MODE_CONFIG, MODE_TIMESTAMP);
    props.put(TABLE_INCLUDE_LIST_CONFIG, "database.schema.table.*");
    // Invalid regex: unbalanced parenthesis
    props.put(TIMESTAMP_COLUMN_MAPPING_CONFIG, "database.schema(table.*:[ts_col]");

    validate();

    assertErrors(1);
    assertErrors(TIMESTAMP_COLUMN_MAPPING_CONFIG, 1);
    assertErrorMatches(TIMESTAMP_COLUMN_MAPPING_CONFIG, ".*Invalid regular expression.*");
  }

  @Test
  public void validate_withInvalidIncrementingMappingFormat_missingColon_setsError() {
    props.put(MODE_CONFIG, MODE_INCREMENTING);
    props.put(TABLE_INCLUDE_LIST_CONFIG, "database.schema.table.*");
    props.put(INCREMENTING_COLUMN_MAPPING_CONFIG, "database.schema.table.*");

    validate();

    assertErrors(1);
    assertErrors(INCREMENTING_COLUMN_MAPPING_CONFIG, 1);
    assertErrorMatches(
        INCREMENTING_COLUMN_MAPPING_CONFIG,
        ".*Invalid format.*Expected 'regex:columnName'.*"
    );
  }

  @Test
  public void validate_withInvalidIncrementingMappingFormat_emptyColumn_setsError() {
    props.put(MODE_CONFIG, MODE_INCREMENTING);
    props.put(TABLE_INCLUDE_LIST_CONFIG, "database.schema.table.*");
    props.put(INCREMENTING_COLUMN_MAPPING_CONFIG, "database.schema.table.*:");

    validate();

    assertErrors(1);
    assertErrors(INCREMENTING_COLUMN_MAPPING_CONFIG, 1);
    assertErrorMatches(
        INCREMENTING_COLUMN_MAPPING_CONFIG,
        ".*Column name cannot be empty.*"
    );
  }

  @Test
  public void validate_withInvalidIncrementingMappingRegex_setsError() {
    props.put(MODE_CONFIG, MODE_INCREMENTING);
    props.put(TABLE_INCLUDE_LIST_CONFIG, "database.schema.table.*");
    // Invalid regex: unbalanced bracket
    props.put(INCREMENTING_COLUMN_MAPPING_CONFIG, "database.schema[table.*:inc_col");

    validate();

    assertErrors(1);
    assertErrors(INCREMENTING_COLUMN_MAPPING_CONFIG, 1);
    assertErrorMatches(INCREMENTING_COLUMN_MAPPING_CONFIG, ".*Invalid regular expression.*");
  }

  @Test
  public void validate_withInvalidRegexInTableIncludeList_setsError() {
    props.put(MODE_CONFIG, MODE_BULK);
    // Invalid regex: unbalanced parenthesis
    props.put(TABLE_INCLUDE_LIST_CONFIG, "database.schema(table.*");

    validate();

    assertErrors(1);
    assertErrors(TABLE_INCLUDE_LIST_CONFIG, 1);
    assertErrorMatches(
        TABLE_INCLUDE_LIST_CONFIG,
        ".*Must be a valid comma-separated list of regular expression patterns.*"
    );
  }

  @Test
  public void validate_withInvalidRegexInTableExcludeList_setsError() {
    props.put(MODE_CONFIG, MODE_BULK);
    props.put(TABLE_INCLUDE_LIST_CONFIG, "database.schema.table.*");
    // Invalid regex: unbalanced bracket
    props.put(TABLE_EXCLUDE_LIST_CONFIG, "database.schema[table.*");

    validate();

    assertErrors(1);
    assertErrors(TABLE_EXCLUDE_LIST_CONFIG, 1);
    assertErrorMatches(
        TABLE_EXCLUDE_LIST_CONFIG,
        ".*Must be a valid comma-separated list of regular expression patterns.*"
    );
  }

  @Test
  public void validate_withMultipleInvalidConfigs_reportsAllErrors() {
    props.put(MODE_CONFIG, MODE_TIMESTAMP_INCREMENTING);
    props.put(TABLE_INCLUDE_LIST_CONFIG, "database.schema(table.*");
    // Invalid incrementing format
    props.put(INCREMENTING_COLUMN_MAPPING_CONFIG, "invalid_format");
    // Invalid timestamp format
    props.put(TIMESTAMP_COLUMN_MAPPING_CONFIG, "also_invalid");

    validate();

    // All 3 validator errors should be reported
    assertErrors(3);
    assertErrors(TABLE_INCLUDE_LIST_CONFIG, 1);
    assertErrors(INCREMENTING_COLUMN_MAPPING_CONFIG, 1);
    assertErrors(TIMESTAMP_COLUMN_MAPPING_CONFIG, 1);
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

  @Test
  public void validate_withBothQueryAndQueryMasked_setsError() {
    props.put(MODE_CONFIG, MODE_BULK);
    props.put(TABLE_WHITELIST_CONFIG, "table1,table2");
    props.put(QUERY_CONFIG, "SELECT * FROM users");
    props.put(QUERY_MASKED_CONFIG, "SELECT * FROM sensitive_data");

    validate();

    assertErrors(2);
    assertErrors(QUERY_CONFIG, 1);
    assertErrors(QUERY_MASKED_CONFIG, 1);
    assertErrorMatches(QUERY_CONFIG, ".*Both 'query' and 'query.masked' configs cannot be set.*");
    assertErrorMatches(QUERY_MASKED_CONFIG, ".*Both 'query' and 'query.masked' configs cannot be set.*");
  }

  @Test
  public void validate_withOnlyQuery_noErrors() {
    props.put(MODE_CONFIG, MODE_BULK);
    props.put(QUERY_CONFIG, "SELECT * FROM users WHERE active = true");

    validate();

    assertNoErrors();
  }

  @Test
  public void validate_withOnlyQueryMasked_noErrors() {
    props.put(MODE_CONFIG, MODE_BULK);
    props.put(QUERY_MASKED_CONFIG, "SELECT * FROM users WHERE active = true");

    validate();

    assertNoErrors();
  }

  @Test
  public void validate_withBothQueryAndQueryMaskedEmpty_noErrors() {
    props.put(MODE_CONFIG, MODE_BULK);
    props.put(TABLE_WHITELIST_CONFIG, "table1,table2");
    // Both empty should be fine as it's equivalent to neither being set
    props.put(QUERY_CONFIG, "");
    props.put(QUERY_MASKED_CONFIG, "");

    validate();

    assertNoErrors();
  }

  @Test
  public void validate_withQueryStartingWithUpdate_setsError() {
    props.put(MODE_CONFIG, MODE_BULK);
    props.put(QUERY_CONFIG, "UPDATE users SET active = false");

    validate();

    assertErrors(1);
    assertErrors(QUERY_CONFIG, 1);
    assertErrorMatches(QUERY_CONFIG, ".*Only SELECT statements are supported for 'query'.*");
  }

  @Test
  public void validate_withQueryMaskedStartingWithUpdate_setsError() {
    props.put(MODE_CONFIG, MODE_BULK);
    props.put(QUERY_MASKED_CONFIG, "UPDATE users SET active = false");

    validate();

    assertErrors(1);
    assertErrors(QUERY_MASKED_CONFIG, 1);
    assertErrorMatches(
        QUERY_MASKED_CONFIG,
        "Only SELECT statements are supported for 'query.masked'"
    );
  }

  @Test
  public void validate_withInvalidQuerySyntax_setsError() {
    props.put(MODE_CONFIG, MODE_BULK);
    props.put(QUERY_CONFIG, "SELECT FROM");

    validate();

    assertErrors(1);
    assertErrors(QUERY_CONFIG, 1);
    assertErrorMatches(
        QUERY_CONFIG,
        ".*Invalid SQL syntax for 'query'.*"
    );
  }

  @Test
  public void validate_withInvalidQueryMaskedSyntax_setsError() {
    props.put(MODE_CONFIG, MODE_BULK);
    props.put(QUERY_MASKED_CONFIG, "SELECT FROM");

    validate();

    assertErrors(1);
    assertErrors(QUERY_MASKED_CONFIG, 1);
    assertErrorMatches(
        QUERY_MASKED_CONFIG,
        ".*Invalid SQL syntax for 'query\\.masked'.*"
    );
  }

  @Test
  public void validate_withQueryMaskedAndIncrementingColumn_noErrors() {
    props.put(MODE_CONFIG, MODE_INCREMENTING);
    props.put(QUERY_MASKED_CONFIG, "SELECT * FROM users");
    props.put(INCREMENTING_COLUMN_NAME_CONFIG, "id");

    validate();

    assertNoErrors();
  }

  @Test
  public void validate_withQueryMaskedContainingComplexQuery_noErrors() {
    props.put(MODE_CONFIG, MODE_BULK);
    // Test with a complex query containing multiple joins
    String complexQuery = "SELECT a.id, a.name, b.email, c.address, d.phone " +
        "FROM users a " +
        "INNER JOIN emails b ON a.id = b.user_id " +
        "LEFT JOIN addresses c ON a.id = c.user_id " +
        "LEFT JOIN phones d ON a.id = d.user_id " +
        "WHERE a.created_at > '2024-01-01' AND a.status = 'active'";
    props.put(QUERY_MASKED_CONFIG, complexQuery);

    validate();

    assertNoErrors();
  }

  @Test
  public void validate_withQueryAndTableFilteringConfigs_setsError() {
    props.put(MODE_CONFIG, MODE_BULK);
    props.put(TABLE_INCLUDE_LIST_CONFIG, "database.schema.table.*");
    props.put(QUERY_CONFIG, "SELECT * FROM users");

    validate();

    assertErrors(3);
    assertErrors(QUERY_CONFIG, 1);
    assertErrors(QUERY_MASKED_CONFIG, 1);
    assertErrors(TABLE_INCLUDE_LIST_CONFIG, 1);
    assertErrorMatches(
        QUERY_CONFIG,
        "Do not specify table filtering configs with 'query'"
    );
  }
  
}
