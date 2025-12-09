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

import io.confluent.connect.jdbc.util.DefaultJdbcCredentialsProvider;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Recommender;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.config.ConfigValue;
import org.easymock.EasyMock;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.api.easymock.PowerMock;
import org.powermock.api.easymock.annotation.Mock;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import io.confluent.connect.jdbc.source.JdbcSourceConnectorConfig.CachedRecommenderValues;
import io.confluent.connect.jdbc.source.JdbcSourceConnectorConfig.CachingRecommender;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

@RunWith(PowerMockRunner.class)
@PrepareForTest({Recommender.class})
@PowerMockIgnore("javax.management.*")
public class JdbcSourceConnectorConfigTest {

  private EmbeddedDerby db;
  private Map<String, String> props;
  private ConfigDef configDef;
  private List<ConfigValue> results;
  @Mock
  private Recommender mockRecommender;
  private MockTime time = new MockTime();

  @Before
  public void setup() throws Exception {
    configDef = null;
    results = null;
    props = new HashMap<>();

    db = new EmbeddedDerby();
    db.createTable("some_table", "id", "INT");

    db.execute("CREATE SCHEMA PUBLIC_SCHEMA");
    db.execute("SET SCHEMA PUBLIC_SCHEMA");
    db.createTable("public_table", "id", "INT");

    db.execute("CREATE SCHEMA PRIVATE_SCHEMA");
    db.execute("SET SCHEMA PRIVATE_SCHEMA");
    db.createTable("private_table", "id", "INT");
    db.createTable("another_private_table", "id", "INT");
  }

  @After
  public void cleanup() throws Exception {
    db.close();
    db.dropDatabase();
  }

  @Test
  public void testConnectionAttemptsAtLeastOne() {
    props.put(JdbcSourceConnectorConfig.CONNECTION_ATTEMPTS_CONFIG, "0");
    Map<String, ConfigValue> validatedConfig =
        JdbcSourceConnectorConfig.baseConfigDef().validateAll(props);
    ConfigValue connectionAttemptsConfig =
        validatedConfig.get(JdbcSourceConnectorConfig.CONNECTION_ATTEMPTS_CONFIG);
    assertNotNull(connectionAttemptsConfig);
    assertFalse(connectionAttemptsConfig.errorMessages().isEmpty());
  }

  @Test
  public void testConfigTableNameRecommenderWithoutSchemaOrTableTypes() throws Exception {
    props.put(JdbcSourceConnectorConfig.CONNECTION_URL_CONFIG, db.getUrl());
    configDef = JdbcSourceConnectorConfig.baseConfigDef();
    results = configDef.validate(props);
    // Should have no recommended values
    assertWhitelistRecommendations();
    assertBlacklistRecommendations();
  }

  @Test
  public void testConfigTableNameRecommenderWitSchemaAndWithoutTableTypes() throws Exception {
    props.put(JdbcSourceConnectorConfig.CONNECTION_URL_CONFIG, db.getUrl());
    props.put(JdbcSourceConnectorConfig.SCHEMA_PATTERN_CONFIG, "PRIVATE_SCHEMA");
    configDef = JdbcSourceConnectorConfig.baseConfigDef();
    results = configDef.validate(props);
    // Should have no recommended values
    assertWhitelistRecommendations();
    assertBlacklistRecommendations();
  }

  @Test
  public void testConfigTableNameRecommenderWithSchemaAndTableTypes() throws Exception {
    props.put(JdbcSourceConnectorConfig.CONNECTION_URL_CONFIG, db.getUrl());
    props.put(JdbcSourceConnectorConfig.SCHEMA_PATTERN_CONFIG, "PRIVATE_SCHEMA");
    props.put(JdbcSourceConnectorConfig.TABLE_TYPE_CONFIG, "VIEW");
    configDef = JdbcSourceConnectorConfig.baseConfigDef();
    results = configDef.validate(props);
    assertWhitelistRecommendations();
    assertBlacklistRecommendations();
  }

  @SuppressWarnings("unchecked")
  @Test
  public void testCachingRecommender() {
    final List<Object> results1 = Collections.singletonList((Object) "xyz");
    final List<Object> results2 = Collections.singletonList((Object) "123");
    // Set up the mock recommender to be called twice, returning different results each time
    EasyMock.expect(mockRecommender.validValues(EasyMock.anyObject(String.class), EasyMock.anyObject(Map.class))).andReturn(results1);
    EasyMock.expect(mockRecommender.validValues(EasyMock.anyObject(String.class), EasyMock.anyObject(Map.class))).andReturn(results2);

    PowerMock.replayAll();

    CachingRecommender recommender = new CachingRecommender(mockRecommender, time, 1000L);

    Map<String, Object> config1 = Collections.singletonMap("k", (Object) "v");
    // Populate the cache
    assertSame(results1, recommender.validValues("x", config1));
    // Try the cache before expiration
    time.sleep(100L);
    assertSame(results1, recommender.validValues("x", config1));
    // Wait for the cache to expire
    time.sleep(2000L);
    assertSame(results2, recommender.validValues("x", config1));

    PowerMock.verifyAll();
  }

  @Test
  public void testDefaultConstructedCachedTableValuesReturnsNull() {
    Map<String, Object> config = Collections.singletonMap("k", (Object) "v");
    CachedRecommenderValues cached = new CachedRecommenderValues();
    assertNull(cached.cachedValue(config, 20L));
  }

  @Test
  public void testCachedTableValuesReturnsCachedResultWithinExpiryTime() {
    Map<String, Object> config1 = Collections.singletonMap("k", (Object) "v");
    Map<String, Object> config2 = Collections.singletonMap("k", (Object) "v");
    List<Object> results = Collections.singletonList((Object) "xyz");
    long expiry = 20L;
    CachedRecommenderValues cached = new CachedRecommenderValues(config1, results, expiry);
    assertSame(results, cached.cachedValue(config2, expiry - 1L));
  }

  @Test
  public void testCachedTableValuesReturnsNullResultAtOrAfterExpiryTime() {
    Map<String, Object> config1 = Collections.singletonMap("k", (Object) "v");
    Map<String, Object> config2 = Collections.singletonMap("k", (Object) "v");
    List<Object> results = Collections.singletonList((Object) "xyz");
    long expiry = 20L;
    CachedRecommenderValues cached = new CachedRecommenderValues(config1, results, expiry);
    assertNull(cached.cachedValue(config2, expiry));
    assertNull(cached.cachedValue(config2, expiry + 1L));
  }

  @Test
  public void testCachedTableValuesReturnsNullResultIfConfigurationChanges() {
    Map<String, Object> config1 = Collections.singletonMap("k", (Object) "v");
    Map<String, Object> config2 = Collections.singletonMap("k", (Object) "zed");
    List<Object> results = Collections.singletonList((Object) "xyz");
    long expiry = 20L;
    CachedRecommenderValues cached = new CachedRecommenderValues(config1, results, expiry);
    assertNull(cached.cachedValue(config2, expiry - 1L));
    assertNull(cached.cachedValue(config2, expiry));
    assertNull(cached.cachedValue(config2, expiry + 1L));
  }

  @Test
  public void testSpacesInTopicPrefix() {
    props.put(JdbcSourceConnectorConfig.TOPIC_PREFIX_CONFIG, " withLeadingTailingSpaces ");
    Map<String, ConfigValue> validatedConfig =
        JdbcSourceConnectorConfig.baseConfigDef().validateAll(props);
    ConfigValue connectionAttemptsConfig =
        validatedConfig.get(JdbcSourceConnectorConfig.TOPIC_PREFIX_CONFIG);
    assertNotNull(connectionAttemptsConfig);
    assertTrue(connectionAttemptsConfig.errorMessages().isEmpty());

    props.put(JdbcSourceConnectorConfig.TOPIC_PREFIX_CONFIG, "with spaces");
    validatedConfig =
        JdbcSourceConnectorConfig.baseConfigDef().validateAll(props);
    connectionAttemptsConfig =
        validatedConfig.get(JdbcSourceConnectorConfig.TOPIC_PREFIX_CONFIG);
    assertNotNull(connectionAttemptsConfig);
    assertFalse(connectionAttemptsConfig.errorMessages().isEmpty());
  }

  @Test
  public void testInvalidCharsInTopicPrefix() {
    props.put(JdbcSourceConnectorConfig.TOPIC_PREFIX_CONFIG, "az_-.09");
    Map<String, ConfigValue> validatedConfig =
        JdbcSourceConnectorConfig.baseConfigDef().validateAll(props);
    ConfigValue connectionAttemptsConfig =
        validatedConfig.get(JdbcSourceConnectorConfig.TOPIC_PREFIX_CONFIG);
    assertNotNull(connectionAttemptsConfig);
    assertTrue(connectionAttemptsConfig.errorMessages().isEmpty());

    props.put(JdbcSourceConnectorConfig.TOPIC_PREFIX_CONFIG, "az_-.!@#$%^&*09");
    validatedConfig =
        JdbcSourceConnectorConfig.baseConfigDef().validateAll(props);
    connectionAttemptsConfig =
        validatedConfig.get(JdbcSourceConnectorConfig.TOPIC_PREFIX_CONFIG);
    assertNotNull(connectionAttemptsConfig);
    assertFalse(connectionAttemptsConfig.errorMessages().isEmpty());
  }

  @Test
  public void testTooLongTopicPrefix() {
    StringBuilder sb = new StringBuilder();
    for (int i = 0; i < 249; i++) {
      sb.append("a");
    }
    props.put(JdbcSourceConnectorConfig.TOPIC_PREFIX_CONFIG, sb.toString());
    Map<String, ConfigValue> validatedConfig =
        JdbcSourceConnectorConfig.baseConfigDef().validateAll(props);
    ConfigValue connectionAttemptsConfig =
        validatedConfig.get(JdbcSourceConnectorConfig.TOPIC_PREFIX_CONFIG);
    assertNotNull(connectionAttemptsConfig);
    assertTrue(connectionAttemptsConfig.errorMessages().isEmpty());

    sb.append("a");
    props.put(JdbcSourceConnectorConfig.TOPIC_PREFIX_CONFIG, sb.toString());
    validatedConfig =
        JdbcSourceConnectorConfig.baseConfigDef().validateAll(props);
    connectionAttemptsConfig =
        validatedConfig.get(JdbcSourceConnectorConfig.TOPIC_PREFIX_CONFIG);
    assertNotNull(connectionAttemptsConfig);
    assertFalse(connectionAttemptsConfig.errorMessages().isEmpty());
  }

  @Test
  public void testCredentialsProviderClassConfig() {
    // Configuring MockTime Class here which does not extends JdbcCredentialsProvider Interface
    props.put(JdbcSourceConnectorConfig.CREDENTIALS_PROVIDER_CLASS_CONFIG,
        MockTime.class.getName());
    Map<String, ConfigValue> validatedConfig =
        JdbcSourceConnectorConfig.baseConfigDef().validateAll(props);
    ConfigValue credentialsProviderConfig =
        validatedConfig.get(JdbcSourceConnectorConfig.CREDENTIALS_PROVIDER_CLASS_CONFIG);

    assertNotNull(credentialsProviderConfig);
    assertFalse(credentialsProviderConfig.errorMessages().isEmpty());

    // Configuring DefaultJdbcCredentialsProvider Class here which extends JdbcCredentialsProvider
    // Interface
    props.put(JdbcSourceConnectorConfig.CREDENTIALS_PROVIDER_CLASS_CONFIG,
        DefaultJdbcCredentialsProvider.class.getName());
    validatedConfig =
        JdbcSourceConnectorConfig.baseConfigDef().validateAll(props);
    credentialsProviderConfig =
        validatedConfig.get(JdbcSourceConnectorConfig.CREDENTIALS_PROVIDER_CLASS_CONFIG);
    assertNotNull(credentialsProviderConfig);
    assertTrue(credentialsProviderConfig.errorMessages().isEmpty());
  }

  @SuppressWarnings("unchecked")
  protected <T> void assertContains(Collection<T> actual, T... expected) {
    for (T e : expected) {
      assertTrue(actual.contains(e));
    }
    assertEquals(expected.length, actual.size());
  }

  protected ConfigValue namedValue(List<ConfigValue> values, String name) {
    for (ConfigValue value : values) {
      if (value.name().equals(name)) return value;
    }
    return null;
  }

  @SuppressWarnings("unchecked")
  protected <T> void assertRecommendedValues(ConfigValue value, T... recommendedValues) {
    assertContains(value.recommendedValues(), recommendedValues);
  }

  @SuppressWarnings("unchecked")
  protected <T> void assertWhitelistRecommendations(T... recommendedValues) {
    assertContains(namedValue(results, JdbcSourceConnectorConfig.TABLE_WHITELIST_CONFIG).recommendedValues(), recommendedValues);
  }

  @SuppressWarnings("unchecked")
  protected <T> void assertBlacklistRecommendations(T... recommendedValues) {
    assertContains(namedValue(results, JdbcSourceConnectorConfig.TABLE_BLACKLIST_CONFIG).recommendedValues(), recommendedValues);
  }

  private Map<String, String> createMinimalConfig() {
    Map<String, String> props = new HashMap<>();
    props.put(JdbcSourceConnectorConfig.CONNECTION_URL_CONFIG, "jdbc:test://localhost");
    props.put(JdbcSourceConnectorConfig.MODE_CONFIG, JdbcSourceConnectorConfig.MODE_BULK);
    return props;
  }

  @Test
  public void testTableIncludeListConfiguration() {
    Map<String, String> props = createMinimalConfig();
    props.put(JdbcSourceConnectorConfig.TABLE_INCLUDE_LIST_CONFIG, "schema1\\.users,schema2\\.orders");
    
    JdbcSourceConnectorConfig config = new JdbcSourceConnectorConfig(props);
    List<String> includeList = config.tableIncludeListRegexes();
    
    assertEquals(2, includeList.size());
    assertTrue(includeList.contains("schema1\\.users"));
    assertTrue(includeList.contains("schema2\\.orders"));
  }

  @Test
  public void testTableExcludeListConfiguration() {
    Map<String, String> props = createMinimalConfig();
    props.put(JdbcSourceConnectorConfig.TABLE_INCLUDE_LIST_CONFIG, ".*");
    props.put(JdbcSourceConnectorConfig.TABLE_EXCLUDE_LIST_CONFIG, ".*temp.*,.*staging.*");
    
    JdbcSourceConnectorConfig config = new JdbcSourceConnectorConfig(props);
    List<String> excludeList = config.tableExcludeListRegexes();
    
    assertEquals(2, excludeList.size());
    assertTrue(excludeList.contains(".*temp.*"));
    assertTrue(excludeList.contains(".*staging.*"));
  }

  @Test
  public void testEmptyIncludeExcludeListsReturnEmptyLists() {
    Map<String, String> props = createMinimalConfig();
    
    JdbcSourceConnectorConfig config = new JdbcSourceConnectorConfig(props);
    
    assertTrue(config.tableIncludeListRegexes().isEmpty());
    assertTrue(config.tableExcludeListRegexes().isEmpty());
  }

  @Test(expected = ConfigException.class)
  public void testInvalidRegexInIncludeListThrowsException() {
    Map<String, String> props = createMinimalConfig();
    props.put(JdbcSourceConnectorConfig.TABLE_INCLUDE_LIST_CONFIG, "[invalid-regex");
    
    new JdbcSourceConnectorConfig(props);
  }

  @Test(expected = ConfigException.class)
  public void testInvalidRegexInExcludeListThrowsException() {
    Map<String, String> props = createMinimalConfig();
    props.put(JdbcSourceConnectorConfig.TABLE_INCLUDE_LIST_CONFIG, ".*");
    props.put(JdbcSourceConnectorConfig.TABLE_EXCLUDE_LIST_CONFIG, "*invalid-regex[");
    
    new JdbcSourceConnectorConfig(props);
  }

  @Test
  public void testValidRegexPatternsAreAccepted() {
    Map<String, String> props = createMinimalConfig();
    props.put(JdbcSourceConnectorConfig.TABLE_INCLUDE_LIST_CONFIG, 
             "schema1\\.users,schema2\\.orders.*,.*\\.products");
    props.put(JdbcSourceConnectorConfig.TABLE_EXCLUDE_LIST_CONFIG, 
             ".*temp.*,.*staging.*,.*backup_\\d+");
    
    JdbcSourceConnectorConfig config = new JdbcSourceConnectorConfig(props);
    
    assertEquals(3, config.tableIncludeListRegexes().size());
    assertEquals(3, config.tableExcludeListRegexes().size());
  }

  @Test
  public void testComplexRegexPatterns() {
    Map<String, String> props = createMinimalConfig();
    props.put(JdbcSourceConnectorConfig.TABLE_INCLUDE_LIST_CONFIG, 
             "prod_.*\\.customer_.*,test_.*\\.user[0-9]+,.*\\.(order|invoice)_data");
    
    JdbcSourceConnectorConfig config = new JdbcSourceConnectorConfig(props);
    List<String> includeList = config.tableIncludeListRegexes();
    
    assertEquals(3, includeList.size());
    assertTrue(includeList.contains("prod_.*\\.customer_.*"));
    assertTrue(includeList.contains("test_.*\\.user[0-9]+"));
    assertTrue(includeList.contains(".*\\.(order|invoice)_data"));
  }

  @Test
  public void testSingleRegexPattern() {
    Map<String, String> props = createMinimalConfig();
    props.put(JdbcSourceConnectorConfig.TABLE_INCLUDE_LIST_CONFIG, "schema1\\..*");
    
    JdbcSourceConnectorConfig config = new JdbcSourceConnectorConfig(props);
    List<String> includeList = config.tableIncludeListRegexes();
    
    assertEquals(1, includeList.size());
    assertEquals("schema1\\..*", includeList.get(0));
  }

  @Test
  public void testWhitespaceInRegexPatternsIsHandledCorrectly() {
    Map<String, String> props = createMinimalConfig();
    props.put(JdbcSourceConnectorConfig.TABLE_INCLUDE_LIST_CONFIG, " schema1\\.users , schema2\\.orders ");
    
    JdbcSourceConnectorConfig config = new JdbcSourceConnectorConfig(props);
    List<String> includeList = config.tableIncludeListRegexes();
    
    assertEquals(2, includeList.size());
    assertTrue(includeList.contains("schema1\\.users"));
    assertTrue(includeList.contains("schema2\\.orders"));
  }

  @Test
  public void testQueryMaskedValueIsMaskedInConfigValue() {
    // Setup config with query.masked
    Map<String, String> props = new HashMap<>();
    props.put(JdbcSourceConnectorConfig.CONNECTION_URL_CONFIG, "jdbc:postgresql://localhost:5432/testdb");
    props.put(JdbcSourceConnectorConfig.CONNECTION_USER_CONFIG, "testUser");
    props.put(JdbcSourceConnectorConfig.TABLE_WHITELIST_CONFIG, "table1,table2");
    String sensitiveQuery = "SELECT * FROM sensitive_table WHERE secret_column = 'confidential'";
    props.put(JdbcSourceConnectorConfig.QUERY_MASKED_CONFIG, sensitiveQuery);

    // Validate and get config values
    Map<String, ConfigValue> validatedConfig = JdbcSourceConnectorConfig.CONFIG_DEF.validateAll(props);
    ConfigValue queryMaskedValue = validatedConfig.get(JdbcSourceConnectorConfig.QUERY_MASKED_CONFIG);

    assertNotNull(queryMaskedValue);

    // The value should be masked (shown as [hidden] or Password object)
    Object maskedValue = queryMaskedValue.value();
    assertNotNull(maskedValue);

    // When converted to string, Password objects show as "[hidden]"
    String maskedString = maskedValue.toString();
    assertEquals("[hidden]", maskedString);

    // The actual query should NOT be visible in the string representation
    assertFalse(maskedString.contains(sensitiveQuery));
  }

  @Test
  public void testGetQueryStringReturnsQueryWhenOnlyQuerySet() {
    // Setup config with only query (not query.masked)
    Map<String, String> props = new HashMap<>();
    props.put(JdbcSourceConnectorConfig.CONNECTION_URL_CONFIG, "jdbc:postgresql://localhost:5432/testdb");
    props.put(JdbcSourceConnectorConfig.CONNECTION_USER_CONFIG, "testUser");
    props.put(JdbcSourceConnectorConfig.TABLE_WHITELIST_CONFIG, "table1,table2");
    String expectedQuery = "SELECT * FROM public_table";
    props.put(JdbcSourceConnectorConfig.QUERY_CONFIG, expectedQuery);

    JdbcSourceConnectorConfig config = new JdbcSourceConnectorConfig(props);

    assertTrue(config.getQuery().isPresent());
    assertEquals(expectedQuery, config.getQuery().get());
  }

  @Test
  public void testGetQueryStringReturnsEmptyWhenNeitherSet() {
    // Setup config without query or query.masked
    Map<String, String> props = new HashMap<>();
    props.put(JdbcSourceConnectorConfig.CONNECTION_URL_CONFIG, "jdbc:postgresql://localhost:5432/testdb");
    props.put(JdbcSourceConnectorConfig.CONNECTION_USER_CONFIG, "testUser");
    props.put(JdbcSourceConnectorConfig.TABLE_WHITELIST_CONFIG, "table1,table2");

    JdbcSourceConnectorConfig config = new JdbcSourceConnectorConfig(props);

    // getQuery() should return empty Optional
    assertFalse(config.getQuery().isPresent());
  }

  @Test
  public void testIsQueryMaskedTrueWhenMaskedQueryPresent() {
    Map<String, String> props = new HashMap<>();
    props.put(JdbcSourceConnectorConfig.CONNECTION_URL_CONFIG, "jdbc:postgresql://localhost:5432/testdb");
    props.put(JdbcSourceConnectorConfig.CONNECTION_USER_CONFIG, "testUser");
    props.put(JdbcSourceConnectorConfig.TABLE_WHITELIST_CONFIG, "table1");
    props.put(JdbcSourceConnectorConfig.QUERY_MASKED_CONFIG, "SELECT * FROM sensitive_table");

    JdbcSourceConnectorConfig config = new JdbcSourceConnectorConfig(props);

    assertTrue(config.isQueryMasked());
  }

  @Test
  public void testIsQueryMaskedFalseWhenMaskedQueryMissing() {
    Map<String, String> props = new HashMap<>();
    props.put(JdbcSourceConnectorConfig.CONNECTION_URL_CONFIG, "jdbc:postgresql://localhost:5432/testdb");
    props.put(JdbcSourceConnectorConfig.CONNECTION_USER_CONFIG, "testUser");
    props.put(JdbcSourceConnectorConfig.TABLE_WHITELIST_CONFIG, "table1");

    JdbcSourceConnectorConfig config = new JdbcSourceConnectorConfig(props);

    assertFalse(config.isQueryMasked());
  }

  @Test
  public void testQueryMaskedSupportsComplexQueryWithMultipleJoins() {
    // Test that complex queries work fine with query.masked
    Map<String, String> props = new HashMap<>();
    props.put(JdbcSourceConnectorConfig.CONNECTION_URL_CONFIG, "jdbc:postgresql://localhost:5432/testdb");
    props.put(JdbcSourceConnectorConfig.CONNECTION_USER_CONFIG, "testUser");
    props.put(JdbcSourceConnectorConfig.TABLE_WHITELIST_CONFIG, "table1,table2");

    String complexQuery = "SELECT u.id, u.username, u.email, " +
        "p.profile_pic, a.street, a.city, a.country, " +
        "o.order_id, o.order_date, o.total_amount " +
        "FROM users u " +
        "INNER JOIN profiles p ON u.id = p.user_id " +
        "LEFT JOIN addresses a ON u.id = a.user_id " +
        "LEFT JOIN orders o ON u.id = o.user_id " +
        "WHERE u.status = 'active' " +
        "AND u.created_at >= '2024-01-01' " +
        "AND o.total_amount > 100.00 " +
        "ORDER BY o.order_date DESC " +
        "LIMIT 1000";

    props.put(JdbcSourceConnectorConfig.QUERY_MASKED_CONFIG, complexQuery);

    JdbcSourceConnectorConfig config = new JdbcSourceConnectorConfig(props);

    assertTrue(config.getQuery().isPresent());
    assertEquals(complexQuery, config.getQuery().get());
  }

  @Test
  public void testQueryMaskedSupportsSpecialCharacters() {
    // Test that queries with special characters work fine
    Map<String, String> props = new HashMap<>();
    props.put(JdbcSourceConnectorConfig.CONNECTION_URL_CONFIG, "jdbc:postgresql://localhost:5432/testdb");
    props.put(JdbcSourceConnectorConfig.CONNECTION_USER_CONFIG, "testUser");
    props.put(JdbcSourceConnectorConfig.TABLE_WHITELIST_CONFIG, "table1,table2");

    String queryWithSpecialChars = "SELECT * FROM users " +
        "WHERE name LIKE '%O''Brien%' " +
        "AND email = 'test@example.com' " +
        "AND description LIKE '%Line1\nLine2%' " +
        "AND data LIKE '%Tab\tSeparated%'";

    props.put(JdbcSourceConnectorConfig.QUERY_MASKED_CONFIG, queryWithSpecialChars);

    JdbcSourceConnectorConfig config = new JdbcSourceConnectorConfig(props);

    // Verify special characters are preserved
    assertTrue(config.getQuery().isPresent());
    assertEquals(queryWithSpecialChars, config.getQuery().get());
  }

  @Test
  public void testQueryMaskedWithEmptyStringBehavior() {
    // Test behavior with empty string
    Map<String, String> props = new HashMap<>();
    props.put(JdbcSourceConnectorConfig.CONNECTION_URL_CONFIG, "jdbc:postgresql://localhost:5432/testdb");
    props.put(JdbcSourceConnectorConfig.CONNECTION_USER_CONFIG, "testUser");
    props.put(JdbcSourceConnectorConfig.TABLE_WHITELIST_CONFIG, "table1,table2");
    props.put(JdbcSourceConnectorConfig.QUERY_MASKED_CONFIG, "");

    JdbcSourceConnectorConfig config = new JdbcSourceConnectorConfig(props);

    // Empty string should return empty Optional
    assertFalse(config.getQuery().isPresent());
  }

  @Test
  public void testShouldLogConfigReturnsTrueWhenQueryMaskedNotSet() {
    // When query.masked is not set, config values should be logged
    Map<String, String> props = new HashMap<>();
    props.put(JdbcSourceConnectorConfig.CONNECTION_URL_CONFIG, "jdbc:postgresql://localhost:5432/testdb");
    props.put(JdbcSourceConnectorConfig.CONNECTION_USER_CONFIG, "testUser");
    props.put(JdbcSourceConnectorConfig.TABLE_WHITELIST_CONFIG, "table1,table2");

    assertTrue(JdbcSourceConnectorConfig.shouldLog(props));
  }

  @Test
  public void testShouldLogConfigReturnsFalseWhenQueryMaskedIsSet() {
    // When query.masked is set, config values should NOT be logged
    Map<String, String> props = new HashMap<>();
    props.put(JdbcSourceConnectorConfig.CONNECTION_URL_CONFIG, "jdbc:postgresql://localhost:5432/testdb");
    props.put(JdbcSourceConnectorConfig.CONNECTION_USER_CONFIG, "testUser");
    props.put(JdbcSourceConnectorConfig.TABLE_WHITELIST_CONFIG, "table1,table2");
    props.put(JdbcSourceConnectorConfig.QUERY_MASKED_CONFIG, "SELECT * FROM sensitive_table");

   assertFalse(JdbcSourceConnectorConfig.shouldLog(props));
  }

  @Test
  public void testShouldLogConfigReturnsTrueWhenQueryAndQueryMaskedAreSet() {
    // When query.masked and query are both set, config values should not be logged
    Map<String, String> props = new HashMap<>();
    props.put(JdbcSourceConnectorConfig.CONNECTION_URL_CONFIG, "jdbc:postgresql://localhost:5432/testdb");
    props.put(JdbcSourceConnectorConfig.CONNECTION_USER_CONFIG, "testUser");
    props.put(JdbcSourceConnectorConfig.TABLE_WHITELIST_CONFIG, "table1,table2");
    props.put(JdbcSourceConnectorConfig.QUERY_CONFIG, "SELECT * FROM public_table");
    props.put(JdbcSourceConnectorConfig.QUERY_MASKED_CONFIG, "SELECT * FROM sensitive_table");

    assertFalse(JdbcSourceConnectorConfig.shouldLog(props));
  }
}
