package io.confluent.connect.jdbc.util;

import org.junit.Before;
import org.junit.Test;

import java.lang.reflect.Field;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;

import static io.confluent.connect.jdbc.source.JdbcSourceConnectorConfig.CONNECTION_USER_CONFIG;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class GcpJdbcCredentialsProviderTest {

  private GcpJdbcCredentialsProvider provider;
  private Map<String, String> config;

  @Before
  public void setUp() {
    provider = new GcpJdbcCredentialsProvider();
    config = new HashMap<>();
    config.put(CONNECTION_USER_CONFIG, "test-db-user");
  }

  @Test
  public void testConfigureMissingDatabaseUser() {
    config.remove(CONNECTION_USER_CONFIG);

    try {
      provider.configure(config);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) {
      assertEquals("Database user must be provided in the configuration.", e.getMessage());
    }
  }

  @Test
  public void testConfigureNullDatabaseUser() {
    config.put(CONNECTION_USER_CONFIG, null);

    try {
      provider.configure(config);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) {
      assertEquals("Database user must be provided in the configuration.", e.getMessage());
    }
  }

  @Test
  public void testGetJdbcCredsWithoutConfigure() {
    // Test calling getJdbcCredentials without calling configure first
    try {
      provider.getJdbcCredentials();
      fail("Expected NullPointerException");
    } catch (NullPointerException e) {
      // Expected - delegatedImpersonatedGoogleCredentials is null
    }
  }

  @Test
  public void testIsTokenValidWithNullCachedToken() throws Exception {
    // Test that isTokenValid returns false when cachedAuthToken is null
    boolean result = invokeIsTokenValid();
    assertFalse("Token should be invalid when cached token is null", result);
  }

  @Test
  public void testIsTokenValidWithExpiredToken() throws Exception {
    // Set a cached token and an expired refresh time
    setCachedAuthToken("test-token");
    setNextTokenRefreshTime(Instant.now().minusSeconds(60)); // expired

    boolean result = invokeIsTokenValid();
    assertFalse("Token should be invalid when refresh time has passed", result);
  }

  @Test
  public void testIsTokenValidWithValidToken() throws Exception {
    // Set a cached token and a future refresh time
    setCachedAuthToken("test-token");
    setNextTokenRefreshTime(Instant.now().plusSeconds(3600)); // valid for 1 hour

    boolean result = invokeIsTokenValid();
    assertTrue("Token should be valid when refresh time is in the future", result);
  }

  @Test
  public void testTokenRefreshSecondsDefaultValue() throws Exception {
    // Configure without specifying token refresh seconds
    config.put(CONNECTION_USER_CONFIG, "test-user");

    // We can't call configure fully because it tries to create DelegatedImpersonatedGoogleCredentials
    // which requires GCP credentials. Instead, we'll just test the parsing logic is correct
    // by verifying the default value constant
    Field field = GcpJdbcCredentialsProvider.class.getDeclaredField("DEFAULT_TOKEN_REFRESH_SECONDS");
    field.setAccessible(true);
    long defaultValue = (long) field.get(null);

    assertEquals("Default token refresh should be 55 minutes (3300 seconds)", 55 * 60, defaultValue);
  }

  // Helper methods using reflection to access private fields and methods

  private boolean invokeIsTokenValid() throws Exception {
    java.lang.reflect.Method method = GcpJdbcCredentialsProvider.class
        .getDeclaredMethod("isTokenValid");
    method.setAccessible(true);
    return (boolean) method.invoke(provider);
  }

  private void setCachedAuthToken(String token) throws Exception {
    Field field = GcpJdbcCredentialsProvider.class
        .getDeclaredField("cachedAuthToken");
    field.setAccessible(true);
    field.set(provider, token);
  }

  private void setNextTokenRefreshTime(Instant time) throws Exception {
    Field field = GcpJdbcCredentialsProvider.class
        .getDeclaredField("nextTokenRefreshTime");
    field.setAccessible(true);
    field.set(provider, time);
  }
}
