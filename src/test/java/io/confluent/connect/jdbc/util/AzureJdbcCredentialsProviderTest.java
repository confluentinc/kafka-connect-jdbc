package io.confluent.connect.jdbc.util;

import org.junit.Before;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class AzureJdbcCredentialsProviderTest {

  private AzureJdbcCredentialsProvider provider;
  private Map<String, String> config;

  @Before
  public void setUp() {
    provider = new AzureJdbcCredentialsProvider();
    config = new HashMap<>();
    config.put("azure.token.scope", "https://database.windows.net/.default");
  }

  @Test
  public void testConfigureMissingTokenScope() {
    config.remove("azure.token.scope");

    try {
      provider.configure(config);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) {
      assertEquals("Azure token scope must be provided in the configuration.", e.getMessage());
    }
  }

  @Test
  public void testConfigureEmptyTokenScope() {
    config.put("azure.token.scope", "");

    try {
      provider.configure(config);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) {
      assertEquals("Azure token scope must be provided in the configuration.", e.getMessage());
    }
  }

  @Test
  public void testGetJdbcCredsWithoutConfigure() {
    // Test calling getJdbcCreds without calling configure first
    try {
      provider.getJdbcCredentials();
      fail("Expected NullPointerException");
    } catch (NullPointerException e) {
      // Expected - azureCredentialsProvider is null
    }
  }
}
