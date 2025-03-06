package io.confluent.connect.jdbc.util;

import static org.junit.Assert.assertEquals;

import java.util.HashMap;
import java.util.Map;
import org.junit.Before;
import org.junit.Test;

public class DefaultJdbcCredentialsProviderTest {

  DefaultJdbcCredentialsProvider defaultCredentialsProvider;
  String testUsername = "username";
  String testPassword = "password";

  @Before
  public void setup() {
    Map<String, String> configMap = new HashMap<>();
    configMap.put("connection.user", testUsername);
    configMap.put("connection.password", testPassword);

    defaultCredentialsProvider = new DefaultJdbcCredentialsProvider();
    defaultCredentialsProvider.configure(configMap);

  }
  @Test
  public void testDatabaseCredsAreConfigurable() {
    // Assert username and password are configured to test values which is configured in setup method
    assertEquals(testUsername, defaultCredentialsProvider.getJdbcCredentials().getUsername());
    assertEquals(testPassword, defaultCredentialsProvider.getJdbcCredentials().getPassword());
  }

  // Test Default Refresh Functionality (which is NoOp)
  @Test
  public void testDefaultRefreshFunctionality() {

    assertEquals(testUsername, defaultCredentialsProvider.getJdbcCredentials().getUsername());
    assertEquals(testPassword, defaultCredentialsProvider.getJdbcCredentials().getPassword());

    defaultCredentialsProvider.refresh();

    // Assert username and password are same
    assertEquals(testUsername, defaultCredentialsProvider.getJdbcCredentials().getUsername());
    assertEquals(testPassword, defaultCredentialsProvider.getJdbcCredentials().getPassword());
  }
}
