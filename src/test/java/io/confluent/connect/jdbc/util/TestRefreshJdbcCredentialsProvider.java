package io.confluent.connect.jdbc.util;

import io.confluent.credentialproviders.DefaultJdbcCredentials;
import io.confluent.credentialproviders.JdbcCredentials;
import io.confluent.credentialproviders.JdbcCredentialsProvider;
import java.util.Map;

/**
 * This is a test Class for JdbcCredentialsProvider Interface which is created to test the refresh
 * functionality. The password is updated everytime credentials are fetched through
 * 'getJdbcCreds()' method.
 */
public class TestRefreshJdbcCredentialsProvider implements JdbcCredentialsProvider {

  String username = "test-user";
  String password;
  private int numRotations;

  public TestRefreshJdbcCredentialsProvider() {
    numRotations = 0;
  }

  @Override
  public JdbcCredentials getJdbcCreds() {
    // Rotate password on each call
    password = "test-password-" + numRotations;
    numRotations++;
    return new DefaultJdbcCredentials(username, password);
  }

  @Override
  public void configure(Map<String, String> map) {
    // No configuration needed for this test class
  }
}
