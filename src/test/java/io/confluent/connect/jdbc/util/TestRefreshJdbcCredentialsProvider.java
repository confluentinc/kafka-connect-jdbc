package io.confluent.connect.jdbc.util;

/**
 * This is a test Class for JdbcCredentialsProvider Interface which is created to test the refresh
 * functionality. The password is updated everytime credentials are fetched through
 * 'getJdbcCredentials()' method.
 */
public class TestRefreshJdbcCredentialsProvider implements JdbcCredentialsProvider {

  String username = "test-user";
  String password;
  private int numRotations;

  public TestRefreshJdbcCredentialsProvider() {
    numRotations = 0;
  }

  @Override
  public JdbcCredentials getJdbcCredentials() {
    refresh();
    return new BasicJdbcCredentials(username, password);
  }

  @Override
  public void refresh() {
    password = "test-password-" + numRotations;
    numRotations++;
  }
}
