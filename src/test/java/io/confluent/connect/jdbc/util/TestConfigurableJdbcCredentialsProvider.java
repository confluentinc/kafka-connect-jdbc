package io.confluent.connect.jdbc.util;

import io.confluent.credentialproviders.DefaultJdbcCredentials;
import io.confluent.credentialproviders.JdbcCredentials;
import io.confluent.credentialproviders.JdbcCredentialsProvider;
import java.util.HashMap;
import java.util.Map;

/**
 * This is a test class for JdbcCredentialsProvider Interface which is created to test the
 * configurable functionality
 */
public class TestConfigurableJdbcCredentialsProvider implements JdbcCredentialsProvider {

  Map<String, String> configMap = new HashMap<>();

  @Override
  public void configure(Map<String, String> map) {
    configMap = new HashMap<>(map);
  }

  @Override
  public JdbcCredentials getJdbcCreds() {
    return new DefaultJdbcCredentials((String) configMap.get("username"),
        (String) configMap.get("password"));
  }
}
