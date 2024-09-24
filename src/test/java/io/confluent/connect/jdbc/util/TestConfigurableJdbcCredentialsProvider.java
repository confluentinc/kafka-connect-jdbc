package io.confluent.connect.jdbc.util;

import java.util.HashMap;
import java.util.Map;
import org.apache.kafka.common.Configurable;

/**
 * This is a test class for JdbcCredentialsProvider Interface which is created to test the
 * configurable functionality
 */
public class TestConfigurableJdbcCredentialsProvider implements JdbcCredentialsProvider,
    Configurable {

  Map<String, ?> configMap = new HashMap<>();

  @Override
  public void configure(Map<String, ?> map) {
    configMap = new HashMap<>(map);
  }

  @Override
  public JdbcCredentials getJdbcCredentials() {
    return new BasicJdbcCredentials((String) configMap.get("username"),
        (String) configMap.get("password"));
  }
}
