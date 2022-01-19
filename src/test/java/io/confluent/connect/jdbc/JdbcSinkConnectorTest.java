package io.confluent.connect.jdbc;


import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.kafka.common.config.Config;

import static io.confluent.connect.jdbc.sink.JdbcSinkConfig.PK_MODE;
import static org.junit.Assert.*;

import org.apache.kafka.common.config.ConfigValue;
import org.junit.Test;

public class JdbcSinkConnectorTest {

  @Test
  public void testValidationWhenDeleteEnabled() {

    Map<String, String> connConfig = new HashMap<>();
    connConfig.put("connector.class", "io.confluent.connect.jdbc.JdbcSinkConnector");
    connConfig.put("delete.enabled", "true");
    connConfig.put("pk.mode", "none");

    JdbcSinkConnector connector = new JdbcSinkConnector();

    List<String> pkModeError = connector.validate(connConfig)
        .configValues()
        .stream()
        .filter(cfg -> PK_MODE.equals(cfg.name()))
        .map(ConfigValue::errorMessages)
        .findFirst()
        .orElse(Collections.emptyList());

    assertFalse(pkModeError.isEmpty());

    connConfig.put("pk.mode", "record_key");

    pkModeError = connector.validate(connConfig)
        .configValues()
        .stream()
        .filter(cfg -> PK_MODE.equals(cfg.name()))
        .map(ConfigValue::errorMessages)
        .findFirst()
        .orElse(Collections.emptyList());

    assertTrue(pkModeError.isEmpty());
  }

  @Test
  public void testValidationWhenDeleteNotEnabled() {

    Map<String, String> connConfig = new HashMap<>();
    connConfig.put("connector.class", "io.confluent.connect.jdbc.JdbcSinkConnector");
    connConfig.put("pk.mode", "none");

    JdbcSinkConnector connector = new JdbcSinkConnector();
    Config config = connector.validate(connConfig);

    List<String> pkModeError = config.configValues()
        .stream()
        .filter(cfg -> PK_MODE.equals(cfg.name()))
        .map(ConfigValue::errorMessages)
        .findFirst()
        .orElse(Collections.emptyList());

    assertTrue(pkModeError.isEmpty());
  }
}
