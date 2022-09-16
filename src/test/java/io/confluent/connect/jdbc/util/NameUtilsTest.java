package io.confluent.connect.jdbc.util;

import org.junit.Test;

import static org.junit.Assert.*;

public class NameUtilsTest {

  @Test
  public void testToTableName() {
    // No replacement
    assertEquals("kafka_connect",
            NameUtils.toTableName("kafka_connect", "anything"));
    // Replace topic name
    assertEquals("kafka_connect",
            NameUtils.toTableName("${topic}", "kafka_connect"));
    assertEquals("pre_kafka_connect_post",
            NameUtils.toTableName("pre_${topic}_post", "kafka_connect"));
    // Replace topic name with regex
    assertEquals("pre_kafka_connect_post",
            NameUtils.toTableName("${topic/^.+\\.ao_/pre_/}_post", "io.confluent.connect.jdbc.ao_kafka_connect"));
    assertEquals("pre_kafka_connect_post",
            NameUtils.toTableName("pre_${topic#^.+\\.ao_##}_post", "io.confluent.connect.jdbc.ao_kafka_connect"));
  }
}
