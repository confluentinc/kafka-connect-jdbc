package io.confluent.connect.jdbc.data;

import org.apache.kafka.connect.data.Schema;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class ConnectDecimalTest {
  private static final String CONNECT_AVRO_PRECISION_FIELD = "connect.decimal.precision";

  /**
   * Regression test for https://github.com/confluentinc/kafka-connect-jdbc/issues/788
   */
  @Test
  public void testConnectDecimalPrecisionParameter() {
    Schema schema = ConnectDecimal.builder(5, 2).build();
    assertEquals(schema.parameters().get(CONNECT_AVRO_PRECISION_FIELD), "5");
  }
}
