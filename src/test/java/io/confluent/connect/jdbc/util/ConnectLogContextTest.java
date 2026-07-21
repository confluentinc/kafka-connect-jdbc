/*
 * Copyright [2024 - 2024] Confluent Inc.
 */

package io.confluent.connect.jdbc.util;

import org.junit.After;
import org.junit.Test;
import org.slf4j.MDC;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public class ConnectLogContextTest {

  private static final String CONNECTOR_MDC_CONTEXT = "connector.context";

  @After
  public void tearDown() {
    MDC.clear();
  }

  @Test
  public void shouldAppendSuffixWhenMdcHasConnectorContext() {
    MDC.put(CONNECTOR_MDC_CONTEXT, "[my-connector|task-0] ");

    try (ConnectLogContext ctx = new ConnectLogContext("tableQuerierProcessor")) {
      // MDC should now have the appended context
      assertEquals(
          "[my-connector|task-0|tableQuerierProcessor] ",
          MDC.get(CONNECTOR_MDC_CONTEXT)
      );
      // prefix should be empty since MDC is used
      assertEquals("", ctx.prefix());
    }

    // After close, MDC should be restored to original value
    assertEquals("[my-connector|task-0] ", MDC.get(CONNECTOR_MDC_CONTEXT));
  }

  @Test
  public void shouldUsePrefixWhenMdcIsEmpty() {
    // No MDC set
    try (ConnectLogContext ctx = new ConnectLogContext("tableQuerierProcessor")) {
      // MDC should still be null
      assertNull(MDC.get(CONNECTOR_MDC_CONTEXT));
      // prefix should contain the log context
      assertEquals("tableQuerierProcessor ", ctx.prefix());
    }
  }

  @Test
  public void shouldHandleNestedContexts() {
    MDC.put(CONNECTOR_MDC_CONTEXT, "[my-connector|task-0] ");

    try (ConnectLogContext outer = new ConnectLogContext("outer")) {
      assertEquals("[my-connector|task-0|outer] ", MDC.get(CONNECTOR_MDC_CONTEXT));

      try (ConnectLogContext inner = new ConnectLogContext("inner")) {
        assertEquals("[my-connector|task-0|outer|inner] ", MDC.get(CONNECTOR_MDC_CONTEXT));
      }

      // After inner closes, should restore to outer's context
      assertEquals("[my-connector|task-0|outer] ", MDC.get(CONNECTOR_MDC_CONTEXT));
    }

    // After outer closes, should restore to original
    assertEquals("[my-connector|task-0] ", MDC.get(CONNECTOR_MDC_CONTEXT));
  }
}
