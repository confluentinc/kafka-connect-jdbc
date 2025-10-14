/*
 * Copyright [2024 - 2024] Confluent Inc.
 */

package io.confluent.connect.jdbc.util;

import java.util.Objects;

import org.slf4j.MDC;

public class ConnectLogContext implements AutoCloseable {

  private static final String CONNECTOR_MDC_CONTEXT = "connector.context";

  private final String previousThreadContext;
  private final String prefix;

  public ConnectLogContext(String threadLogContext) {
    Objects.requireNonNull(threadLogContext);
    previousThreadContext = MDC.get(CONNECTOR_MDC_CONTEXT);
    if (previousThreadContext != null) {
      String newThreadContext = appendConnectMdcContext(previousThreadContext, threadLogContext);
      MDC.put(CONNECTOR_MDC_CONTEXT, newThreadContext);
      prefix = "";
    } else {
      prefix = String.format("%s ", threadLogContext);
    }
  }

  public String prefix() {
    return prefix;
  }

  @Override
  public void close() {
    if (previousThreadContext != null) {
      MDC.put(CONNECTOR_MDC_CONTEXT, previousThreadContext);
    }
  }

  protected static String appendConnectMdcContext(String existingContext, String suffix) {
    if (existingContext.endsWith("] ") && existingContext.length() > 2) {
      return existingContext.substring(0, existingContext.length() - 2) + "|" + suffix + "] ";
    } else {
      return existingContext + suffix;
    }
  }
}