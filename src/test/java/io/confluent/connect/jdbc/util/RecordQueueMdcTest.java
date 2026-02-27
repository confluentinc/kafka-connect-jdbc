/*
 * Copyright [2024 - 2024] Confluent Inc.
 */

package io.confluent.connect.jdbc.util;

import org.apache.kafka.connect.source.SourceRecord;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.MDC;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class RecordQueueMdcTest {

  private static final String CONNECTOR_MDC_CONTEXT = "connector.context";

  private RecordQueue<SourceRecord> queue;

  @Before
  public void setUp() {
    queue = RecordQueue.<SourceRecord>builder()
        .maxBatchSize(10)
        .maxExecutorThreads(1)
        .build();
  }

  @After
  public void tearDown() throws InterruptedException {
    MDC.clear();
    if (queue != null) {
      queue.stop();
      queue.awaitTermination(Duration.ofSeconds(5));
    }
  }

  @Test
  public void shouldPropagateConnectorContextToExecutorThread() throws Exception {
    // Simulate Kafka Connect setting MDC on the calling thread
    MDC.put(CONNECTOR_MDC_CONTEXT, "[my-connector|task-0] ");

    AtomicReference<String> capturedMdc = new AtomicReference<>();

    CompletableFuture<Boolean> future = queue.submit(
        "Test Operation",
        "testProcessor",
        destination -> {
          // Capture the MDC value on the executor thread
          capturedMdc.set(MDC.get(CONNECTOR_MDC_CONTEXT));
          return true;
        }
    );

    assertTrue(future.get(10, TimeUnit.SECONDS));

    // The executor thread should have seen the connector context with appended suffix
    assertNotNull("MDC should be propagated to executor thread", capturedMdc.get());
    assertEquals(
        "[my-connector|task-0|testProcessor] ",
        capturedMdc.get()
    );
  }

  @Test
  public void shouldCleanUpMdcAfterExecution() throws Exception {
    MDC.put(CONNECTOR_MDC_CONTEXT, "[my-connector|task-0] ");

    AtomicReference<String> mdcAfterExecution = new AtomicReference<>();

    // First submit — sets MDC on executor thread
    CompletableFuture<Boolean> future1 = queue.submit(
        "First Operation",
        "firstProcessor",
        destination -> true
    );
    future1.get(10, TimeUnit.SECONDS);

    // Clear caller's MDC to simulate a different calling context
    MDC.clear();

    // Second submit — without MDC on the calling thread
    CompletableFuture<Boolean> future2 = queue.submit(
        "Second Operation",
        "secondProcessor",
        destination -> {
          mdcAfterExecution.set(MDC.get(CONNECTOR_MDC_CONTEXT));
          return true;
        }
    );
    future2.get(10, TimeUnit.SECONDS);

    // The second execution should NOT have the first execution's MDC
    // (it should be null since the caller had no MDC set)
    assertNull(
        "MDC should be cleaned up between executions on pooled threads",
        mdcAfterExecution.get()
    );
  }

  @Test
  public void shouldWorkWhenCallerHasNoMdc() throws Exception {
    // No MDC set on the calling thread
    AtomicReference<String> capturedMdc = new AtomicReference<>();

    CompletableFuture<Boolean> future = queue.submit(
        "Test Operation",
        "testProcessor",
        destination -> {
          capturedMdc.set(MDC.get(CONNECTOR_MDC_CONTEXT));
          return true;
        }
    );

    assertTrue(future.get(10, TimeUnit.SECONDS));

    // Without MDC, ConnectLogContext falls back to prefix mode — no MDC set
    assertNull(capturedMdc.get());
  }
}
