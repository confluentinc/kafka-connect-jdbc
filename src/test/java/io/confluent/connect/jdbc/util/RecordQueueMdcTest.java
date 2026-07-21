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
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.AbstractExecutorService;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.function.Supplier;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

public class RecordQueueMdcTest {

  private static final String CONNECTOR_MDC_CONTEXT = "connector.context";
  private static final String TRACE_MDC_CONTEXT = "trace.id";
  private static final Duration TERMINATION_TIMEOUT = Duration.ofSeconds(5);

  private RecordQueue<SourceRecord> queue;

  @Before
  public void setUp() {
    queue = queueBuilder()
        .maxExecutorThreads(1)
        .build();
  }

  @After
  public void tearDown() throws InterruptedException {
    MDC.clear();
    stopQueue();
  }

  private RecordQueue.Builder<SourceRecord> queueBuilder() {
    return RecordQueue.<SourceRecord>builder()
        .maxBatchSize(10);
  }

  private void replaceQueue(RecordQueue<SourceRecord> replacement) throws InterruptedException {
    stopQueue();
    queue = replacement;
  }

  private void stopQueue() throws InterruptedException {
    if (queue != null) {
      queue.stop();
      queue.awaitTermination(TERMINATION_TIMEOUT);
      queue = null;
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

  @Test
  public void shouldInvokeProtectedCreateLoggingSupplierOverride() throws Exception {
    TrackingRecordQueue trackingQueue = new TrackingRecordQueue(
        queueBuilder().maxExecutorThreads(1)
    );
    replaceQueue(trackingQueue);

    assertTrue(queue.submit(
        "Test Operation",
        "testProcessor",
        destination -> true
    ).get(10, TimeUnit.SECONDS));

    assertTrue(trackingQueue.loggingSupplierCreated());
  }

  @Test
  public void shouldRestoreCallerMdcWithDirectExecutor() throws Exception {
    replaceQueue(
        queueBuilder()
            .executorFactory(DirectExecutorService::new)
            .build()
    );
    MDC.put(CONNECTOR_MDC_CONTEXT, "[my-connector|task-0] ");
    MDC.put(TRACE_MDC_CONTEXT, "caller-trace");
    Map<String, String> callerMdc = MDC.getCopyOfContextMap();

    assertTrue(queue.submit(
        "Test Operation",
        "testProcessor",
        destination -> true
    ).get(10, TimeUnit.SECONDS));

    assertEquals(callerMdc, MDC.getCopyOfContextMap());
  }

  @Test
  public void shouldRestorePreExistingExecutorMdc() throws Exception {
    ExecutorService executor = useExecutorWithMdc(TRACE_MDC_CONTEXT, "worker-trace");
    AtomicReference<String> capturedMdc = new AtomicReference<>();

    assertTrue(queue.submit(
        "Test Operation",
        "testProcessor",
        destination -> {
          capturedMdc.set(MDC.get(TRACE_MDC_CONTEXT));
          return true;
        }
    ).get(10, TimeUnit.SECONDS));

    assertNull(capturedMdc.get());
    assertEquals(
        "worker-trace",
        executor.submit(() -> MDC.get(TRACE_MDC_CONTEXT)).get(10, TimeUnit.SECONDS)
    );
  }

  @Test
  public void shouldRestorePreExistingExecutorMdcAfterFailure() throws Exception {
    ExecutorService executor = useExecutorWithMdc(TRACE_MDC_CONTEXT, "worker-trace");
    MDC.put(CONNECTOR_MDC_CONTEXT, "[my-connector|task-0] ");

    CompletableFuture<Boolean> future = queue.submit(
        "Test Operation",
        "testProcessor",
        destination -> {
          throw new IllegalStateException("test failure");
        }
    );

    assertThrows(
        ExecutionException.class,
        () -> future.get(10, TimeUnit.SECONDS)
    );
    assertEquals(
        "worker-trace",
        executor.submit(() -> MDC.get(TRACE_MDC_CONTEXT)).get(10, TimeUnit.SECONDS)
    );
  }

  private ExecutorService useExecutorWithMdc(String key, String value) throws Exception {
    ExecutorService executor = Executors.newSingleThreadExecutor();
    executor.submit(() -> MDC.put(key, value)).get(10, TimeUnit.SECONDS);
    replaceQueue(
        queueBuilder()
            .executorFactory(() -> executor)
            .build()
    );
    return executor;
  }

  private static class TrackingRecordQueue extends RecordQueue<SourceRecord> {

    private final AtomicBoolean loggingSupplierCreated = new AtomicBoolean();

    private TrackingRecordQueue(RecordQueue.Builder<SourceRecord> builder) {
      super(builder);
    }

    @Override
    protected <T> Supplier<T> createLoggingSupplier(
        String operationName,
        String logContext,
        RecordDestination<SourceRecord> destination,
        Function<RecordDestination<SourceRecord>, T> generatorProcessor
    ) {
      loggingSupplierCreated.set(true);
      return super.createLoggingSupplier(
          operationName,
          logContext,
          destination,
          generatorProcessor
      );
    }

    private boolean loggingSupplierCreated() {
      return loggingSupplierCreated.get();
    }
  }

  private static class DirectExecutorService extends AbstractExecutorService {

    private final AtomicBoolean shutdown = new AtomicBoolean();

    @Override
    public void shutdown() {
      shutdown.set(true);
    }

    @Override
    public List<Runnable> shutdownNow() {
      shutdown.set(true);
      return Collections.emptyList();
    }

    @Override
    public boolean isShutdown() {
      return shutdown.get();
    }

    @Override
    public boolean isTerminated() {
      return shutdown.get();
    }

    @Override
    public boolean awaitTermination(long timeout, TimeUnit unit) {
      return isTerminated();
    }

    @Override
    public void execute(Runnable command) {
      if (isShutdown()) {
        throw new RejectedExecutionException();
      }
      command.run();
    }
  }
}
