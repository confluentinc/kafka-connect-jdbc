/*
 * Copyright [2024 - 2024] Confluent Inc.
 */

package io.confluent.connect.jdbc.util;

import org.apache.kafka.common.utils.Time;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.source.SourceRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.function.IntFunction;
import java.util.function.Supplier;

import org.slf4j.MDC;

/**
 * A concurrent and blocking queue for source records, with the ability to asynchronously execute
 * record generators that send records to this queue.
 *
 * @param <RecordT> the type of {@link SourceRecord}
 */
public class RecordQueue<RecordT extends SourceRecord> implements RecordDestination<RecordT>  {

  private static final Logger log = LoggerFactory.getLogger(RecordQueue.class);

  /**
   * Builder for RecordQueue that can be specialized.
   *
   * @param <RecordT>  the type of record
   */
  public static class Builder<RecordT extends SourceRecord> {

    private int maxBatchSize = 1024;
    private int maxQueueSize = 0;
    private Duration linger = Duration.ZERO;
    private Duration maxPollInterruptTime = Duration.ofSeconds(1);
    private Time clock = Time.SYSTEM;
    private BlockingQueue<SourceRecord> queue;
    private IntFunction<List<SourceRecord>> batchFactory = ArrayList::new;
    private IntFunction<BlockingQueue<SourceRecord>> queueFactory = ArrayBlockingQueue::new;
    private Supplier<ExecutorService> executorFactory = Executors::newCachedThreadPool;

    /**
     * Set the maximum size of the batches that {@link RecordQueue#poll()} may return.
     *
     * <p>By default, the maximum batch size is 1024.
     *
     * @param maxBatchSize the maximum batch size
     * @return this builder; never null
     * @throws IllegalArgumentException if {@code maxBatchSize} is not positive
     */
    public Builder<RecordT> maxBatchSize(int maxBatchSize) {
      if (maxBatchSize < 1) {
        throw new IllegalArgumentException("Maximum batch size must be positive");
      }
      this.maxBatchSize = maxBatchSize;
      return this;
    }

    /**
     * Get the maximum size of the batches.
     *
     * @return the maximum batch size
     */
    public int maxBatchSize() {
      return maxBatchSize;
    }

    /**
     * Set the maximum size of the queue.
     *
     * @param maxQueueSize the maximum queue size
     * @return this builder; never null
     * @throws IllegalArgumentException if {@code maxQueueSize} is not positive
     */
    public Builder<RecordT> maxQueueSize(int maxQueueSize) {
      if (maxQueueSize < 1) {
        throw new IllegalArgumentException("Maximum queue size must be positive");
      }
      this.maxQueueSize = maxQueueSize;
      return this;
    }

    /**
     * Get the maximum size of the queue. By default this will be twice the
     * {@link #maxBatchSize() maximum batch size}.
     *
     * @return the maximum queue size
     */
    public int maxQueueSize() {
      return maxQueueSize < 1 ? maxBatchSize() * 2 : maxQueueSize;
    }

    /**
     * Set the maximum duration that a single call to {@link RecordQueue#poll()} should
     * block while waiting for a batch to fill.
     *
     * <p>By default this is 0 seconds.
     *
     * @param duration the maximum duration for a poll for records; may not be null
     * @return this builder; never null
     * @throws NullPointerException if the duration is null
     */
    public Builder<RecordT> maxPollLinger(Duration duration) {
      this.linger = Objects.requireNonNull(duration);
      return this;
    }

    /**
     * Set the maximum time for the {@link RecordQueue#poll()} to detect that the queue has been
     * stopped or a failure has occurred.
     *
     * <p>By default this is 1 second.
     *
     * @param duration the maximum interrupt detection time during a poll for records;
     *                 may not be null
     * @return this builder; never null
     * @throws NullPointerException if the duration is null
     */
    public Builder<RecordT> maxPollInterruptTime(Duration duration) {
      this.maxPollInterruptTime = Objects.requireNonNull(duration);
      return this;
    }

    /**
     * Set the clock that the {@link RecordQueue} should use when determining poll timeouts.
     *
     * @param clock the clock; may not be null
     * @return this builder; never null
     * @throws NullPointerException if the clock is null
     */
    public Builder<RecordT> clock(Time clock) {
      this.clock = Objects.requireNonNull(clock);
      return this;
    }

    /**
     * Set the function that will be used to create the batch lists given the maximum batch size.
     *
     * <p>By default, the {@link ArrayList#ArrayList(int)} constructor is used.
     *
     * @param batchFactory the function used to create batches given the maximum batch size; may
     *                     not be null
     * @return this builder; never null
     * @throws NullPointerException if {@code batchFactory} is null
     */
    public Builder<RecordT> batchFactory(IntFunction<List<SourceRecord>> batchFactory) {
      this.batchFactory = Objects.requireNonNull(batchFactory);
      return this;
    }

    /**
     * Set the function that will be used to create the blocking queue implementation given the
     * maximum capacity of the queue.
     *
     * <p>By default, the {@link ArrayBlockingQueue#ArrayBlockingQueue(int)} constructor is used.
     *
     * @param queueFactory the function to create the blocking queue implementation; may not be null
     * @return this builder; never null
     * @throws NullPointerException if {@code queueFactory} is null
     */
    public Builder<RecordT> queueFactory(IntFunction<BlockingQueue<SourceRecord>> queueFactory) {
      this.queueFactory = Objects.requireNonNull(queueFactory);
      return this;
    }

    /**
     * Set the function that will be used to create the {@link ExecutorService} used to execute
     * record generator functions.
     *
     * <p>By default, the {@link Executors#newCachedThreadPool()} method is used.
     *
     * <p>This will override any previous invocation of {@link #maxExecutorThreads(int)}.
     *
     * @param executorFactory the function to create the executor service; may not be null
     * @return this builder; never null
     * @throws NullPointerException if {@code executorFactory} is null
     */
    public Builder<RecordT> executorFactory(Supplier<ExecutorService> executorFactory) {
      this.executorFactory = Objects.requireNonNull(executorFactory);
      return this;
    }

    /**
     * Set the maximum number of threads that the {@link ExecutorService} will use
     * to execute record generator functions.
     *
     * <p>This will override any previous invocation of {@link #executorFactory(Supplier)}.
     *
     * @param maxThreads the maximum number of threads; must be positive
     * @return this builder; never null
     * @throws IllegalArgumentException if {@code maxThreads} is not positive
     */
    public Builder<RecordT> maxExecutorThreads(int maxThreads) {
      return executorFactory(() -> {
        ThreadPoolExecutor executor = new ThreadPoolExecutor(
            maxThreads,
            maxThreads,
            60L,
            TimeUnit.SECONDS,
            new LinkedBlockingQueue<>()
        );
        executor.allowCoreThreadTimeOut(true);
        executor.prestartCoreThread();
        return executor;
      });
    }

    public Builder<RecordT> queue(BlockingQueue<SourceRecord> queue) {
      this.queue = queue;
      return this;
    }

    /**
     * Create a new {@link RecordQueue} instance using this builder's current state.
     *
     * @return the new {@link RecordQueue}; never null
     */
    public RecordQueue<RecordT> build() {
      return new RecordQueue<>(this);
    }
  }

  /**
   * Create a new builder for {@link RecordQueue} instances.
   *
   * @param <T> the type of record
   * @return the builder; never null
   */
  public static <T extends SourceRecord> Builder<T> builder() {
    return new Builder<>();
  }

  private final int maxBatchSize;
  private final boolean useLinger;
  private final Duration linger;
  private final Duration maxPollInterruptTime;
  private final BlockingQueue<SourceRecord> queue;
  private final Time clock;
  private final AtomicBoolean running = new AtomicBoolean(true);
  private final AtomicReference<Throwable> failure = new AtomicReference<>();
  private final IntFunction<List<SourceRecord>> batchFactory;
  private final ExecutorService executor;

  protected RecordQueue(Builder<RecordT> builder) {
    this.maxBatchSize = builder.maxBatchSize;
    this.linger = builder.linger;
    this.useLinger = !linger.isZero() && !linger.isNegative();
    this.maxPollInterruptTime = builder.maxPollInterruptTime;
    this.clock = builder.clock;
    this.batchFactory = builder.batchFactory;
    this.queue = builder.queue != null
        ? builder.queue
        : builder.queueFactory.apply(builder.maxQueueSize());
    this.executor = builder.executorFactory.get();
  }

  /**
   * Submit for asynchronous execution the given generator function with an operation name and
   * log context. The generator function will be called with this record destination so that all
   * generated records are sent to this queue.
   *
   * <p>The generator function can contain as long as the destination {@link #isRunning() is
   * running}.
   *
   * <p>This reads the Connect MDC log context of the task, and sets the execution thread's MDC log
   * context to the task's context plus the specified log context. For example, if the task's
   * logging context were:
   * <pre>
   *   [my-connector|task-0]
   * </pre>
   * and the given log context is "snapshot-1", then the execution thread's logging context
   * would be set to:
   * <pre>
   *   [my-connector|task-0|snapshot-1]
   * </pre>
   *
   * <p>The resulting {@link Future} can be used to monitor, cancel, and obtain the result of
   * the generator function's execution.
   *
   * @param generatorProcessor the processing function; may not be null
   * @param <T>                the result type for the function
   * @param operationName      the name of the operation
   * @param logContext         the log context
   * @return a future for the execution; never null
   * @throws NullPointerException if the specified element is null
   * @throws DestinationClosedException if the element cannot be sent at this
   *         time because the destination has been closed and will not accept additional records
   */
  public <T> CompletableFuture<T> submit(
      String operationName,
      String logContext,
      Function<RecordDestination<RecordT>, T> generatorProcessor
  ) {
    log.trace("Submitting {} operation", operationName);
    Objects.requireNonNull(generatorProcessor);
    verifyRunning();

    // Set up the destination so that we can cancel it from the future before
    // this destination is stopped
    final AtomicBoolean stillRun = new AtomicBoolean(true);
    final RecordDestination<RecordT> cancellableDestination =
        this.withAdditionalRunningCondition(stillRun::get);

    // Capture the caller's MDC (which includes Connect's connector.context)
    // so it can be restored on the executor thread
    Map<String, String> callerMdc = MDC.getCopyOfContextMap();

    // Submit the function and use the cancellable destination
    Supplier<T> supplier = createLoggingSupplier(
        operationName,
        logContext,
        cancellableDestination,
        generatorProcessor,
        callerMdc
    );
    return CompletableFuture.supplyAsync(supplier, executor);
  }

  @Override
  public final boolean isRunning() {
    return running.get();
  }

  @Override
  public void send(RecordT record) {
    verifyRunning();
    queue.add(Objects.requireNonNull(record));
  }

  @Override
  public boolean send(RecordT record, Duration timeout) throws InterruptedException {
    verifyRunning();
    return queue.offer(Objects.requireNonNull(record), timeout.toMillis(), TimeUnit.MILLISECONDS);
  }

  @Override
  public void failWith(Throwable failure) {
    this.failure.compareAndSet(null, Objects.requireNonNull(failure));
  }

  /**
   * Determine whether this queue has no failures.
   *
   * @return true there are no failures, or false if there is a failure
   */
  protected final boolean hasNoFailure() {
    return failure.get() == null;
  }

  /**
   * Poll for the next batch of records. This method will block up to the maximum batch
   *
   * @return the next batch of records, which may be empty if there are no records in the queue;
   *         or null if the queue has been {@link #stop() stopped}
   * @throws InterruptedException if the thread was interrupted while waiting to dequeue records
   *         from the queue
   * @throws ConnectException if the producer {@link #failWith(Throwable) failed} with an
   *         exception that should be propagated to the Connect framework
   */
  public List<SourceRecord> poll() throws InterruptedException {
    List<SourceRecord> records = batchFactory.apply(maxBatchSize);
    if (!isRunning()) {
      // Discarding all records
      log.debug("Returning null batch after shutdown");
      return null;
    }

    if (failure.get() != null) {
      throw new ConnectException("Error while polling for records", failure.get());
    }

    // Drain all available records up to the max batch size
    int drained = queue.drainTo(records, maxBatchSize);

    if (drained > 0) {
      // We have found at least one record, which means that we've either filled our batch
      // or we've drained all available records
      log.debug("Returning {} records after draining queue", drained);
      return records;
    }

    // We have no records, so maybe linger until there are some
    if (useLinger) {
      int count = drained;
      long lingerRemainderMs = linger.toMillis();
      final long start = clock.milliseconds();
      final long lingerEndMs = start + lingerRemainderMs;
      SourceRecord record;
      while (isRunning() && hasNoFailure() && count < maxBatchSize && lingerRemainderMs > 0L) {

        // Since we need to be able to detect that we've stopped within a max interrupt time,
        // we can wait no more than this interrupt time or the remainder of our linger
        // (whichever is smaller)
        long maxPollTime = Math.min(maxPollInterruptTime.toMillis(), lingerRemainderMs);
        record = queue.poll(maxPollTime, TimeUnit.MILLISECONDS);
        if (record != null) {
          records.add(record);
          ++count;
        }
        lingerRemainderMs = lingerEndMs - clock.milliseconds();
      }

      if (log.isTraceEnabled()) {
        lingerRemainderMs = Math.max(0, lingerRemainderMs);
        log.trace(
            "Returning {} records after lingering {} ms; {} records waiting in buffer",
            count,
            linger.toMillis() - lingerRemainderMs,
            queue.size()
        );
      }
    } else {
      log.trace("Returning 0 records; {} records waiting in buffer", queue.size());
    }
    return records;
  }

  /**
   * Stop this queue, ensuring that subsequent calls to {@link #poll()} will return a null list.
   *
   * @return true if this queue was stopped, or false if it was already stopped
   * @see #awaitTermination(Duration)
   */
  public boolean stop() {
    if (!running.compareAndSet(true, false)) {
      return false;
    }
    executor.shutdownNow();
    return true;
  }

  /**
   * Stop this queue, ensuring that subsequent calls to {@link #poll()} will return a null list.
   *
   * @param timeout the maximum amount of time to wait before all processes are stopped
   * @return true if this queue was stopped successfully within the timeout, or false if it could
   *         not be stopped within the timeout
   * @throws InterruptedException if the thread were interrupted while waiting for the
   *         {@link #submit submitted generators} to stop
   * @see #stop()
   */
  public boolean awaitTermination(Duration timeout) throws InterruptedException {
    return executor.awaitTermination(timeout.toMillis(), TimeUnit.MILLISECONDS);
  }

  private void verifyRunning() {
    if (!isRunning()) {
      throw new RecordDestination.DestinationClosedException();
    }
  }

  protected <T> Supplier<T> createLoggingSupplier(
      String operationName,
      String logContext,
      RecordDestination<RecordT> destination,
      Function<RecordDestination<RecordT>, T> generatorProcessor,
      Map<String, String> callerMdc
  ) {
    return () -> {
      // Restore the caller's MDC on this executor thread so that
      // ConnectLogContext can find and extend the connector.context
      if (callerMdc != null) {
        MDC.setContextMap(callerMdc);
      }
      try (ConnectLogContext context = new ConnectLogContext(logContext)) {
        try {
          log.debug("{}Starting {}", context.prefix(), operationName);
          return generatorProcessor.apply(destination);
        } catch (Throwable e) {
          log.error("{}Exception in RecordQueue thread", context.prefix(), e);
          failWith(e);
          throw e;
        } finally {
          log.debug("{}Stopped {}", context.prefix(), operationName);
        }
      } finally {
        // Clean up MDC to prevent leaking context to pooled threads
        MDC.clear();
      }
    };
  }
}
