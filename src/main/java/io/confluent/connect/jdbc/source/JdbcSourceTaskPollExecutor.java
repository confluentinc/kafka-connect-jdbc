/*
 * Copyright 2018 Confluent Inc.
 *
 * Licensed under the Confluent Community License (the "License"); you may not use
 * this file except in compliance with the License.  You may obtain a copy of the
 * License at
 *
 * http://www.confluent.io/confluent-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OF ANY KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations under the License.
 */

package io.confluent.connect.jdbc.source;

import static io.confluent.connect.jdbc.source.JdbcSourceConnectorConfig.POLL_MAX_WAIT_TIME_MS_CONFIG;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

import java.io.Closeable;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

import org.apache.kafka.common.utils.Time;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.source.SourceRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Utility responsible for managing execution of the JDBC source task poll operation.
 * <p>
 * When the poll.max.wait.time.ms is set to zero, the executor will simply execute the poll
 * operation directly in the current thread.
 * Otherwise, the poll operation will be executed in a new thread and the executor will
 * wait up to the configured poll.max.wait.time.ms time for the started thread to finish.
 * If the thread is not finished in time, the executor will return a null list of
 * source records, signaling there is no data to the worker.
 * In the next poll call, the executor will either try to wait again for the previously
 * started thread to finish, or create a new one and apply the same wait logic.
 * </p>
 */
final class JdbcSourceTaskPollExecutor implements Closeable {
  private static final Logger LOG = LoggerFactory.getLogger(JdbcSourceTaskPollExecutor.class);
  private static final List<SourceRecord> NO_DATA = null;

  private final AtomicReference<PollingFuture> pollFuture = new AtomicReference<>();
  private final AtomicBoolean closed = new AtomicBoolean();

  private final Time time;
  private final ExecutorService pollTaskExecutor;
  private final Supplier<List<SourceRecord>> pollOperation;
  private final int pollMaxWaitTimeMs;

  /**
   * Creates the {@link JdbcSourceTaskPollExecutor}.
   *
   * @param time
   *          the component providing the current time measurement
   * @param config
   *          the configuration of the JDBC source connector
   * @param pollOperation
   *          the poll operation function
   */
  JdbcSourceTaskPollExecutor(Time time, JdbcSourceConnectorConfig config,
      Supplier<List<SourceRecord>> pollOperation) {
    this.time = requireNonNull(time, "time must not be null");
    this.pollOperation = requireNonNull(pollOperation, "pollOperation must not be null");
    pollMaxWaitTimeMs = requireNonNull(config, "config must not be null")
        .getInt(POLL_MAX_WAIT_TIME_MS_CONFIG);
    pollTaskExecutor = Executors.newSingleThreadExecutor(createPollThreadFactory(config));
  }

  List<SourceRecord> poll() throws InterruptedException {
    if (isClosed()) {
      LOG.debug("Ignoring poll request - the {} is closed", getClass().getSimpleName());
      return NO_DATA;
    }
    if (pollMaxWaitTimeMs <= 0) {
      // waiting without timeout
      return pollOperation.get();
    }
    PollingFuture polling = getOrCreatePollingFuture();
    try {
      List<SourceRecord> result = polling.future.get(pollMaxWaitTimeMs, MILLISECONDS);
      pollFuture.compareAndSet(polling, null);
      return result;
    } catch (@SuppressWarnings("unused") TimeoutException e) {
      LOG.info("Polling exceeded maximum duration of {}ms the total elapsed time is {}ms",
          pollMaxWaitTimeMs, polling.elapsed(time));
      return NO_DATA;
    } catch (CancellationException e) {
      LOG.debug("Polling cancelled", e);
      return NO_DATA;
    } catch (ExecutionException e) {
      if (e.getCause() instanceof RuntimeException) {
        throw (RuntimeException) e.getCause();
      }
      throw new ConnectException("Error while polling", e);
    }
  }

  private boolean isClosed() {
    return closed.get();
  }

  private PollingFuture getOrCreatePollingFuture() {
    return pollFuture.updateAndGet(polling -> polling != null && !polling.future.isCancelled()
        ? polling : new PollingFuture(time.milliseconds(),
            pollTaskExecutor.submit(pollOperation::get)));
  }

  @Override
  public void close() {
    closed.set(true);
    cancelCurrentPolling();
    pollTaskExecutor.shutdown();
  }

  private void cancelCurrentPolling() {
    pollFuture.updateAndGet(polling -> {
      if (polling != null) {
        polling.future.cancel(true);
      }
      return null;
    });
  }

  private static ThreadFactory createPollThreadFactory(JdbcSourceConnectorConfig config) {
    final String threadName = pollThreadName(config);
    return task -> {
      Thread thread = new Thread(task);
      thread.setName(threadName);
      return thread;
    };
  }

  private static String pollThreadName(JdbcSourceConnectorConfig config) {
    Map<String, String> originalStrings = config.originalsStrings();
    String connectorName = originalStrings.getOrDefault("name", "connector");
    String taskId = originalStrings.getOrDefault("task.id", "0");
    return String.format("%s-poll-thread-%s", connectorName, taskId);
  }

  private static class PollingFuture {
    private final long startTimeMillis;
    private final Future<List<SourceRecord>> future;

    private PollingFuture(long startTimeMillis, Future<List<SourceRecord>> future) {
      this.startTimeMillis = startTimeMillis;
      this.future = future;
    }

    private long elapsed(Time time) {
      return time.milliseconds() - startTimeMillis;
    }
  }
}
