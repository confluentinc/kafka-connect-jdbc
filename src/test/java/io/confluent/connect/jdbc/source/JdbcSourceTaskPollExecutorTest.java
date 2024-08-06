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
import static java.util.Arrays.asList;
import static java.util.Collections.unmodifiableList;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.testcontainers.shaded.org.awaitility.Awaitility.await;

import java.util.ArrayList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.kafka.common.utils.Time;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class JdbcSourceTaskPollExecutorTest {
  private static final Logger LOG = LoggerFactory.getLogger(JdbcSourceConnectorConfigTest.class);
  private static final long TEST_TIMEOUT_MS = 1_000L;

  private final Queue<SourceRecord> recordsToPoll = new LinkedBlockingQueue<>();
  private final Queue<String> pollThreadNames = new LinkedBlockingQueue<>();
  private final AtomicInteger interruptedPollThreads = new AtomicInteger();
  private final AtomicReference<Runnable> pollInterceptor = new AtomicReference<>();
  private JdbcSourceConnectorConfig config;

  @Before
  public void setup() {
    config = mock(JdbcSourceConnectorConfig.class);
    doReturn("test-connector").when(config).getString("name");
    doReturn("7").when(config).getString("task.id");

    recordsToPoll.clear();
    pollThreadNames.clear();
    interruptedPollThreads.set(0);
    pollInterceptor.set(null);
  }

  @Test(timeout = TEST_TIMEOUT_MS)
  public void shouldPollSourceRecordsInTheCurrentThreadWhenPollMaxWaitTimeIsZero() throws InterruptedException {
    // given
    List<SourceRecord> expected = asList(mock(SourceRecord.class), mock(SourceRecord.class));
    recordsToPoll.addAll(expected);
    doReturn(0).when(config).getInt(POLL_MAX_WAIT_TIME_MS_CONFIG);
    try (JdbcSourceTaskPollExecutor tested = createPollExecutor()) {
      // when
      List<SourceRecord> result = tested.poll();

      // then
      assertEquals(expected, result);
      assertEquals(1, pollThreadNames.size());
      assertEquals(Thread.currentThread().getName(), pollThreadNames.peek());
      assertEquals(0, interruptedPollThreads.get());
    }
  }

  @Test(timeout = TEST_TIMEOUT_MS)
  public void shouldPollSourceRecordsInAnotherThreadWhenPollMaxWaitTimeIsAboveZero() throws InterruptedException {
    // given
    List<SourceRecord> expected = asList(mock(SourceRecord.class));
    recordsToPoll.addAll(expected);
    doReturn(1).when(config).getInt(POLL_MAX_WAIT_TIME_MS_CONFIG);
    try (JdbcSourceTaskPollExecutor tested = createPollExecutor()) {
      // when
      List<SourceRecord> result = tested.poll();

      // then
      assertEquals(expected, result);
      assertEquals(1, pollThreadNames.size());
      assertEquals("test-connector-poll-thread-7", pollThreadNames.peek());
      assertNotEquals(Thread.currentThread().getName(), pollThreadNames.peek());
      assertEquals(0, interruptedPollThreads.get());
    }
  }

  @Test(timeout = TEST_TIMEOUT_MS)
  public void shouldReturnNullSourceRecordsWhenPollMaxWaitTimeIsExceeded() throws InterruptedException {
   // given
   List<SourceRecord> expected = asList(mock(SourceRecord.class));
   CountDownLatch pollLatch = new CountDownLatch(1);
   recordsToPoll.addAll(expected);
   blockPollingWith(pollLatch);
   doReturn(100).when(config).getInt(POLL_MAX_WAIT_TIME_MS_CONFIG);
   try (JdbcSourceTaskPollExecutor tested = createPollExecutor()) {
     // when
     List<SourceRecord> result1 = tested.poll();

     // then
     assertNull(result1);
     assertEquals(1, pollThreadNames.size());
     assertNotEquals(Thread.currentThread().getName(), pollThreadNames.peek());

     // when called again
     List<SourceRecord> result2 = tested.poll();

     // then still no result
     assertNull(result2);
     assertEquals(1, pollThreadNames.size());

     // when unblocked
     pollLatch.countDown();
     List<SourceRecord> result3 = tested.poll();

     // then polling results should be finally returned
     assertEquals(expected, result3);
     assertEquals(1, pollThreadNames.size());
     assertEquals(0, interruptedPollThreads.get());
    }
  }

  @Test(timeout = TEST_TIMEOUT_MS)
  public void shouldReturnResultOfPreviouslyStartedAndFinishedPolling() throws InterruptedException {
   // given
   List<SourceRecord> expected = asList(mock(SourceRecord.class), mock(SourceRecord.class));
   List<SourceRecord> expectedNextResult = asList(mock(SourceRecord.class));
   CountDownLatch pollLatch = new CountDownLatch(1);
   recordsToPoll.addAll(expected);
   doReturn(100).when(config).getInt(POLL_MAX_WAIT_TIME_MS_CONFIG);
   blockPollingWith(pollLatch);
   try (JdbcSourceTaskPollExecutor tested = createPollExecutor()) {
     assertNull(tested.poll());
     pollLatch.countDown();
     // waiting for the started polling to be finished
     await().atMost(200, MILLISECONDS).until(() -> recordsToPoll.isEmpty());

     // when
     List<SourceRecord> result1 = tested.poll();

     // then
     assertEquals(expected, result1);
     assertEquals(1, pollThreadNames.size());
     assertNotEquals(Thread.currentThread().getName(), pollThreadNames.peek());

     // when new records are added and next polling is called
     recordsToPoll.addAll(expectedNextResult);
     List<SourceRecord> result2 = tested.poll();

     // then the new results should be returned in a new thread
     assertEquals(expectedNextResult, result2);
     assertEquals(2, pollThreadNames.size());
     assertEquals(0, interruptedPollThreads.get());
    }
  }

  @Test(timeout = TEST_TIMEOUT_MS)
  public void shouldRethrowPollRuntimeException() {
    ConnectException expectedException = new ConnectException("something bad happened:(");
    doReturn(50).when(config).getInt(POLL_MAX_WAIT_TIME_MS_CONFIG);
    pollInterceptor.set(() -> {
        throw expectedException;
    });
    try (JdbcSourceTaskPollExecutor tested = createPollExecutor()) {
      // when
      Exception caughtException = assertThrows(Exception.class, tested::poll);

      // then
      assertEquals(expectedException, caughtException);
     }
  }

  @Test(timeout = TEST_TIMEOUT_MS)
  public void shouldCancelCurrentPollingOnClose() throws InterruptedException {
    // given
    CountDownLatch pollLatch = new CountDownLatch(1);
    recordsToPoll.offer(mock(SourceRecord.class));
    doReturn(50).when(config).getInt(POLL_MAX_WAIT_TIME_MS_CONFIG);
    blockPollingWith(pollLatch);
    try (JdbcSourceTaskPollExecutor tested = createPollExecutor()) {
      assertNull(tested.poll());

      // when
      tested.close();

      // then
      await().atMost(200, MILLISECONDS).until(() -> recordsToPoll.isEmpty());
      assertEquals(1, interruptedPollThreads.get());
    }
  }

  private JdbcSourceTaskPollExecutor createPollExecutor() {
    return new JdbcSourceTaskPollExecutor(Time.SYSTEM, config, this::poll);
  }

  private void blockPollingWith(CountDownLatch latch) {
    pollInterceptor.set(() -> {
      if (latch.getCount() > 0) {
        try {
          if (!latch.await(TEST_TIMEOUT_MS, MILLISECONDS)) {
            throw new AssertionError("Timeout while waiting to unblock polling");
          }
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          LOG.warn("Polling lock interrupted", e);
        }
      }
    });
  }

  private List<SourceRecord> poll() {
    Runnable pollHandler = pollInterceptor.get();
    pollThreadNames.offer(Thread.currentThread().getName());
    if (pollHandler != null) {
      pollHandler.run();
    }
    final List<SourceRecord> result = new ArrayList<>();
    while (!recordsToPoll.isEmpty()) {
      result.add(recordsToPoll.poll());
    }
    if (Thread.currentThread().isInterrupted()) {
      interruptedPollThreads.incrementAndGet();
    }
    return unmodifiableList(result);
  }
}
