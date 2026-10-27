/*
 * Copyright [2024 - 2024] Confluent Inc.
 */

package io.confluent.connect.jdbc.util;

import io.confluent.connect.jdbc.source.MockTime;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.After;
import org.junit.Test;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

public class RecordQueueTest {

  private static final Duration LONG_LINGER = Duration.ofSeconds(5);
  private static final long MIN_WAIT_MS = RecordQueue.MIN_IDLE_POLL_WAIT.toMillis();

  private RecordQueue<SourceRecord> queue;

  @After
  public void tearDown() {
    if (queue != null) {
      queue.stop();
    }
  }

  @Test
  public void pollReturnsAsSoonAsTheFirstRecordArrives() throws Exception {
    ScriptedQueue backing = new ScriptedQueue().arriving(record(1));
    queue = RecordQueue.builder().maxBatchSize(1000).maxPollLinger(LONG_LINGER).queue(backing)
        .build();

    List<SourceRecord> batch = queue.poll();

    // A low-volume table never fills a batch, so the record must not be held for the linger.
    assertEquals(1, batch.size());
    assertEquals("poll() kept waiting after the record arrived", 1, backing.timeoutsMs.size());
  }

  @Test
  public void pollReturnsTheRecordsQueuedBehindTheFirstOne() throws Exception {
    ScriptedQueue backing = new ScriptedQueue().arriving(record(1), record(2), record(3));
    queue = RecordQueue.builder().maxBatchSize(1000).maxPollLinger(LONG_LINGER).queue(backing)
        .build();

    List<SourceRecord> batch = queue.poll();

    assertEquals(3, batch.size());
    assertEquals("poll() waited for more records", 1, backing.timeoutsMs.size());
  }

  @Test
  public void pollWithZeroLingerBlocksBrieflyOnAnEmptyQueue() throws Exception {
    queue = RecordQueue.builder().maxPollLinger(Duration.ZERO).build();

    long start = System.nanoTime();
    List<SourceRecord> batch = queue.poll();
    long elapsedMs = elapsedMs(start);

    // Returning at once would make the worker's poll loop spin at full CPU while idle.
    assertTrue(batch.isEmpty());
    assertTrue("poll() returned after " + elapsedMs + " ms", elapsedMs >= MIN_WAIT_MS);
  }

  @Test
  public void pollWithZeroLingerReturnsARecordThatArrivesWhileWaiting() throws Exception {
    ScriptedQueue backing = new ScriptedQueue().arriving(record(1));
    queue = RecordQueue.builder().maxPollLinger(Duration.ZERO).queue(backing).build();

    List<SourceRecord> batch = queue.poll();

    assertEquals(1, batch.size());
    assertEquals(Collections.singletonList(MIN_WAIT_MS), backing.timeoutsMs);
  }

  @Test(timeout = 10_000)
  public void pollWithZeroLingerWaitsTheMinimumEvenWithShortInterruptSlices() throws Exception {
    ScriptedQueue backing = new ScriptedQueue();
    queue = RecordQueue.builder()
        .maxPollLinger(Duration.ZERO)
        .maxPollInterruptTime(Duration.ofMillis(10))
        .clock(new MockTime())
        .queue(backing)
        .build();

    List<SourceRecord> batch = queue.poll();

    assertTrue(batch.isEmpty());
    assertEquals(MIN_WAIT_MS, backing.totalTimeoutMs());
    assertTrue(backing.timeoutsMs.stream().allMatch(ms -> ms <= 10));
  }

  @Test(timeout = 10_000)
  public void pollStopsLingeringWhenTheClockDoesNotAdvance() throws Exception {
    // The task's MockTime only moves when something sleeps on it, so the time already spent in
    // empty timed polls must end the linger by itself.
    ScriptedQueue backing = new ScriptedQueue();
    queue = RecordQueue.builder()
        .maxPollLinger(Duration.ofMillis(250))
        .maxPollInterruptTime(Duration.ofMillis(100))
        .clock(new MockTime())
        .queue(backing)
        .build();

    List<SourceRecord> batch = queue.poll();

    assertTrue(batch.isEmpty());
    assertEquals(Arrays.asList(100L, 100L, 50L), backing.timeoutsMs);
  }

  @Test(timeout = 10_000)
  public void pollWithTheMostNegativeLingersWaitsTheMinimum() throws Exception {
    // On Java 8, Duration.toMillis() overflows for both values.
    assertHugeNegativeLingerWaitsTheMinimum(Long.MIN_VALUE);
    assertHugeNegativeLingerWaitsTheMinimum(Long.MIN_VALUE + 1);
  }

  private void assertHugeNegativeLingerWaitsTheMinimum(long lingerMs) throws Exception {
    // A clock jump during the first wait, as after a long GC pause, must not overflow the
    // remaining linger into a huge positive wait.
    MockTime clock = new MockTime();
    ScriptedQueue backing = new ScriptedQueue().duringEachPoll(() -> clock.sleep(1000));
    queue = RecordQueue.builder()
        .maxPollLinger(Duration.ofMillis(lingerMs))
        .clock(clock)
        .queue(backing)
        .build();

    List<SourceRecord> batch = queue.poll();
    queue.stop();

    assertTrue(batch.isEmpty());
    assertEquals(Collections.singletonList(MIN_WAIT_MS), backing.timeoutsMs);
  }

  @Test(timeout = 10_000)
  public void pollWithAHugePositiveLingerWaitsInInterruptSlices() throws Exception {
    ScriptedQueue backing = new ScriptedQueue().arriving(record(1));
    queue = RecordQueue.builder()
        .maxPollLinger(Duration.ofMillis(Long.MAX_VALUE))
        .maxPollInterruptTime(Duration.ofMillis(100))
        .queue(backing)
        .build();

    List<SourceRecord> batch = queue.poll();

    assertEquals(1, batch.size());
    assertEquals(Collections.singletonList(100L), backing.timeoutsMs);
  }

  @Test
  public void pollReturnsAtMostTheMaxBatchSizeAndKeepsTheRestInOrder() throws Exception {
    ScriptedQueue backing = new ScriptedQueue().arriving(record(1), record(2), record(3));
    queue = RecordQueue.builder().maxBatchSize(2).maxPollLinger(LONG_LINGER).queue(backing)
        .build();

    assertEquals(Arrays.asList(1, 2), values(queue.poll()));
    assertEquals(Collections.singletonList(3), values(queue.poll()));
    assertEquals("poll() waited although records were queued", 1, backing.timeoutsMs.size());
  }

  @Test
  public void pollWithABatchSizeOfOneReturnsOneRecordAtATime() throws Exception {
    ScriptedQueue backing = new ScriptedQueue().arriving(record(1), record(2));
    queue = RecordQueue.builder().maxBatchSize(1).maxPollLinger(LONG_LINGER).queue(backing)
        .build();

    assertEquals(Collections.singletonList(1), values(queue.poll()));
    assertEquals(Collections.singletonList(2), values(queue.poll()));
  }

  @Test(timeout = 10_000)
  public void pollReturnsNullWhenTheQueueIsStoppedWhileWaiting() throws Exception {
    ScriptedQueue backing = new ScriptedQueue().duringEachPoll(() -> queue.stop());
    queue = RecordQueue.builder()
        .maxPollLinger(LONG_LINGER)
        .maxPollInterruptTime(Duration.ofMillis(50))
        .queue(backing)
        .build();

    assertNull(queue.poll());
    assertEquals(1, backing.timeoutsMs.size());
  }

  @Test(timeout = 10_000)
  public void pollThrowsWhenTheProducerFailsWhileWaiting() throws Exception {
    ScriptedQueue backing = new ScriptedQueue()
        .duringEachPoll(() -> queue.failWith(new RuntimeException("query failed")));
    queue = RecordQueue.builder()
        .maxPollLinger(LONG_LINGER)
        .maxPollInterruptTime(Duration.ofMillis(50))
        .queue(backing)
        .build();

    ConnectException e = assertThrows(ConnectException.class, () -> queue.poll());
    assertEquals("query failed", e.getCause().getMessage());
    assertEquals(1, backing.timeoutsMs.size());
  }

  @Test(timeout = 10_000)
  public void pollReturnsAnEmptyBatchOnceTheLingerExpires() throws Exception {
    Duration linger = Duration.ofMillis(300);
    queue = RecordQueue.builder().maxPollLinger(linger).build();

    long start = System.nanoTime();
    List<SourceRecord> batch = queue.poll();
    long elapsedMs = elapsedMs(start);

    assertTrue(batch.isEmpty());
    assertTrue("poll() returned after " + elapsedMs + " ms", elapsedMs >= linger.toMillis());
  }

  private static SourceRecord record(int id) {
    return new SourceRecord(null, null, "topic", 0, null, id);
  }

  private static long elapsedMs(long startNanos) {
    return TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startNanos);
  }

  private static List<Object> values(List<SourceRecord> batch) {
    return batch.stream().map(SourceRecord::value).collect(Collectors.toList());
  }

  /**
   * A queue that records the timeout of every timed poll, and that makes records arrive or runs
   * an action during a timed poll, so that the tests do not depend on thread scheduling.
   */
  private static class ScriptedQueue extends ArrayBlockingQueue<SourceRecord> {
    private final List<SourceRecord> arrivals = new ArrayList<>();
    private final List<Long> timeoutsMs = new ArrayList<>();
    private Runnable duringEachPoll = () -> { };

    ScriptedQueue() {
      super(10);
    }

    /** Make these records arrive while the first timed poll waits. */
    ScriptedQueue arriving(SourceRecord... records) {
      arrivals.addAll(Arrays.asList(records));
      return this;
    }

    /** Run this action during every timed poll, such as moving the clock or stopping. */
    ScriptedQueue duringEachPoll(Runnable action) {
      this.duringEachPoll = action;
      return this;
    }

    long totalTimeoutMs() {
      return timeoutsMs.stream().mapToLong(Long::longValue).sum();
    }

    @Override
    public SourceRecord poll(long timeout, TimeUnit unit) throws InterruptedException {
      if (timeoutsMs.isEmpty()) {
        addAll(arrivals);
      }
      timeoutsMs.add(unit.toMillis(timeout));
      duringEachPoll.run();
      return super.poll(timeout, unit);
    }
  }
}
