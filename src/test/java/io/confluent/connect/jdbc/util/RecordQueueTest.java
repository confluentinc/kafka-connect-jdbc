/*
 * Copyright [2024 - 2024] Confluent Inc.
 */

package io.confluent.connect.jdbc.util;

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
import static org.junit.Assert.assertTrue;

public class RecordQueueTest {

  /** The default, and the minimum, that the source connector config allows. */
  private static final Duration LINGER = Duration.ofMillis(100);

  private RecordQueue<SourceRecord> queue;

  @After
  public void tearDown() {
    if (queue != null) {
      queue.stop();
    }
  }

  @Test
  public void pollReturnsQueuedRecordsWithoutWaiting() throws Exception {
    ScriptedQueue backing = new ScriptedQueue();
    queue = RecordQueue.builder().maxPollLinger(LINGER).queue(backing).build();
    backing.add(record(1));
    backing.add(record(2));

    assertEquals(Arrays.asList(1, 2), values(queue.poll()));
    assertTrue("poll() waited although records were queued", backing.timeoutsMs.isEmpty());
  }

  @Test(timeout = 10_000)
  public void pollKeepsRecordsArrivingDuringTheLingerInOneBatch() throws Exception {
    // A burst that arrives while poll() waits stays in one batch, which with exactly-once source
    // and transaction.boundary=poll is one transaction
    ScriptedQueue backing = new ScriptedQueue().arriving(record(1), record(2), record(3));
    queue = RecordQueue.builder().maxBatchSize(1000).maxPollLinger(LINGER).queue(backing).build();

    assertEquals(Arrays.asList(1, 2, 3), values(queue.poll()));
  }

  @Test(timeout = 10_000)
  public void pollReturnsAnEmptyBatchAfterWaitingTheLinger() throws Exception {
    queue = RecordQueue.builder().maxPollLinger(LINGER).build();

    long start = System.nanoTime();
    List<SourceRecord> batch = queue.poll();
    long elapsedMs = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start);

    // Returning at once would make the worker's poll loop spin at full CPU while idle
    assertTrue(batch.isEmpty());
    assertTrue("poll() returned after " + elapsedMs + " ms", elapsedMs >= LINGER.toMillis());
  }

  @Test(timeout = 10_000)
  public void pollReturnsAFullBatchWithoutWaitingOutTheLinger() throws Exception {
    ScriptedQueue backing = new ScriptedQueue().arriving(record(1), record(2), record(3));
    queue = RecordQueue.builder()
        .maxBatchSize(2)
        .maxPollLinger(Duration.ofSeconds(60))
        .queue(backing)
        .build();

    assertEquals(Arrays.asList(1, 2), values(queue.poll()));
    assertEquals(Collections.singletonList(3), values(queue.poll()));
  }

  private static SourceRecord record(int id) {
    return new SourceRecord(null, null, "topic", 0, null, id);
  }

  private static List<Object> values(List<SourceRecord> batch) {
    return batch.stream().map(SourceRecord::value).collect(Collectors.toList());
  }

  /**
   * A queue that records the timeout of every timed poll, and that makes records arrive during
   * the first timed poll, so that the tests do not depend on thread scheduling.
   */
  private static class ScriptedQueue extends ArrayBlockingQueue<SourceRecord> {
    private final List<SourceRecord> arrivals = new ArrayList<>();
    private final List<Long> timeoutsMs = new ArrayList<>();

    ScriptedQueue() {
      super(10);
    }

    /** Make these records arrive while the first timed poll waits. */
    ScriptedQueue arriving(SourceRecord... records) {
      arrivals.addAll(Arrays.asList(records));
      return this;
    }

    @Override
    public SourceRecord poll(long timeout, TimeUnit unit) throws InterruptedException {
      if (timeoutsMs.isEmpty()) {
        addAll(arrivals);
      }
      timeoutsMs.add(unit.toMillis(timeout));
      return super.poll(timeout, unit);
    }
  }
}
