/*
 * Copyright [2024 - 2024] Confluent Inc.
 */

package io.confluent.connect.jdbc.util;

import org.apache.kafka.connect.source.SourceRecord;
import org.junit.After;
import org.junit.Test;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class RecordQueueTest {

  private static final Duration LONG_LINGER = Duration.ofSeconds(5);

  private final ScheduledExecutorService producer = Executors.newSingleThreadScheduledExecutor();
  private RecordQueue<SourceRecord> queue;

  @After
  public void tearDown() {
    producer.shutdownNow();
    if (queue != null) {
      queue.stop();
    }
  }

  @Test
  public void pollReturnsAsSoonAsTheFirstRecordArrives() throws Exception {
    queue = RecordQueue.builder().maxBatchSize(1000).maxPollLinger(LONG_LINGER).build();
    producer.schedule(() -> queue.send(record(1)), 200, TimeUnit.MILLISECONDS);

    long start = System.nanoTime();
    List<SourceRecord> batch = queue.poll();
    long elapsedMs = elapsedMs(start);

    // A low-volume table never fills a batch, so the record must not be held for the linger.
    assertEquals(1, batch.size());
    assertTrue("poll() held the record for " + elapsedMs + " ms", elapsedMs < 2000);
  }

  @Test
  public void pollReturnsTheRecordsQueuedBehindTheFirstOne() throws Exception {
    // Three records become available together while poll() is waiting on an empty queue.
    AtomicBoolean released = new AtomicBoolean();
    ArrayBlockingQueue<SourceRecord> backing = new ArrayBlockingQueue<SourceRecord>(10) {
      @Override
      public SourceRecord poll(long timeout, TimeUnit unit) throws InterruptedException {
        if (released.compareAndSet(false, true)) {
          add(record(1));
          add(record(2));
          add(record(3));
        }
        return super.poll(timeout, unit);
      }
    };
    queue = RecordQueue.builder().maxBatchSize(1000).maxPollLinger(LONG_LINGER).queue(backing)
        .build();

    long start = System.nanoTime();
    List<SourceRecord> batch = queue.poll();
    long elapsedMs = elapsedMs(start);

    assertEquals(3, batch.size());
    assertTrue("poll() waited " + elapsedMs + " ms for more records", elapsedMs < 2000);
  }

  @Test
  public void pollWithZeroLingerBlocksBrieflyOnAnEmptyQueue() throws Exception {
    queue = RecordQueue.builder().maxPollLinger(Duration.ZERO).build();

    long start = System.nanoTime();
    List<SourceRecord> batch = queue.poll();
    long elapsedMs = elapsedMs(start);

    // Returning at once would make the worker's poll loop spin at full CPU while idle.
    assertTrue(batch.isEmpty());
    assertTrue(
        "poll() returned after " + elapsedMs + " ms",
        elapsedMs >= RecordQueue.MIN_IDLE_POLL_WAIT.toMillis()
    );
  }

  @Test
  public void pollWithZeroLingerReturnsARecordThatArrivesWhileWaiting() throws Exception {
    queue = RecordQueue.builder().maxPollLinger(Duration.ZERO).build();
    producer.schedule(() -> queue.send(record(1)), 20, TimeUnit.MILLISECONDS);

    List<SourceRecord> batch = queue.poll();

    assertEquals(1, batch.size());
  }

  @Test
  public void pollReturnsAnEmptyBatchOnceTheLingerExpires() throws Exception {
    Duration linger = Duration.ofMillis(300);
    queue = RecordQueue.builder().maxPollLinger(linger).build();

    long start = System.nanoTime();
    List<SourceRecord> batch = queue.poll();
    long elapsedMs = elapsedMs(start);

    assertTrue(batch.isEmpty());
    assertTrue("poll() returned after " + elapsedMs + " ms", elapsedMs >= linger.toMillis());
    assertTrue("poll() returned after " + elapsedMs + " ms", elapsedMs < 2000);
  }

  private static SourceRecord record(int id) {
    return new SourceRecord(null, null, "topic", 0, null, id);
  }

  private static long elapsedMs(long startNanos) {
    return TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startNanos);
  }
}
