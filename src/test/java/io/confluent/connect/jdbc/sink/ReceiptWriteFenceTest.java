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

package io.confluent.connect.jdbc.sink;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.runtime.InternalSinkRecord;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.LongSupplier;

import static org.junit.Assert.assertThrows;

public class ReceiptWriteFenceTest {

  @Test
  public void disabledFenceNeverExpiresRecords() {
    MutableClock clock = new MutableClock();
    ReceiptWriteFence fence = new ReceiptWriteFence(config(0, false), clock);
    List<SinkRecord> records = Collections.singletonList(record("topic", 0, 1L));

    fence.recordPutDelivery(records);
    clock.set(Long.MAX_VALUE);
    fence.check(records);
  }

  @Test
  public void expiresAtExactBudgetButNotBefore() {
    MutableClock clock = new MutableClock();
    ReceiptWriteFence fence = new ReceiptWriteFence(config(100, false), clock);
    List<SinkRecord> records = Collections.singletonList(record("topic", 0, 1L));

    fence.recordPutDelivery(records);
    clock.set(ms(100) - 1);
    fence.check(records);
    clock.set(ms(100));

    assertThrows(JdbcWriteFenceException.class, () -> fence.check(records));
  }

  @Test
  public void oldestReceiptIsSelectedAcrossNanoTimeWrap() {
    MutableClock clock = new MutableClock();
    ReceiptWriteFence fence = new ReceiptWriteFence(config(100, false), clock);
    long firstReceipt = Long.MAX_VALUE - ms(50);
    SinkRecord old = record("topic", 0, 1L);
    SinkRecord fresh = record("topic", 1, 1L);
    clock.set(firstReceipt);
    fence.recordPutDelivery(Collections.singletonList(old));
    clock.set(firstReceipt + ms(101));

    assertThrows(
        JdbcWriteFenceException.class,
        () -> fence.recordPutDelivery(Arrays.asList(old, fresh))
    );
  }

  @Test
  public void receiptAtMaximumNanoTimeStillExpires() {
    MutableClock clock = new MutableClock();
    ReceiptWriteFence fence = new ReceiptWriteFence(config(100, false), clock);
    List<SinkRecord> records = Collections.singletonList(record("topic", 0, 1L));
    clock.set(Long.MAX_VALUE);
    fence.recordPutDelivery(records);
    clock.set(Long.MAX_VALUE + ms(100));

    assertThrows(JdbcWriteFenceException.class, () -> fence.check(records));
  }

  @Test
  public void completedRecordStartsFreshInLaterHealthyBatch() {
    MutableClock clock = new MutableClock();
    ReceiptWriteFence fence = new ReceiptWriteFence(config(100, false), clock);
    List<SinkRecord> records = Collections.singletonList(record("topic", 0, 1L));

    fence.recordPutDelivery(records);
    fence.complete(records);
    clock.set(ms(100));

    fence.recordPutDelivery(records);
    fence.check(records);
  }

  @Test
  public void internalSinkRecordUsesOriginalSourceCoordinatesAcrossTopicTransforms() {
    MutableClock clock = new MutableClock();
    ReceiptWriteFence fence = new ReceiptWriteFence(config(100, false), clock);
    SinkRecord firstTransformed = record("destination-a", 99, 900L);
    SinkRecord secondTransformed = record("destination-b", 98, 901L);
    InternalSinkRecord first = internalRecord("source", 3, 12L, firstTransformed);
    InternalSinkRecord second = internalRecord("source", 3, 12L, secondTransformed);

    fence.recordPutDelivery(Collections.singletonList(first));
    clock.set(ms(100));

    assertThrows(
        JdbcWriteFenceException.class,
        () -> fence.recordPutDelivery(Collections.singletonList(second))
    );
  }

  @Test
  public void internalSinkRecordWithoutOriginalRecordFailsClosed() {
    MutableClock clock = new MutableClock();
    ReceiptWriteFence fence = new ReceiptWriteFence(config(100, false), clock);
    SinkRecord transformed = record("destination", 99, 900L);

    assertThrows(
        ConnectException.class,
        () -> fence.recordPutDelivery(Collections.singletonList(
            new InternalSinkRecord(null, transformed)
        ))
    );
  }

  @Test
  public void sinkRecordWithPublicOriginalAccessorsUsesOriginalCoordinates() {
    MutableClock clock = new MutableClock();
    ReceiptWriteFence fence = new ReceiptWriteFence(config(100, false), clock);
    SinkRecord transformed = new SinkRecord("destination", 99, null, null, null, null, 900L);
    ModernOriginalSinkRecord first =
        new ModernOriginalSinkRecord(transformed, "source", 3, 12L);

    fence.recordPutDelivery(Collections.singletonList(first));
    clock.set(ms(100));

    assertThrows(
        JdbcWriteFenceException.class,
        () -> fence.recordPutDelivery(Collections.singletonList(record("source", 3, 12L)))
    );
  }

  @Test
  public void plainSinkRecordUsesItsOwnKafkaCoordinates() {
    MutableClock clock = new MutableClock();
    ReceiptWriteFence fence = new ReceiptWriteFence(config(100, false), clock);

    fence.recordPutDelivery(Collections.singletonList(record("plain", 4, 15L)));
    clock.set(ms(100));

    assertThrows(
        JdbcWriteFenceException.class,
        () -> fence.recordPutDelivery(Collections.singletonList(record("plain", 4, 15L)))
    );
  }

  @Test
  public void resetOnAssignmentOnlyClearsNewlyOpenedSourcePartitions() {
    MutableClock clock = new MutableClock();
    ReceiptWriteFence fence = new ReceiptWriteFence(config(100, true), clock);
    List<SinkRecord> records = Arrays.asList(record("topic", 0, 1L), record("topic", 1, 1L));

    fence.recordPutDelivery(records);
    fence.open(Collections.singletonList(new TopicPartition("topic", 0)));
    clock.set(ms(100));
    fence.check(Collections.singletonList(record("topic", 0, 1L)));

    assertThrows(
        JdbcWriteFenceException.class,
        () -> fence.check(Collections.singletonList(record("topic", 1, 1L)))
    );
  }

  @Test
  public void defaultLifetimePreservesReceiptsAcrossOpen() {
    MutableClock clock = new MutableClock();
    ReceiptWriteFence fence = new ReceiptWriteFence(config(100, false), clock);
    List<SinkRecord> records = Collections.singletonList(record("topic", 0, 1L));

    fence.recordPutDelivery(records);
    fence.open(Collections.singletonList(new TopicPartition("topic", 0)));
    clock.set(ms(100));

    assertThrows(JdbcWriteFenceException.class, () -> fence.check(records));
  }

  @Test
  public void subsetDeliveryPrunesRecordsNoLongerPending() {
    MutableClock clock = new MutableClock();
    ReceiptWriteFence fence = new ReceiptWriteFence(config(100, false), clock);
    SinkRecord retained = record("topic", 0, 1L);
    SinkRecord pruned = record("topic", 1, 1L);

    fence.recordPutDelivery(Arrays.asList(retained, pruned));
    clock.set(ms(50));
    fence.recordPutDelivery(Collections.singletonList(retained));
    fence.complete(Collections.singletonList(retained));
    clock.set(ms(100));

    fence.recordPutDelivery(Collections.singletonList(pruned));
    fence.check(Collections.singletonList(pruned));
  }

  private static InternalSinkRecord internalRecord(
      String originalTopic,
      int originalPartition,
      long originalOffset,
      SinkRecord transformed
  ) {
    ConsumerRecord<byte[], byte[]> original =
        new ConsumerRecord<>(originalTopic, originalPartition, originalOffset, null, null);
    return new InternalSinkRecord(original, transformed);
  }

  private static SinkRecord record(String topic, int partition, long offset) {
    return new SinkRecord(topic, partition, null, null, null, null, offset);
  }

  private static JdbcSinkConfig config(long timeoutMs, boolean resetOnAssignment) {
    Map<String, String> props = new HashMap<>();
    props.put(JdbcSinkConfig.CONNECTION_URL, "jdbc:sqlite:memory");
    props.put(JdbcSinkConfig.INSERT_MODE, "upsert");
    props.put(JdbcSinkConfig.PK_MODE, "kafka");
    props.put(JdbcSinkConfig.WRITE_FENCE_TIMEOUT_MS, String.valueOf(timeoutMs));
    props.put(
        JdbcSinkConfig.WRITE_FENCE_RECEIPT_RESET_ON_ASSIGNMENT,
        String.valueOf(resetOnAssignment)
    );
    return new JdbcSinkConfig(props);
  }

  private static long ms(long millis) {
    return millis * 1_000_000L;
  }

  private static class MutableClock implements LongSupplier {
    private long nanos;

    void set(long nanos) {
      this.nanos = nanos;
    }

    @Override
    public long getAsLong() {
      return nanos;
    }
  }

  public static class ModernOriginalSinkRecord extends SinkRecord {
    private final String originalTopic;
    private final Integer originalKafkaPartition;
    private final Long originalKafkaOffset;

    ModernOriginalSinkRecord(
        SinkRecord transformed,
        String originalTopic,
        Integer originalKafkaPartition,
        Long originalKafkaOffset
    ) {
      super(
          transformed.topic(),
          transformed.kafkaPartition(),
          transformed.keySchema(),
          transformed.key(),
          transformed.valueSchema(),
          transformed.value(),
          transformed.kafkaOffset()
      );
      this.originalTopic = originalTopic;
      this.originalKafkaPartition = originalKafkaPartition;
      this.originalKafkaOffset = originalKafkaOffset;
    }

    public String originalTopic() {
      return originalTopic;
    }

    public Integer originalKafkaPartition() {
      return originalKafkaPartition;
    }

    public Long originalKafkaOffset() {
      return originalKafkaOffset;
    }
  }
}
