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
import org.apache.kafka.connect.sink.SinkRecord;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.LongSupplier;

final class ReceiptWriteFence {
  private static final ReceiptWriteFence DISABLED =
      new ReceiptWriteFence(false, 0L, false, System::nanoTime);

  private final boolean enabled;
  private final long timeoutNanos;
  private final boolean resetOnAssignment;
  private final LongSupplier nanoClock;
  private final Map<RecordIdentity, Long> firstSeenNanosByRecord = new HashMap<>();

  private JdbcWriteFenceException terminalFence;

  ReceiptWriteFence(JdbcSinkConfig config, LongSupplier nanoClock) {
    this(
        config.writeFenceTimeoutNanos > 0,
        config.writeFenceTimeoutNanos,
        config.writeFenceReceiptResetOnAssignment,
        nanoClock
    );
  }

  private ReceiptWriteFence(
      boolean enabled,
      long timeoutNanos,
      boolean resetOnAssignment,
      LongSupplier nanoClock
  ) {
    this.enabled = enabled;
    this.timeoutNanos = timeoutNanos;
    this.resetOnAssignment = resetOnAssignment;
    this.nanoClock = nanoClock;
  }

  static ReceiptWriteFence disabled() {
    return DISABLED;
  }

  boolean enabled() {
    return enabled;
  }

  void recordPutDelivery(Collection<SinkRecord> records) {
    if (!enabled || records.isEmpty()) {
      return;
    }
    throwIfTerminal();

    long now = nanoClock.getAsLong();
    List<RecordIdentity> delivered = identities(records);
    Set<RecordIdentity> pending = new HashSet<>(delivered);
    firstSeenNanosByRecord.keySet().retainAll(pending);

    for (RecordIdentity identity : delivered) {
      firstSeenNanosByRecord.putIfAbsent(identity, now);
    }
    throwIfExpired(delivered, now);
  }

  void check(Collection<SinkRecord> records) {
    if (!enabled || records.isEmpty()) {
      return;
    }
    throwIfTerminal();
    throwIfExpired(identities(records), nanoClock.getAsLong());
  }

  void complete(Collection<SinkRecord> records) {
    if (!enabled || records.isEmpty() || terminalFence != null) {
      return;
    }
    for (RecordIdentity identity : identities(records)) {
      firstSeenNanosByRecord.remove(identity);
    }
  }

  void open(Collection<TopicPartition> partitions) {
    if (!enabled || !resetOnAssignment || terminalFence != null || partitions.isEmpty()) {
      return;
    }
    firstSeenNanosByRecord.keySet().removeIf(identity -> partitions.contains(identity.partition()));
  }

  private void throwIfTerminal() {
    if (terminalFence != null) {
      throw terminalFence;
    }
  }

  private void throwIfExpired(List<RecordIdentity> identities, long now) {
    RecordIdentity oldestIdentity = null;
    long oldestElapsedNanos = 0;
    // Compare elapsed durations, not absolute nanoTime values, which can wrap.
    for (RecordIdentity identity : identities) {
      Long firstSeen = firstSeenNanosByRecord.get(identity);
      if (firstSeen == null) {
        continue;
      }
      long elapsedNanos = now - firstSeen;
      if (oldestIdentity == null || elapsedNanos > oldestElapsedNanos) {
        oldestElapsedNanos = elapsedNanos;
        oldestIdentity = identity;
      }
    }
    if (oldestIdentity == null) {
      return;
    }
    if (oldestElapsedNanos >= timeoutNanos) {
      terminalFence = new JdbcWriteFenceException(String.format(
          "JDBC write fence expired for source record %s after %d ns (budget %d ns)",
          oldestIdentity,
          oldestElapsedNanos,
          timeoutNanos
      ));
      throw terminalFence;
    }
  }

  private List<RecordIdentity> identities(Collection<SinkRecord> records) {
    List<RecordIdentity> identities = new ArrayList<>(records.size());
    for (SinkRecord record : records) {
      identities.add(RecordIdentity.from(record));
    }
    return identities;
  }

  private static final class RecordIdentity {
    private final String topic;
    private final int partition;
    private final long offset;

    private RecordIdentity(String topic, int partition, long offset) {
      this.topic = topic;
      this.partition = partition;
      this.offset = offset;
    }

    private static RecordIdentity from(SinkRecord record) {
      RecordIdentity publicOriginal = publicOriginalIdentity(record);
      if (publicOriginal != null) {
        return publicOriginal;
      }

      if (isInternalSinkRecord(record)) {
        return internalOriginalIdentity(record);
      }

      if (record.topic() == null || record.kafkaPartition() == null) {
        throw new ConnectException(
            "Cannot identify JDBC write fence source coordinates from a SinkRecord without "
                + "topic and partition"
        );
      }
      return new RecordIdentity(record.topic(), record.kafkaPartition(), record.kafkaOffset());
    }

    private static RecordIdentity publicOriginalIdentity(SinkRecord record) {
      Method topicMethod;
      Method partitionMethod;
      Method offsetMethod;
      try {
        topicMethod = record.getClass().getMethod("originalTopic");
        partitionMethod = record.getClass().getMethod("originalKafkaPartition");
        offsetMethod = record.getClass().getMethod("originalKafkaOffset");
      } catch (NoSuchMethodException e) {
        return null;
      }
      try {
        Object topic = topicMethod.invoke(record);
        Object partition = partitionMethod.invoke(record);
        Object offset = offsetMethod.invoke(record);
        if (!(topic instanceof String)
            || !(partition instanceof Integer)
            || !(offset instanceof Long)) {
          throw new ConnectException(
              "Cannot identify JDBC write fence source coordinates from incompatible original "
                  + "SinkRecord accessor return types"
          );
        }
        return new RecordIdentity((String) topic, (Integer) partition, (Long) offset);
      } catch (IllegalAccessException | InvocationTargetException e) {
        throw new ConnectException(
            "Cannot identify JDBC write fence source coordinates from original SinkRecord "
                + "accessors",
            e
        );
      }
    }

    private static RecordIdentity internalOriginalIdentity(SinkRecord record) {
      Object original;
      try {
        original = record.getClass().getMethod("originalRecord").invoke(record);
      } catch (NoSuchMethodException | IllegalAccessException | InvocationTargetException e) {
        throw new ConnectException(
            "Cannot identify JDBC write fence source coordinates from an InternalSinkRecord",
            e
        );
      }
      if (!(original instanceof ConsumerRecord)) {
        throw new ConnectException(
            "Cannot identify JDBC write fence source coordinates from an InternalSinkRecord "
                + "with no original ConsumerRecord"
        );
      }
      ConsumerRecord<?, ?> originalRecord = (ConsumerRecord<?, ?>) original;
      return new RecordIdentity(
          originalRecord.topic(),
          originalRecord.partition(),
          originalRecord.offset()
      );
    }

    private static boolean isInternalSinkRecord(SinkRecord record) {
      Class<?> current = record.getClass();
      while (current != null) {
        if ("org.apache.kafka.connect.runtime.InternalSinkRecord".equals(current.getName())) {
          return true;
        }
        current = current.getSuperclass();
      }
      return false;
    }

    private TopicPartition partition() {
      return new TopicPartition(topic, partition);
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (!(o instanceof RecordIdentity)) {
        return false;
      }
      RecordIdentity that = (RecordIdentity) o;
      return partition == that.partition
          && offset == that.offset
          && topic.equals(that.topic);
    }

    @Override
    public int hashCode() {
      return Objects.hash(topic, partition, offset);
    }

    @Override
    public String toString() {
      return topic + "-" + partition + "-" + offset;
    }
  }
}
