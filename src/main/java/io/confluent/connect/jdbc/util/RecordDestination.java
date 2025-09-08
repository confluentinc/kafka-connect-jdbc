/*
 * Copyright [2024 - 2024] Confluent Inc.
 */

package io.confluent.connect.jdbc.util;

import java.time.Duration;
import java.util.function.BooleanSupplier;

import org.apache.kafka.connect.errors.ConnectException;

/**
 * A destination for sending SourceRecord
 * @param <RecordT> the type of record
 */
public interface RecordDestination<RecordT> {

  /**
   * An exception that signals the {@link RecordDestination} has been stopped.
   */
  class DestinationClosedException extends ConnectException {

    public DestinationClosedException() {
      super("SourceRecordQueue has been stopped");
    }
  }

  /**
   * Determine if this destination is currently running.
   *
   * @return true if it is running, or false otherwise
   */
  boolean isRunning();

  /**
   * Send a record to this destination if there is room for the record, or throwing
   * {@link IllegalStateException} if there is no room or {@link DestinationClosedException}
   * if the destination has been closed and will not accept any additional records.
   *
   * @param record the record to send; may not be null
   * @throws DestinationClosedException if the element cannot be sent at this
   *         time because the destination has been closed and will not accept additional records
   * @throws IllegalStateException if the element cannot be sent at this
   *         time due to capacity restrictions
   * @throws ClassCastException if the class of the specified element
   *         prevents it from being sent to this destination
   * @throws NullPointerException if the specified element is null
   * @throws IllegalArgumentException if some property of the specified
   *         element prevents it from being sent to this destination
   * @see #send(Object, Duration)
   */
  void send(RecordT record);

  /**
   * Send a record to this destination, waiting up to the specified timeout for room in the
   * destination to appear.
   *
   * @param record the record to send; may not be null
   * @param timeout the maximum amount of time to wait for room to appear in the destination;
   *                may not be null
   * @return true if the record was sent, or false otherwise
   * @throws DestinationClosedException if the element cannot be sent at this
   *         time because the destination has been closed and will not accept additional records
   * @throws InterruptedException if interrupted while waiting
   * @throws ClassCastException if the class of the specified element
   *         prevents it from being sent to this destination
   * @throws NullPointerException if the specified element is null
   * @throws IllegalArgumentException if some property of the specified
   *         element prevents it from being sent to this destination
   * @see #send(Object)
   */
  boolean send(RecordT record, Duration timeout) throws InterruptedException;

  /**
   * Notify the destination of an unrecoverable failure.
   *
   * @param failure the exception; may not be null
   */
  void failWith(Throwable failure);

  /**
   * Obtain a new {@link RecordDestination} instance that sends all records and failures to this
   * destination, but on which the {@link #isRunning()} method will return true as long as this
   * destination is running and while the specified function returns <em>false</em>.
   *
   * @param isRunning function that defines an additional condition used to determine whether the
   *                  resulting {@link RecordDestination} is still running; may not be null
   * @return the new record destination; never null
   */
  default RecordDestination<RecordT> withCancellationCondition(BooleanSupplier isRunning) {
    return withAdditionalRunningCondition(() -> !isRunning.getAsBoolean());
  }

  /**
   * Obtain a new {@link RecordDestination} instance that sends all records and failures to this
   * destination, but on which the {@link #isRunning()} method will return true as long as this
   * destination is running and while the specified function also returns <em>true</em>.
   *
   * @param isRunning function that defines an additional condition used to determine whether the
   *                  resulting {@link RecordDestination} is still running; may not be null
   * @return the new record destination; never null
   */
  default RecordDestination<RecordT> withAdditionalRunningCondition(BooleanSupplier isRunning) {
    final RecordDestination<RecordT> original = this;
    return new RecordDestination<RecordT>() {
      @Override
      public boolean isRunning() {
        return original.isRunning() && isRunning.getAsBoolean();
      }

      @Override
      public void send(RecordT record) {
        original.send(record);
      }

      @Override
      public boolean send(
          RecordT record,
          Duration timeout
      ) throws InterruptedException {
        return original.send(record, timeout);
      }

      @Override
      public void failWith(Throwable failure) {
        original.failWith(failure);
      }
    };
  }
}