/**
 * Copyright 2016 Confluent Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 **/

package io.confluent.connect.jdbc.source;

import java.sql.Timestamp;
import java.util.Calendar;
import java.util.HashMap;
import java.util.Map;

public class TimestampIncrementingOffset {
  public static final String INCREMENTING_FIELD = "incrementing";
  public static final String INCREMENTING_SPAN_FIELD = "incrementing_span";
  public static final String TIMESTAMP_FIELD = "timestamp";
  public static final String TIMESTAMP_SPAN_DAYS_FIELD = "timestamp_span_days";
  private static final String TIMESTAMP_NANOS_FIELD = "timestamp_nanos";

  private final Long incrementingOffset;
  private final Timestamp timestampOffset;
  private final Integer maxTimestampDaySpan;
  private final Integer incrementingSpan;


  public TimestampIncrementingOffset copy(Timestamp newTimestampOffset,
          Long newIncrementingOffset) {
    return new TimestampIncrementingOffset(newTimestampOffset, newIncrementingOffset,
            maxTimestampDaySpan, incrementingSpan);
  }


  /**
   * @param timestampOffset    the timestamp offset.
   *                           If null, {@link #getTimestampOffset()} will return
   *                           {@code new Timestamp(0)}.
   * @param incrementingOffset the incrementing offset.
   *                           If null, {@link #getIncrementingOffset()} will return -1.
   */
  public TimestampIncrementingOffset(final Timestamp timestampOffset,
          final Long incrementingOffset) {
    this(timestampOffset, incrementingOffset, null, null);
  }


  /**
   * @param timestampOffset the timestamp offset.
   *                        If null, {@link #getTimestampOffset()} will return
   *                        {@code new Timestamp(0)}.
   * @param incrementingOffset the incrementing offset.
   *                           If null, {@link #getIncrementingOffset()} will return -1.
   * @param maxTimestampDaySpan the maximum number of days to include in a timestamp based query.
   *                            If null, {@link #getTimestampSpanEndOffset()} will return
   *                            {@code new Timestamp(Integer.MAX_VALUE)}
   * @param incrementingSpan the maximum span of increments to cover in as single query.
   *                         If null, {@link #getIncrementingSpanEndOffset()} will return
   *                         {@code Long.MAX_VALUE}.
   */
  public TimestampIncrementingOffset(Timestamp timestampOffset, Long incrementingOffset,
          final Integer maxTimestampDaySpan, final Integer incrementingSpan) {

    if (incrementingSpan == null || incrementingSpan <= 0) {
      this.incrementingSpan = JdbcSourceConnectorConfig.INCREMENTING_SPAN_MAX_DEFAULT;
    } else {
      this.incrementingSpan = incrementingSpan;
    }

    this.timestampOffset = timestampOffset;
    this.incrementingOffset = incrementingOffset;
    this.maxTimestampDaySpan = maxTimestampDaySpan;
  }



  public long getIncrementingOffset() {
    return incrementingOffset == null ? -1 : incrementingOffset;
  }


  /**
   * Get the end of the span or Integer.MAX_VALUE.
   * </p>
   * Long.MAX_VALUE is avoided due to risk of overflow. If more than Integer.MAX_VALUE is
   * needed as a span then it should be configured.
   */
  public long getIncrementingSpanEndOffset() {
    return getIncrementingOffset() + incrementingSpan;
  }

  public Timestamp getTimestampOffset() {
    return timestampOffset == null ? new Timestamp(0) : timestampOffset;
  }


  public Timestamp getTimestampSpanEndOffset() {
    return maxTimestampDaySpan == null
            ? new Timestamp(Long.MAX_VALUE)
            : plusDays(getTimestampOffset(), maxTimestampDaySpan);
  }

  private Timestamp plusDays(Timestamp startingFrom, int days) {
    Calendar cal = Calendar.getInstance();
    cal.setTime(startingFrom);

    cal.add(Calendar.DAY_OF_YEAR,days);

    return new Timestamp(cal.getTime().getTime());
  }

  public Map<String, Object> toMap() {
    Map<String, Object> map = new HashMap<>(3);
    if (incrementingOffset != null) {
      map.put(INCREMENTING_FIELD, incrementingOffset);
    }
    if (timestampOffset != null) {
      map.put(TIMESTAMP_FIELD, timestampOffset.getTime());
      map.put(TIMESTAMP_NANOS_FIELD, (long) timestampOffset.getNanos());
    }
    return map;
  }

  public static TimestampIncrementingOffset fromMap(Map<String, ?> map) {
    if (map == null || map.isEmpty()) {
      return new TimestampIncrementingOffset(null, null, null, null);
    }

    Long incr = (Long) map.get(INCREMENTING_FIELD);
    Integer incrementingSpan = (Integer) map.get(INCREMENTING_SPAN_FIELD);
    Long millis = (Long) map.get(TIMESTAMP_FIELD);
    Integer spanDays = (Integer) map.get(TIMESTAMP_SPAN_DAYS_FIELD);
    Timestamp ts = null;
    if (millis != null) {
      ts = new Timestamp(millis);
      Long nanos = (Long) map.get(TIMESTAMP_NANOS_FIELD);
      if (nanos != null) {
        ts.setNanos(nanos.intValue());
      }
    }
    return new TimestampIncrementingOffset(ts, incr, spanDays, incrementingSpan);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    TimestampIncrementingOffset that = (TimestampIncrementingOffset) o;

    if (incrementingOffset != null
        ? !incrementingOffset.equals(that.incrementingOffset)
        : that.incrementingOffset != null) {
      return false;
    }
    return timestampOffset != null
           ? timestampOffset.equals(that.timestampOffset)
           : that.timestampOffset == null;

  }

  @Override
  public int hashCode() {
    int result = incrementingOffset != null ? incrementingOffset.hashCode() : 0;
    result = 31 * result + (timestampOffset != null ? timestampOffset.hashCode() : 0);
    return result;
  }
}
