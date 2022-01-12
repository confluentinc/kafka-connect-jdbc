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

package io.confluent.connect.jdbc.util;

import org.junit.Test;

import java.sql.Timestamp;
import java.time.Instant;

import static org.junit.Assert.assertEquals;

public class DateTimeUtilsTest {

  @Test
  public void testTimestampToNanos() {
    Timestamp timestamp = Timestamp.from(Instant.now());
    timestamp.setNanos(141362049);
    long nanos = DateTimeUtils.toEpochNanos(timestamp);
    assertEquals(timestamp, DateTimeUtils.toTimestamp(nanos));
  }

  @Test
  public void testTimestampToString() {
    Timestamp timestamp = Timestamp.from(Instant.now());
    timestamp.setNanos(141362049);
    String nanos = String.valueOf(DateTimeUtils.toEpochNanos(timestamp));
    assertEquals(timestamp, DateTimeUtils.toTimestamp(nanos));
  }

  @Test
  public void testTimestampToIsoDateTime() {
    Timestamp timestamp = Timestamp.from(Instant.now());
    timestamp.setNanos(141362049);
    String isoDateTime = DateTimeUtils.toIsoDateTimeString(timestamp);
    assertEquals(timestamp, DateTimeUtils.toTimestampFromIsoDateTime(isoDateTime));
  }
}
