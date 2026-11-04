/*
 * Copyright 2026 Confluent Inc.
 *
 * Licensed under the Confluent Community License (the "License"); you may not use
 * this file except in compliance with the License.  You may obtain a copy of the
 * License at
 *
 * http://www.confluent.io/confluent-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package io.confluent.connect.jdbc.sink;

import java.util.function.LongSupplier;

final class WriteFence {
  private static final WriteFence DISABLED = new WriteFence(null, 0, 0, false);

  private final LongSupplier nanoClock;
  private final long startNanos;
  private final long budgetNanos;
  private final boolean enabled;

  private WriteFence(
      LongSupplier nanoClock,
      long startNanos,
      long budgetNanos,
      boolean enabled
  ) {
    this.nanoClock = nanoClock;
    this.startNanos = startNanos;
    this.budgetNanos = budgetNanos;
    this.enabled = enabled;
  }

  static WriteFence disabled() {
    return DISABLED;
  }

  static WriteFence start(LongSupplier nanoClock, long budgetNanos) {
    if (budgetNanos <= 0) {
      return disabled();
    }
    return new WriteFence(nanoClock, nanoClock.getAsLong(), budgetNanos, true);
  }

  boolean enabled() {
    return enabled;
  }

  void check(String boundary) {
    if (!enabled) {
      return;
    }
    long elapsedNanos = nanoClock.getAsLong() - startNanos;
    if (elapsedNanos >= budgetNanos) {
      throw new WriteFenceTimeoutException(boundary, elapsedNanos, budgetNanos);
    }
  }
}
