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
 * WARRANTIES OF ANY KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations under the License.
 */

package io.confluent.connect.jdbc.sink;

import org.apache.log4j.AppenderSkeleton;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.spi.LoggingEvent;
import org.junit.Assert;

import java.util.ArrayList;
import java.util.List;

final class LogCapture extends AppenderSkeleton {
  private final Logger logger;
  private final Level previousLevel;
  private final List<LoggingEvent> events = new ArrayList<>();

  LogCapture(Class<?> loggedClass) {
    logger = Logger.getLogger(loggedClass);
    previousLevel = logger.getLevel();
    logger.setLevel(Level.DEBUG);
    logger.addAppender(this);
  }

  String output(Level level, String messageFragment) {
    StringBuilder output = new StringBuilder();
    for (LoggingEvent event : events) {
      String message = event.getRenderedMessage();
      if (!level.equals(event.getLevel()) || !message.contains(messageFragment)) {
        continue;
      }
      output.append(message).append(System.lineSeparator());
      String[] throwableLines = event.getThrowableStrRep();
      if (throwableLines != null) {
        for (String line : throwableLines) {
          output.append(line).append(System.lineSeparator());
        }
      }
    }
    return output.toString();
  }

  static void assertSensitiveValue(
      String output,
      String canary,
      boolean sanitizeSensitiveLogs
  ) {
    Assert.assertFalse("Expected a captured log event", output.isEmpty());
    if (sanitizeSensitiveLogs) {
      Assert.assertTrue(
          "Redaction marker missing from log: " + output,
          output.contains("<redacted>")
      );
      Assert.assertFalse("Raw value leaked into log: " + output, output.contains(canary));
    } else {
      Assert.assertTrue("Raw value missing from log: " + output, output.contains(canary));
      Assert.assertFalse(
          "Unexpected redaction marker in log: " + output,
          output.contains("<redacted>")
      );
    }
  }

  @Override
  protected void append(LoggingEvent event) {
    events.add(event);
  }

  @Override
  public void close() {
    logger.removeAppender(this);
    logger.setLevel(previousLevel);
    closed = true;
  }

  @Override
  public boolean requiresLayout() {
    return false;
  }
}
