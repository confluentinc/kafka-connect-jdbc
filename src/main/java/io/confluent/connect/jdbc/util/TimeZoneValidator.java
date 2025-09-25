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

import java.time.ZoneId;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TimeZoneValidator implements ConfigDef.Validator {

  public static final TimeZoneValidator INSTANCE = new TimeZoneValidator();
  private static final Logger log = LoggerFactory.getLogger(TimeZoneValidator.class);

  @Override
  public void ensureValid(String name, Object value) {
    if (value != null) {
      if (!ZoneId.getAvailableZoneIds().contains(value.toString())) {
        // Try the short IDs for compatibility with legacy values like "EST", "PST", etc.
        try {
          ZoneId.of(value.toString(), ZoneId.SHORT_IDS);
          log.info("Falling back to short IDs for timezone: {}", value);
        } catch (Exception e) {
          throw new ConfigException(name, value,
              "Invalid time zone. See the list of valid time zones at "
                  + "https://docs.oracle.com/javase/8/docs/api/java/time/ZoneId.html#shortIds");
        }
      }
    }
  }

  @Override
  public String toString() {
    return "Any valid JDK time zone";
  }
}
