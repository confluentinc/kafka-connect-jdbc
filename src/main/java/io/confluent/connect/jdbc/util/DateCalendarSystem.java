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

/**
 * Enumeration of supported date calendar systems for handling DATE and TIMESTAMP columns.
 * 
 * <p>This enum controls how the connector interprets and converts date/time values,
 * particularly for historical dates before the Gregorian calendar adoption
 * (October 15, 1582).</p>
 */
public enum DateCalendarSystem {
  
  /**
   * Legacy calendar system using hybrid Julian/Gregorian calendar.
   * 
   * <p>This system matches the behavior of legacy Java date/time classes
   * (java.util.Date, java.util.Calendar, java.sql.Date, java.sql.Timestamp):</p>
   * <ul>
   *   <li>Dates before October 15, 1582 use the Julian calendar</li>
   *   <li>Dates on or after October 15, 1582 use the Gregorian calendar</li>
   *   <li>Maintains backward compatibility with existing data pipelines</li>
   * </ul>
   * 
   * <p>This is the default setting to ensure backward compatibility.</p>
   */
  LEGACY,
  
  /**
   * Proleptic Gregorian calendar system.
   * 
   * <p>This system matches the behavior of modern Java time classes
   * (java.time.LocalDateTime, java.time.ZonedDateTime, etc.):</p>
   * <ul>
   *   <li>Uses Gregorian calendar rules for all dates (extends backward)</li>
   *   <li>Uses ISO-8601 year numbering (year 0 = 1 BC, year -1 = 2 BC, etc.)</li>
   *   <li>Provides consistent date calculations across all historical periods</li>
   * </ul>
   * 
   * <p>Choose this setting when working with systems that use java.time APIs
   * or when consistency across historical dates is more important than
   * backward compatibility.</p>
   */
  PROLEPTIC_GREGORIAN;
  
  /**
   * Parses a configuration value string to the corresponding enum value.
   * 
   * @param configValue the configuration value string (case-insensitive)
   * @return the corresponding DateCalendarSystem enum value
   * @throws IllegalArgumentException if the config value is not recognized
   */
  public static DateCalendarSystem fromConfigValue(String configValue) {
    if (configValue == null) {
      return LEGACY; // Default fallback
    }
    
    String normalized = configValue.trim();
    for (DateCalendarSystem system : values()) {
      if (system.toString().equals(normalized)) {
        return system;
      }
    }
    
    throw new IllegalArgumentException(
        "Invalid date calendar system: '" + configValue + "'. "
        + "Valid values are: 'LEGACY', 'PROLEPTIC_GREGORIAN'"
    );
  }
  
  /**
   * Checks if this calendar system uses modern (proleptic Gregorian) semantics.
   * 
   * @return true if this is PROLEPTIC_GREGORIAN, false if LEGACY
   */
  public boolean isModern() {
    return this == PROLEPTIC_GREGORIAN;
  }
  
  /**
   * Gets all valid configuration values as an array.
   * 
   * @return array of valid configuration value strings
   */
  public static String[] getValidConfigValues() {
    DateCalendarSystem[] systems = values();
    String[] configValues = new String[systems.length];
    for (int i = 0; i < systems.length; i++) {
      configValues[i] = systems[i].toString();
    }
    return configValues;
  }
}
