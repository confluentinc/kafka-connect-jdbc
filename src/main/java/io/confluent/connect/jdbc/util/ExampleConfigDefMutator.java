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

import io.confluent.connect.jdbc.source.JdbcSourceConnectorConfig;
import org.apache.kafka.common.config.ConfigDef;

import java.util.Map;

/**
 * Example implementation of ConfigDefMutator that demonstrates how to mutate
 * configuration definitions. This example changes the QUERY_CONFIG type from
 * STRING to PASSWORD for enhanced security.
 * 
 * <p>To use this mutator, set the following in your connector configuration:</p>
 * <pre>
 * config.def.mutator.class=io.confluent.connect.jdbc.util.ExampleConfigDefMutator
 * </pre>
 * 
 * <p>This is provided as an example. Users can create their own implementations
 * by implementing the ConfigDefMutator interface and packaging it with the connector.</p>
 */
public class ExampleConfigDefMutator implements ConfigDefMutator {

  @Override
  public ConfigDef mutate(ConfigDef configDef) {
    // Create a new ConfigDef to hold the mutated configuration
    ConfigDef newConfigDef = new ConfigDef();
    
    // Copy all existing config keys to the new ConfigDef, modifying the ones we want to change
    Map<String, ConfigDef.ConfigKey> configKeys = configDef.configKeys();
    
    for (Map.Entry<String, ConfigDef.ConfigKey> entry : configKeys.entrySet()) {
      String configName = entry.getKey();
      ConfigDef.ConfigKey configKey = entry.getValue();
      
      // Check if this is the config we want to mutate
      if (JdbcSourceConnectorConfig.QUERY_CONFIG.equals(configName)) {
        // Redefine with PASSWORD type instead of STRING
        newConfigDef.define(
            configName,
            ConfigDef.Type.PASSWORD,  // Changed from STRING to PASSWORD
            configKey.defaultValue,
            configKey.validator,
            configKey.importance,
            configKey.documentation,
            configKey.group,
            configKey.orderInGroup,
            configKey.width,
            configKey.displayName,
            configKey.dependents,
            configKey.recommender
        );
      } else {
        // Copy the config as-is
        newConfigDef.define(
            configName,
            configKey.type,
            configKey.defaultValue,
            configKey.validator,
            configKey.importance,
            configKey.documentation,
            configKey.group,
            configKey.orderInGroup,
            configKey.width,
            configKey.displayName,
            configKey.dependents,
            configKey.recommender
        );
      }
    }
    
    // You can add more mutation logic here for other config keys
    // For example, you could also change other STRING configs to PASSWORD type
    
    return newConfigDef;
  }
}

