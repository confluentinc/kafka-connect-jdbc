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

import org.apache.kafka.common.config.ConfigDef;

/**
 * Interface for mutating ConfigDef objects to customize configuration definitions.
 * Implementations can modify configuration types, validators, or other properties
 * to adapt the connector's configuration schema for specific requirements.
 * 
 * <p>For example, an implementation might change the type of a configuration
 * from STRING to PASSWORD for enhanced security.</p>
 */
public interface ConfigDefMutator {

  /**
   * Mutates the provided ConfigDef and returns the mutated version.
   * Implementations should modify the ConfigDef as needed for their specific use case.
   *
   * @param configDef the original ConfigDef to mutate
   * @return the mutated ConfigDef
   */
  ConfigDef mutate(ConfigDef configDef);
}

