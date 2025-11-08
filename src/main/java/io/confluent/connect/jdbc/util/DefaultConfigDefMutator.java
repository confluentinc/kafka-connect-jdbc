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
 * Default implementation of ConfigDefMutator that performs no mutations.
 * This is the default behavior where the ConfigDef is returned as-is.
 */
public class DefaultConfigDefMutator implements ConfigDefMutator {

  @Override
  public ConfigDef mutate(ConfigDef configDef) {
    // No-op: return the ConfigDef unchanged
    return configDef;
  }
}

