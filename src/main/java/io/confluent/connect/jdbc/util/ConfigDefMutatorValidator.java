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
import org.apache.kafka.common.config.ConfigException;

/**
 * Validator for ConfigDefMutator class configuration.
 * Ensures that the provided class implements the ConfigDefMutator interface.
 */
public class ConfigDefMutatorValidator implements ConfigDef.Validator {

  @Override
  public void ensureValid(String name, Object provider) {
    if (provider != null && provider instanceof Class
        && !ConfigDefMutator.class.isAssignableFrom((Class<?>) provider)) {
      throw new ConfigException(
          name,
          provider,
          "Class must implement ConfigDefMutator interface"
      );
    }
  }

  @Override
  public String toString() {
    return "Any class implementing " + ConfigDefMutator.class;
  }
}

