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

import java.util.Map;
import org.apache.kafka.common.Configurable;

public class DefaultJdbcCredentialsProvider implements JdbcCredentialsProvider, Configurable {

  private static final String DB_USERNAME_CONFIG = "connection.user";
  private static final String DB_PASSWORD_CONFIG = "connection.password";
  String username;
  String password;

  @Override
  public JdbcCredentials getJdbcCredentials() {
    return new BasicJdbcCredentials(username, password);
  }

  @Override
  public void configure(Map<String, ?> map) {
    username = (String) map.get(DB_USERNAME_CONFIG);
    password = (String) map.get(DB_PASSWORD_CONFIG);
  }
}
