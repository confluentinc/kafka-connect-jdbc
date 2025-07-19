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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class EnvVarsJdbcCredentialsProvider implements JdbcCredentialsProvider {

  private static final Logger log = LoggerFactory.getLogger(EnvVarsJdbcCredentialsProvider.class);

  private static final String ENV_VAR_DB_USERNAME = "CONNECT_JDBC_CONNECTION_USER";
  private static final String ENV_VAR_DB_PASSWORD = "CONNECT_JDBC_CONNECTION_PASSWORD";

  @Override
  public JdbcCredentials getJdbcCredentials() {
    String username = System.getenv(ENV_VAR_DB_USERNAME);
    String password = System.getenv(ENV_VAR_DB_PASSWORD);
    if (username == null || password == null) {
      throw new RuntimeException("Environment variables " + ENV_VAR_DB_USERNAME + " and "
          + ENV_VAR_DB_PASSWORD + " must be set when using EnvVarsJdbcCredentialsProvider");
    }
    log.info("Using JDBC credentials from environment variables " + ENV_VAR_DB_USERNAME + " and "
        + ENV_VAR_DB_PASSWORD);
    return new BasicJdbcCredentials(username, password);
  }

}
