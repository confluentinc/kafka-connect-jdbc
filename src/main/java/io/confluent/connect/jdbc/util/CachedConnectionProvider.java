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

import org.apache.kafka.connect.errors.ConnectException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.SQLException;
import java.time.Clock;

public class CachedConnectionProvider implements ConnectionProvider {

  private static final Logger log = LoggerFactory.getLogger(CachedConnectionProvider.class);

  private static final int VALIDITY_CHECK_TIMEOUT_S = 5;

  private final long cacheTTL;
  private long connectionStartTs;
  private final Clock clock;
  private final ConnectionProvider provider;
  private final int maxConnectionAttempts;
  private final long connectionRetryBackoff;

  private int count = 0;
  private Connection connection;

  public CachedConnectionProvider(
          ConnectionProvider provider,
          int maxConnectionAttempts,
          long connectionRetryBackoff,
          long cacheTTL
  ) {
    this.provider = provider;
    this.maxConnectionAttempts = maxConnectionAttempts;
    this.connectionRetryBackoff = connectionRetryBackoff;
    this.cacheTTL = cacheTTL;
    this.clock = Clock.systemUTC();
  }

  public CachedConnectionProvider(
          ConnectionProvider provider,
          int maxConnectionAttempts,
          long connectionRetryBackoff,
          long cacheTTL,
          Clock clock,
          Connection connection,
          Long connectionStartTs
  ) {
    this.provider = provider;
    this.maxConnectionAttempts = maxConnectionAttempts;
    this.connectionRetryBackoff = connectionRetryBackoff;
    this.cacheTTL = cacheTTL;
    this.clock = clock;
    this.connection = connection;
    this.connectionStartTs = connectionStartTs;
  }

  public boolean connectionIsExpired() {
    if (cacheTTL == -1) {
      return false;
    }
    long now = clock.millis();
    long connectionLifetime = now - connectionStartTs;
    return cacheTTL < connectionLifetime;
  }

  @Override
  public synchronized Connection getConnection() {
    try {
      if (connection == null) {
        newConnection();
      } else if (connectionIsExpired()) {
        log.info("The database connection is expired. Reconnecting...");
        close();
        newConnection();
      } else if (!isConnectionValid(connection, VALIDITY_CHECK_TIMEOUT_S)) {
        log.info("The database connection is invalid. Reconnecting...");
        close();
        newConnection();
      }
    } catch (SQLException sqle) {
      throw new ConnectException(sqle);
    }
    return connection;
  }

  @Override
  public boolean isConnectionValid(Connection connection, int timeout) {
    try {
      return provider.isConnectionValid(connection, timeout);
    } catch (SQLException sqle) {
      log.debug("Unable to check if the underlying connection is valid", sqle);
      return false;
    }
  }

  private void newConnection() throws SQLException {
    int attempts = 0;
    while (attempts < maxConnectionAttempts) {
      try {
        ++count;
        log.info("Attempting to open connection #{} to {}", count, provider);
        connection = provider.getConnection();
        connectionStartTs = clock.millis();
        onConnect(connection);
        return;
      } catch (SQLException sqle) {
        attempts++;
        if (attempts < maxConnectionAttempts) {
          log.info("Unable to connect to database on attempt {}/{}. Will retry in {} ms.", attempts,
                   maxConnectionAttempts, connectionRetryBackoff, sqle
          );
          try {
            Thread.sleep(connectionRetryBackoff);
          } catch (InterruptedException e) {
            // this is ok because just woke up early
          }
        } else {
          throw sqle;
        }
      }
    }
  }

  @Override
  public synchronized void close() {
    if (connection != null) {
      try {
        log.info("Closing connection #{} to {}", count, provider);
        connection.close();
      } catch (SQLException sqle) {
        log.warn("Ignoring error closing connection", sqle);
      } finally {
        connection = null;
        provider.close();
      }
    }
  }

  @Override
  public String identifier() {
    return provider.identifier();
  }

  protected void onConnect(Connection connection) throws SQLException {
  }

}
