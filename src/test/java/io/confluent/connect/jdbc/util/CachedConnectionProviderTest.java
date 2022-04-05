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
import org.easymock.EasyMock;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.powermock.api.easymock.PowerMock;
import org.powermock.api.easymock.annotation.Mock;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.sql.Connection;
import java.sql.SQLException;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneId;

import static org.junit.Assert.*;

@RunWith(PowerMockRunner.class)
@PrepareForTest({CachedConnectionProviderTest.class})
@PowerMockIgnore("javax.management.*")
public class CachedConnectionProviderTest {

  @Mock
  private ConnectionProvider provider;

  private ConnectionProvider mockProvider;
  private Connection mockConnection;
  private Clock mockClock;
  private int maxConnectionAttempts;
  private long connectionRetryBackOff;

  @Before
  public void setUp() {
    mockProvider = Mockito.mock(ConnectionProvider.class);
    mockConnection = Mockito.mock(Connection.class);
    mockClock = Clock.fixed(Instant.ofEpochMilli(10000), ZoneId.systemDefault());
    maxConnectionAttempts = 15;
    connectionRetryBackOff = 100L;
  }

  @Test
  public void retryTillFailure() throws SQLException {
    ConnectionProvider connectionProvider =
            new CachedConnectionProvider(provider, maxConnectionAttempts, connectionRetryBackOff, -1);
    EasyMock.expect(provider.getConnection()).andThrow(new SQLException("test")).times(maxConnectionAttempts);
    PowerMock.replayAll();

    try {
      connectionProvider.getConnection();
    }catch(ConnectException ce){
      assertNotNull(ce);
    }
    PowerMock.verifyAll();
  }


  @Test
  public void retryTillConnect() throws SQLException {
    Connection connection = EasyMock.createMock(Connection.class);
    ConnectionProvider connectionProvider =
            new CachedConnectionProvider(provider, maxConnectionAttempts, connectionRetryBackOff, -1);
    EasyMock.expect(provider.getConnection())
            .andThrow(new SQLException("test"))
            .times(maxConnectionAttempts-1)
            .andReturn(connection);
    PowerMock.replayAll();

    assertNotNull(connectionProvider.getConnection());

    PowerMock.verifyAll();
  }

  @Test
  public void testConnectionIsExpiredReturnsFalseWhenConnectionIsInfinite() {
    CachedConnectionProvider connectionProvider = mockCachedConnectionProvider(-1, -1);

    boolean expired = connectionProvider.connectionIsExpired();

    assertFalse(expired);
  }

  @Test
  public void testConnectionIsExpiredReturnsTrueWhenConnectionIsOlderThanTTL() {
    CachedConnectionProvider connectionProvider = mockCachedConnectionProvider(10L, mockClock.millis() - 11);

    boolean expired = connectionProvider.connectionIsExpired();

    assertTrue(expired);
  }

  @Test
  public void testConnectionIsExpiredReturnsFalseWhenConnectionIsYoungerThanTTL() {
    CachedConnectionProvider connectionProvider = mockCachedConnectionProvider(10L, mockClock.millis() - 9);

    boolean expired = connectionProvider.connectionIsExpired();

    assertFalse(expired);
  }

  @Test
  public void testGetConnectionCallsCloseWhenConnectionIsExpired() throws SQLException {
    CachedConnectionProvider connProvider = mockCachedConnectionProvider(10L, mockClock.millis() - 11);

    connProvider.getConnection();

    Mockito.verify(mockConnection, Mockito.times(1)).close();
  }

  @Test
  public void testGetConnectionCallsNewConnectionWhenConnectionIsExpired() throws SQLException {
    CachedConnectionProvider connProvider = mockCachedConnectionProvider(10L, mockClock.millis() - 11);

    connProvider.getConnection();

    Mockito.verify(mockProvider, Mockito.times(1)).getConnection();
  }

  @Test
  public void testGetConnectionDoesNotCallCloseWhenConnectionIsValidAndUnexpired() throws SQLException {
    Mockito.stub(mockProvider.isConnectionValid(mockConnection, 5)).toReturn(true);
    CachedConnectionProvider connProvider = mockCachedConnectionProvider(10L, mockClock.millis() - 9);

    connProvider.getConnection();

    Mockito.verify(mockConnection, Mockito.times(0)).close();
  }

  @Test
  public void testGetConnectionDoesNotCallNewConnectionWhenConnectionIsValidAndUnexpired() throws SQLException {
    Mockito.stub(mockProvider.isConnectionValid(mockConnection, 5)).toReturn(true);
    CachedConnectionProvider connProvider = mockCachedConnectionProvider(10L, mockClock.millis() - 9);

    connProvider.getConnection();

    Mockito.verify(mockProvider, Mockito.times(0)).getConnection();
  }

  private CachedConnectionProvider mockCachedConnectionProvider(long ttl, long connectionStartTs)  {
    return new CachedConnectionProvider(
            mockProvider,
            maxConnectionAttempts,
            connectionRetryBackOff,
            ttl,
            mockClock,
            mockConnection,
            connectionStartTs
    );
  }
}
