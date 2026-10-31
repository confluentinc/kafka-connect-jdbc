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
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.junit.MockitoJUnitRunner;
import org.mockito.stubbing.Answer;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static org.junit.Assert.assertNotNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class CachedConnectionProviderTest {

  @Mock
  private ConnectionProvider provider;

  @Test
  public void retryTillFailure() throws SQLException {
    int retries = 15;
    ConnectionProvider connectionProvider = new CachedConnectionProvider(provider, retries, 100L);
    when(provider.getConnection()).thenThrow(new SQLException("test"));

    try {
      connectionProvider.getConnection();
    }catch(ConnectException ce){
      assertNotNull(ce);
    }

    verify(provider, times(retries)).getConnection();
  }


  @Test
  public void retryTillConnect() throws SQLException {
    Connection connection = mock(Connection.class);
    int retries = 15;

    ConnectionProvider connectionProvider = new CachedConnectionProvider(provider, retries, 100L);
    when(provider.getConnection()).thenAnswer(new Answer<Connection>() {
      private int callCount = 0;

      @Override
      public Connection answer(InvocationOnMock invocation) throws Throwable {
        callCount++;
        if (callCount < retries) {
          throw new SQLException("test");
        }
        return connection;
      }
    });

    assertNotNull(connectionProvider.getConnection());

    verify(provider, times(retries)).getConnection();
  }

  @Test
  public void retryTillClose() throws SQLException {
    final CountDownLatch latch = new CountDownLatch(1);
    CachedConnectionProvider connectionProvider = new CachedConnectionProvider(
        new ConnectionProvider() {
          @Override
          public Connection getConnection() throws SQLException {
            latch.countDown();
            throw new SQLException("test");
          }

          @Override
          public boolean isConnectionValid(Connection connection, int timeout) throws SQLException {
            return false;
          }

          @Override
          public void close() {
          }
        }, Integer.MAX_VALUE, 100L);

    ExecutorService executorService = Executors.newSingleThreadExecutor();
    executorService.execute(() -> {
      try {
        latch.await();
        connectionProvider.close(true);
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
    });

    try {
      connectionProvider.getConnection();
    } catch (ConnectException ce) {
      assertNotNull(ce);
    }
  }

}
