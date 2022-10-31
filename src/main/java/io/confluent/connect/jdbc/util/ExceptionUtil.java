/*
 * Copyright 2022 Confluent Inc.
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

import java.sql.SQLException;
import java.util.Iterator;
import java.util.NoSuchElementException;

public class ExceptionUtil {

  public static Iterable<Throwable> iterator(Throwable ex) {
    return ex instanceof SQLException
        ? ((SQLException) ex)
        : () -> new Iterator<Throwable>() {
          Throwable firstException = ex;
          Throwable cause = firstException.getCause();

          @Override
          public boolean hasNext() {
            return firstException != null || cause != null;
          }

          @Override
          public Throwable next() {
            Throwable throwable;
            if (firstException != null) {
              throwable = firstException;
              firstException = null;
            } else if (cause != null) {
              throwable = cause;
              cause = cause.getCause();
            } else {
              throw new NoSuchElementException();
            }
            return throwable;
          }

          @Override
          public void remove() {
            throw new UnsupportedOperationException();
          }
        };
  }

}
