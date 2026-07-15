/*
 * Copyright 2026 Confluent Inc.
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

public final class SafeSqlException extends SQLException {

  private static final long serialVersionUID = 1L;

  private final SafeSqlDiagnostic diagnostic;

  SafeSqlException(SafeSqlDiagnostic diagnostic) {
    super(diagnostic.message, diagnostic.canonicalSqlState, 0);
    this.diagnostic = diagnostic;
  }

  @Override
  public synchronized Throwable initCause(Throwable cause) {
    return this;
  }

  @Override
  public synchronized Throwable fillInStackTrace() {
    return this;
  }

  @Override
  public void setStackTrace(StackTraceElement[] stackTrace) {
  }

  @Override
  public void setNextException(SQLException exception) {
  }

  public SafeSqlDiagnostic diagnostic() {
    return diagnostic;
  }
}
