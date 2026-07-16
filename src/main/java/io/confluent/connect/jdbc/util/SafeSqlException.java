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

/**
 * A final SQLException whose message, SQLState, and toString come only from an immutable
 * SafeSqlDiagnostic, never the driver's text. It neutralizes the cause, the next-exception edge, and
 * the stack trace, and reports vendor code zero, so logging it or handing it to the framework cannot
 * surface a raw driver message. A fresh instance is minted from the diagnostic before each handoff.
 */
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
