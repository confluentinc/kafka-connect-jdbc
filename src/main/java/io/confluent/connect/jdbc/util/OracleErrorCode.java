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

public final class OracleErrorCode {

  private OracleErrorCode() {
  }

  public static String tag(int errorCode) {
    switch (errorCode) {
      case 1:
      case 1400:
      case 1407:
      case 2290:
      case 2291:
      case 2292:
      case 12899:
        return "ORA-" + String.format("%05d", errorCode);
      default:
        return errorCode >= 20000 && errorCode <= 20999 ? "application_error" : null;
    }
  }
}
