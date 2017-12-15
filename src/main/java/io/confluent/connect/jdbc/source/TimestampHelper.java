/**
 * Copyright 2017 Confluent Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 **/

package io.confluent.connect.jdbc.source;

import java.sql.Timestamp;
import org.apache.kafka.connect.data.Struct;

/**
 * TimestampHelper is an interface to construct and operate on timestamp based queries. The
 * implementation handles two types of queries: single timestamp or multiple timestamps.
 */
public interface TimestampHelper {

  /**
   * Builds a SQL query by appending where clause to @link{StringBuilder} with timestamp and
   * incrementing column ID.
   * @param builder SQL statement represented as @link{String}
   * @param quoteString String representation of quotes to use in the SQL statement.
   * @param incrementingColumn name of the incrementing column ID.
   */
  public void addWhereClause(StringBuilder builder, String quoteString, String incrementingColumn);

  /**
   * Appends where clause to @link{StringBuilder} to build a query for timestamp mode.
   * @param builder SQL statement represented as String.
   * @param quoteString String representation of quotes to use in the SQL statement.
   */
  public void addWhereClause(StringBuilder builder, String quoteString);

  /**
   * Extracts timestamp offset from @link{record} .
   * @param record struct that may contain the offset
   * @return the last recorded timestamp offset stored in @link{record}
   */
  public Timestamp extractOffset(Struct record);

}
