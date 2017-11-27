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

import io.confluent.connect.jdbc.util.JdbcUtils;
import java.sql.Timestamp;
import org.apache.kafka.connect.data.Struct;

/**
 * SingleColumnTimestampHelper handles generation of queries when a single timestamp
 * column is specified.
 */
public class SingleColumnTimestampHelper implements TimestampHelper {

  private String timestampColumn;

  public SingleColumnTimestampHelper(String timestampColumn) {
    this.timestampColumn = timestampColumn;
  }

  @Override
  public void addWhereClause(StringBuilder builder, String quoteString, String incrementingColumn) {
    builder.append(" WHERE ");
    builder.append(JdbcUtils.quoteString(timestampColumn, quoteString));
    builder.append(" < ? AND ((");
    builder.append(JdbcUtils.quoteString(timestampColumn, quoteString));
    builder.append(" = ? AND ");
    builder.append(JdbcUtils.quoteString(incrementingColumn, quoteString));
    builder.append(" > ?");
    builder.append(") OR ");
    builder.append(JdbcUtils.quoteString(timestampColumn, quoteString));
    builder.append(" > ?)");
    builder.append(" ORDER BY ");
    builder.append(JdbcUtils.quoteString(timestampColumn, quoteString));
    builder.append(",");
    builder.append(JdbcUtils.quoteString(incrementingColumn, quoteString));
    builder.append(" ASC");
  }

  @Override
  public void addWhereClause(StringBuilder builder, String quoteString) {
    builder.append(" WHERE ");
    builder.append(JdbcUtils.quoteString(timestampColumn, quoteString));
    builder.append(" > ? AND ");
    builder.append(JdbcUtils.quoteString(timestampColumn, quoteString));
    builder.append(" < ? ORDER BY ");
    builder.append(JdbcUtils.quoteString(timestampColumn, quoteString));
    builder.append(" ASC");
  }

  @Override
  public Timestamp extractOffset(Struct record) {
    return (Timestamp) record.get(timestampColumn);
  }

  @Override
  public String toString() {
    return "SingleColumnTimestampHelper{"
        + "timestampColumn='" + timestampColumn + '\''
        + '}';
  }

}
