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
import io.confluent.connect.jdbc.util.StringUtils;
import java.sql.Timestamp;
import java.util.List;
import org.apache.kafka.connect.data.Struct;

/**
 * MultiColumnTimestampHelper handles generation of queries when multiple timestamp
 * columns are specified. Based on the input order, it selects the first not null
 * timestamp value to detect new or modified rows.
 */
public class MultiColumnTimestampHelper implements TimestampHelper {

  private List<String> timestampColumns;

  public MultiColumnTimestampHelper(List<String> timestampColumns) {
    this.timestampColumns = timestampColumns;
  }

  @Override
  public void addWhereClause(StringBuilder builder,
      String quoteString, String incrementingColumn) {
    List<String> quotedList = JdbcUtils.quoteList(timestampColumns, quoteString);
    String quotedTimestampColumn = StringUtils.join(quotedList, ",");
    builder.append(" WHERE COALESCE(");
    builder.append(quotedTimestampColumn);
    builder.append(") < ? AND ((COALESCE(");
    builder.append(quotedTimestampColumn);
    builder.append(") = ? AND ");
    builder.append(JdbcUtils.quoteString(incrementingColumn, quoteString));
    builder.append(" > ?");
    builder.append(") OR COALESCE(");
    builder.append(quotedTimestampColumn);
    builder.append(") > ?)");
    builder.append(" ORDER BY COALESCE(");
    builder.append(quotedTimestampColumn);
    builder.append("),");
    builder.append(JdbcUtils.quoteString(incrementingColumn, quoteString));
    builder.append(" ASC");
  }

  @Override
  public void addWhereClause(StringBuilder builder, String quoteString) {
    List<String> quotedList = JdbcUtils.quoteList(timestampColumns, quoteString);
    String quotedTimestampColumn = StringUtils.join(quotedList, ",");
    builder.append(" WHERE COALESCE(");
    builder.append(quotedTimestampColumn);
    builder.append(") > ? AND COALESCE(");
    builder.append(quotedTimestampColumn);
    builder.append(") < ? ORDER BY COALESCE(");
    builder.append(quotedTimestampColumn);
    builder.append(") ASC");
  }

  @Override
  public Timestamp extractOffset(Struct record) {
    for (String timestampColumn: timestampColumns) {
      Timestamp timestampOffset = (Timestamp) record.getWithoutDefault(timestampColumn);
      if (timestampOffset != null) {
        return timestampOffset;
      }
    }
    return null;
  }

  @Override
  public String toString() {
    return "MultiColumnTimestampHelper{"
        + "timestampColumns=" + timestampColumns
        + '}';
  }
}
