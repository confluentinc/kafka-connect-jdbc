package io.confluent.connect.jdbc.source;

import io.confluent.connect.jdbc.util.JdbcUtils;
import io.confluent.connect.jdbc.util.StringUtils;
import java.sql.Timestamp;
import java.util.List;
import org.apache.kafka.connect.data.Struct;

public class MultiColumnTimestampHelper implements TimestampHelper {

  private List<String> timestampColumns;

  public MultiColumnTimestampHelper(List<String> timestampColumns) {
    this.timestampColumns = timestampColumns;
  }

  @Override
  public void incrementingWhereClauseBuilder(StringBuilder builder,
      String incrementingColumn, String quoteString) {
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
  public void buildWhereClause(StringBuilder builder, String quoteString) {
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
    return "MultiColumnTimestampHelper{" +
        "timestampColumns=" + timestampColumns +
        '}';
  }
}
