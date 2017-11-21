package io.confluent.connect.jdbc.source;

import io.confluent.connect.jdbc.util.JdbcUtils;
import java.sql.Timestamp;
import org.apache.kafka.connect.data.Struct;

public class SingleColumnTimestampHelper implements TimestampHelper {

  private String timestampColumn;

  public SingleColumnTimestampHelper(String timestampColumn) {
    this.timestampColumn = timestampColumn;
  }

  @Override
  public void incrementingWhereClauseBuilder(StringBuilder builder, String incrementingColumn, String quoteString) {
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
  public void buildWhereClause(StringBuilder builder, String quoteString) {
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
    return "SingleColumnTimestampHelper{" +
        "timestampColumn='" + timestampColumn + '\'' +
        '}';
  }

}
