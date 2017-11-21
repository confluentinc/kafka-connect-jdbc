package io.confluent.connect.jdbc.source;

import java.sql.Timestamp;
import org.apache.kafka.connect.data.Struct;

public interface TimestampHelper {

  public void incrementingWhereClauseBuilder(StringBuilder builder, String incrementingColumn, String quoteString);

  public void buildWhereClause(StringBuilder builder, String quoteString);

  public Timestamp extractOffset(Struct record);

}
