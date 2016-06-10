package io.confluent.connect.jdbc.querybuilder;

/**
 * Created by stlowenthal on 6/3/16.
 */
public interface QueryBuilderTest {
    String TABLE_NAME = "table1";
    String QUOTE = "\"";
    String INCREMENTING_COLUMN = "seq1";
    String TIMESTAMP_COLUMN = "ts1";
    int LIMIT = 1000;
}
