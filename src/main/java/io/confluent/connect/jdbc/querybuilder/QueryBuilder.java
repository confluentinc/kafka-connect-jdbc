package io.confluent.connect.jdbc.querybuilder;

import java.util.List;

/**
 * Created by stlowenthal on 6/3/16.
 */
public interface QueryBuilder {

    QueryBuilder withQuoteString(String quoteString);

    QueryBuilder withTableName(String tablename);

    QueryBuilder withUserQuery(String userQuery);

    QueryBuilder withLimit(int limit);

    QueryBuilder withIncrementingColumn(String incrementingColumn);

    QueryBuilder withTimestampColumn(String timestampColumn);

    void buildQuery();

    String getQuery();

    List<QueryParameter> getQueryParameters();

    enum QueryParameter {
        TIMESTAMP_COLUMN,
        INCREMENTING_COLUMN
    }

    enum DBType {
        GENERIC,
        POSTGRES,
        MS_SQL
    }

}
