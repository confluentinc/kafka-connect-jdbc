package io.confluent.connect.jdbc.querybuilder;

import io.confluent.connect.jdbc.JdbcUtils;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by stlowenthal on 6/3/16.
 */
public class PostgresQueryBuilder extends GenericQueryBuilder {


    public PostgresQueryBuilder() {
        super();
    }

    @Override
    protected StringBuilder buildIncrementing() {
        return maybeAddLimit(super.buildIncrementing());
    }

    @Override
    protected StringBuilder buildTimestamp() {
        return maybeAddLimit(super.buildTimestamp());
    }

    protected List<QueryParameter> getIncrementingTimestampParameters() {
        List<QueryParameter> parameters = new ArrayList<>(1);
        parameters.add(QueryParameter.TIMESTAMP_COLUMN);
        parameters.add(QueryParameter.INCREMENTING_COLUMN);
        return parameters;
    }

    @Override
    protected StringBuilder buildIncrementingTimeStamp() {
        StringBuilder builder = new StringBuilder();

        builder.append(" WHERE ")
                .append(JdbcUtils.quoteString(timestampColumn, quoteString))
                .append(" < CURRENT_TIMESTAMP AND (")
                .append(JdbcUtils.quoteString(timestampColumn, quoteString))
                .append(", ")
                .append(JdbcUtils.quoteString(incrementingColumn, quoteString))
                .append(") > (?, ?)")
                .append(" ORDER BY ")
                .append(JdbcUtils.quoteString(timestampColumn, quoteString))
                .append(",")
                .append(JdbcUtils.quoteString(incrementingColumn, quoteString))
                .append(" ASC");

        maybeAddLimit(builder);

        return builder;
    }

    private StringBuilder maybeAddLimit(StringBuilder builder) {

        // A value of 0 means NO LIMIT
        if (limit > 0) {
            builder.append(" LIMIT ")
                    .append(limit);
        }
        return builder;
    }
}
