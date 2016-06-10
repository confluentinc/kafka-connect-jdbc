package io.confluent.connect.jdbc.querybuilder;

import io.confluent.connect.jdbc.JdbcUtils;
import io.confluent.connect.jdbc.querybuilder.QueryBuilder;

import javax.naming.OperationNotSupportedException;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by stlowenthal on 6/3/16.
 */
public class GenericQueryBuilder implements QueryBuilder {

    private String query;
    private List<QueryParameter> queryParameters = null;

    protected String quoteString;
    protected String tableName = null;
    protected String userQuery = null;
    protected int limit = 0;
    protected String incrementingColumn = null;
    protected String timestampColumn = null;


    public GenericQueryBuilder() {
    }

    @Override
    public QueryBuilder withQuoteString(String quoteString) {
        this.quoteString = quoteString;
        return this;
    }

    @Override
    public QueryBuilder withTableName(String tablename) {
        this.tableName = tablename;
        return this;
    }

    @Override
    public QueryBuilder withUserQuery(String userQuery) {
        throw new IllegalStateException("This Builder does not support Custom Queries");
    }

    @Override
    public QueryBuilder withLimit(int limit) {
        this.limit = limit;
        return this;
    }

    @Override
    public QueryBuilder withIncrementingColumn(String incrementingColumn) {
        this.incrementingColumn = incrementingColumn;
        return this;
    }

    @Override
    public QueryBuilder withTimestampColumn(String timestampColumn) {
        this.timestampColumn = timestampColumn;
        return this;
    }

    @Override
    public void buildQuery() {
        StringBuilder builder = new StringBuilder();

        builder.append(buildSelectList());

        builder.append(' ');

        if (incrementingColumn != null && timestampColumn != null) {
            builder.append(buildIncrementingTimeStamp());
            queryParameters = getIncrementingTimestampParameters();
        } else if (incrementingColumn != null) {
            builder.append(buildIncrementing());
            queryParameters = getIncrementingParameters();
        } else if (timestampColumn != null) {
            builder.append(buildTimestamp());
            queryParameters = getTimeStampParameters();
        }

        query = builder.toString();
    }

    protected StringBuilder buildSelectList() {
        StringBuilder builder = new StringBuilder()
                .append("SELECT * FROM ")
                .append(JdbcUtils.quoteString(tableName, quoteString));
        return builder;
    }

    protected List<QueryParameter> getTimeStampParameters() {
        List<QueryParameter> parameters = new ArrayList<>(1);
        parameters.add(QueryParameter.TIMESTAMP_COLUMN);
        return parameters;
    }

    protected StringBuilder buildTimestamp() {

        StringBuilder builder = new StringBuilder()
                .append(" WHERE ")
                .append(JdbcUtils.quoteString(timestampColumn, quoteString))
                .append(" > ? AND ")
                .append(JdbcUtils.quoteString(timestampColumn, quoteString))
                .append(" < CURRENT_TIMESTAMP ORDER BY ")
                .append(JdbcUtils.quoteString(timestampColumn, quoteString))
                .append(" ASC");
        return builder;
    }

    protected List<QueryParameter> getIncrementingParameters() {
        List<QueryParameter> parameters = new ArrayList<>(1);
        parameters.add(QueryParameter.INCREMENTING_COLUMN);
        return parameters;
    }

    protected StringBuilder buildIncrementing() {

        StringBuilder builder = new StringBuilder()
                .append(" WHERE ")
                .append(JdbcUtils.quoteString(incrementingColumn, quoteString))
                .append(" > ?")
                .append(" ORDER BY ")
                .append(JdbcUtils.quoteString(incrementingColumn, quoteString))
                .append(" ASC");
        return builder;
    }

    protected List<QueryParameter> getIncrementingTimestampParameters() {
        List<QueryParameter> parameters = new ArrayList<>(1);
        parameters.add(QueryParameter.TIMESTAMP_COLUMN);
        parameters.add(QueryParameter.INCREMENTING_COLUMN);
        parameters.add(QueryParameter.TIMESTAMP_COLUMN);
        return parameters;
    }

    protected StringBuilder buildIncrementingTimeStamp() {
        // This version combines two possible conditions. The first checks timestamp == last
        // timestamp and incrementing > last incrementing. The timestamp alone would include
        // duplicates, but adding the incrementing condition ensures no duplicates, e.g. you would
        // get only the row with id = 23:
        //  timestamp 1234, id 22 <- last
        //  timestamp 1234, id 23
        // The second check only uses the timestamp >= last timestamp. This covers everything new,
        // even if it is an update of the existing row. If we previously had:
        //  timestamp 1234, id 22 <- last
        // and then these rows were written:
        //  timestamp 1235, id 22
        //  timestamp 1236, id 23
        // We should capture both id = 22 (an update) and id = 23 (a new row)

        StringBuilder builder = new StringBuilder()
                .append(" WHERE ")
                .append(JdbcUtils.quoteString(timestampColumn, quoteString))
                .append(" < CURRENT_TIMESTAMP AND ((")
                .append(JdbcUtils.quoteString(timestampColumn, quoteString))
                .append(" = ? AND ")
                .append(JdbcUtils.quoteString(incrementingColumn, quoteString))
                .append(" > ?) OR ")
                .append(JdbcUtils.quoteString(timestampColumn, quoteString))
                .append(" > ?)")
                .append(" ORDER BY ")
                .append(JdbcUtils.quoteString(timestampColumn, quoteString))
                .append(",")
                .append(JdbcUtils.quoteString(incrementingColumn, quoteString))
                .append(" ASC");
        return builder;
    }

    @Override
    public String getQueryString() {
        return query;
    }

    @Override
    public List<QueryParameter> getQueryParameters() {
        return queryParameters;
    }
}
