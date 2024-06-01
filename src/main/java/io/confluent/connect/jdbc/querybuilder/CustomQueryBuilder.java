package io.confluent.connect.jdbc.querybuilder;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by stlowenthal on 6/6/16.
 */
public class CustomQueryBuilder extends GenericQueryBuilder {

    @Override
    public QueryBuilder withUserQuery(String userQuery) {
        this.userQuery = userQuery;
        return this;
    }

    @Override
    public QueryBuilder withLimit(int limit) {
        throw new IllegalStateException("This Builder does not support Limits");
    }

    @Override
    public QueryBuilder withIncrementingColumn(String incrementingColumn) {
        throw new IllegalStateException("This Builder does not support an incrementing column");
    }

    @Override
    public QueryBuilder withTimestampColumn(String timestampColumn) {
        throw new IllegalStateException("This Builder does not support a timestamp column");
    }

    @Override
    public void buildQuery() {

        // TODO - Add support for parameterized table name and parameters

    }

    @Override
    public String getQueryString() {
        return userQuery;
    }

    @Override
    public List<QueryParameter> getQueryParameters() {
        return new ArrayList<QueryParameter>();
    }
}
