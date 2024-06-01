package io.confluent.connect.jdbc.querybuilder;

import io.confluent.connect.jdbc.JdbcUtils;

/**
 * Created by stlowenthal on 6/3/16.
 */
public class MSSqlQueryBuilder extends GenericQueryBuilder {

    @Override
    protected StringBuilder buildSelectList() {

        StringBuilder builder = new StringBuilder()
                .append("SELECT");
                 if (limit > 0) {
                     builder.append(" TOP ").append(limit);
                 }
                builder.append(" * FROM ")
                .append(JdbcUtils.quoteString(tableName, quoteString));
        return builder;
    }
}
