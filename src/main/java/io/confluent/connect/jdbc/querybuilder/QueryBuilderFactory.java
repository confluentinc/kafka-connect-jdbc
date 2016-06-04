package io.confluent.connect.jdbc.querybuilder;

/**
 * Created by stlowenthal on 6/3/16.
 */
public class QueryBuilderFactory {

    public static QueryBuilder QueryBuilderFactory(QueryBuilder.DBType dbType) {
        switch (dbType) {
            case POSTGRES:
                return new PostgresQueryBuilder();
            case MS_SQL:
                return new PostgresQueryBuilder();
            default:
                return new GenericQueryBuilder();
        }
    }
}
