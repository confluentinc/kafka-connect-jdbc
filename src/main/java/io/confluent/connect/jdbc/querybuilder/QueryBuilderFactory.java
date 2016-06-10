package io.confluent.connect.jdbc.querybuilder;
import static io.confluent.connect.jdbc.querybuilder.QueryBuilder.DBType;


/**
 * Created by stlowenthal on 6/3/16.
 */

public class QueryBuilderFactory {

    public static QueryBuilder getQueryBuilder(DBType dbType) {
        switch (dbType) {
            case CUSTOM_QUERY:
                return new CustomQueryBuilder();
            case POSTGRES:
                return new PostgresQueryBuilder();
            case MS_SQL:
                return new MSSqlQueryBuilder();
            default:
                return new GenericQueryBuilder();
        }
    }

    public static DBType dbProductNameToDBType(String productName) {
        if (productName.contentEquals("PostgreSQL"))
            return DBType.POSTGRES;
        else if (productName.contentEquals("Microsoft SQL Server"))
            return DBType.MS_SQL;
        else
            return DBType.GENERIC;
    }
}
