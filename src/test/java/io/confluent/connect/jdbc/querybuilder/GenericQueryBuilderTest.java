package io.confluent.connect.jdbc.querybuilder;

import org.junit.Test;

import static io.confluent.connect.jdbc.querybuilder.QueryBuilder.DBType.GENERIC;
import static junit.framework.TestCase.assertEquals;
import static junit.framework.TestCase.assertTrue;
import static io.confluent.connect.jdbc.querybuilder.QueryBuilder.QueryParameter;

/**
 * Created by stlowenthal on 6/3/16.
 */
public class GenericQueryBuilderTest implements QueryBuilderTest {

    @Test
    public void genericTimestampQuery() {
        QueryBuilder qb = QueryBuilderFactory.getQueryBuilder(GENERIC)
                .withQuoteString(QUOTE)
                .withTableName(TABLE_NAME)
                .withIncrementingColumn(null)
                .withTimestampColumn(TIMESTAMP_COLUMN);

        qb.buildQuery();

        assertEquals("SELECT * FROM \"table1\"  WHERE \"ts1\" > ? AND \"ts1\" < CURRENT_TIMESTAMP ORDER BY \"ts1\" ASC", qb.getQuery());
        assertEquals(1, qb.getQueryParameters().size());
        assertEquals(QueryParameter.TIMESTAMP_COLUMN,qb.getQueryParameters().get(0));

    }

    @Test
    public void genericIncrementingQuery() {
        QueryBuilder qb = QueryBuilderFactory.getQueryBuilder(GENERIC)
                .withQuoteString(QUOTE)
                .withTableName(TABLE_NAME)
                .withIncrementingColumn(INCREMENTING_COLUMN)
                .withTimestampColumn(null);

        qb.buildQuery();

        assertEquals("SELECT * FROM \"table1\"  WHERE \"seq1\" > ? ORDER BY \"seq1\" ASC", qb.getQuery());
        assertEquals(1, qb.getQueryParameters().size());
        assertEquals(QueryParameter.INCREMENTING_COLUMN,qb.getQueryParameters().get(0));
    }

    @Test
    public void genericIncrementingTimestampQuery() {
        QueryBuilder qb = QueryBuilderFactory.getQueryBuilder(GENERIC)
                .withQuoteString(QUOTE)
                .withTableName(TABLE_NAME)
                .withIncrementingColumn(INCREMENTING_COLUMN)
                .withTimestampColumn(TIMESTAMP_COLUMN);

        qb.buildQuery();

        assertEquals("SELECT * FROM \"table1\"  WHERE \"ts1\" < CURRENT_TIMESTAMP AND ((\"ts1\" = ? AND \"seq1\" > ?) OR \"ts1\" > ?) ORDER BY \"ts1\",\"seq1\" ASC", qb.getQuery());
        assertEquals(3, qb.getQueryParameters().size());
        assertEquals(QueryParameter.TIMESTAMP_COLUMN,qb.getQueryParameters().get(0));
        assertEquals(QueryParameter.INCREMENTING_COLUMN,qb.getQueryParameters().get(1));
        assertEquals(QueryParameter.TIMESTAMP_COLUMN,qb.getQueryParameters().get(2));
    }

}