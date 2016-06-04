package io.confluent.connect.jdbc.querybuilder;

import org.junit.Test;

import static io.confluent.connect.jdbc.querybuilder.QueryBuilder.DBType.POSTGRES;
import static junit.framework.TestCase.assertEquals;
import static org.junit.Assert.*;

/**
 * Created by stlowenthal on 6/3/16.
 */
public class PostgresQueryBuilderTest implements QueryBuilderTest {
    @Test
    public void postgresTimestampQuery() {
        QueryBuilder qb = QueryBuilderFactory.getQueryBuilder(POSTGRES)
                .withQuoteString(QUOTE)
                .withTableName(TABLE_NAME)
                .withIncrementingColumn(null)
                .withTimestampColumn(TIMESTAMP_COLUMN);

        qb.buildQuery();

        assertEquals("SELECT * FROM \"table1\"  WHERE \"ts1\" > ? AND \"ts1\" < CURRENT_TIMESTAMP ORDER BY \"ts1\" ASC", qb.getQuery());
        assertEquals(1, qb.getQueryParameters().size());
        assertEquals(QueryBuilder.QueryParameter.TIMESTAMP_COLUMN,qb.getQueryParameters().get(0));

    }

    @Test
    public void postgresIncrementingQuery() {
        QueryBuilder qb = QueryBuilderFactory.getQueryBuilder(POSTGRES)
                .withQuoteString(QUOTE)
                .withTableName(TABLE_NAME)
                .withIncrementingColumn(INCREMENTING_COLUMN)
                .withTimestampColumn(null);

        qb.buildQuery();

        assertEquals("SELECT * FROM \"table1\"  WHERE \"seq1\" > ? ORDER BY \"seq1\" ASC", qb.getQuery());
        assertEquals(1, qb.getQueryParameters().size());
        assertEquals(QueryBuilder.QueryParameter.INCREMENTING_COLUMN,qb.getQueryParameters().get(0));
    }

    @Test
    public void postgresIncrementingTimestampQuery() {
        QueryBuilder qb = QueryBuilderFactory.getQueryBuilder(POSTGRES)
                .withQuoteString(QUOTE)
                .withTableName(TABLE_NAME)
                .withIncrementingColumn(INCREMENTING_COLUMN)
                .withTimestampColumn(TIMESTAMP_COLUMN);

        qb.buildQuery();

        assertEquals("SELECT * FROM \"table1\"  WHERE \"ts1\" < CURRENT_TIMESTAMP AND (\"ts1\", \"seq1\") > (?, ?) ORDER BY \"ts1\",\"seq1\" ASC", qb.getQuery());
        assertEquals(2, qb.getQueryParameters().size());
        assertEquals(QueryBuilder.QueryParameter.TIMESTAMP_COLUMN,qb.getQueryParameters().get(0));
        assertEquals(QueryBuilder.QueryParameter.INCREMENTING_COLUMN,qb.getQueryParameters().get(1));
    }

    @Test
    public void postgresTimestampQueryWithLimit() {
        QueryBuilder qb = QueryBuilderFactory.getQueryBuilder(POSTGRES)
                .withQuoteString(QUOTE)
                .withTableName(TABLE_NAME)
                .withIncrementingColumn(null)
                .withTimestampColumn(TIMESTAMP_COLUMN)
                .withLimit(LIMIT);

        qb.buildQuery();

        assertEquals("SELECT * FROM \"table1\"  WHERE \"ts1\" > ? AND \"ts1\" < CURRENT_TIMESTAMP ORDER BY \"ts1\" ASC LIMIT 1000", qb.getQuery());
        assertEquals(1, qb.getQueryParameters().size());
        assertEquals(QueryBuilder.QueryParameter.TIMESTAMP_COLUMN,qb.getQueryParameters().get(0));

    }

    @Test
    public void postgresIncrementingQueryWithLimit() {
        QueryBuilder qb = QueryBuilderFactory.getQueryBuilder(POSTGRES)
                .withQuoteString(QUOTE)
                .withTableName(TABLE_NAME)
                .withIncrementingColumn(INCREMENTING_COLUMN)
                .withTimestampColumn(null)
                .withLimit(LIMIT);

        qb.buildQuery();

        assertEquals("SELECT * FROM \"table1\"  WHERE \"seq1\" > ? ORDER BY \"seq1\" ASC LIMIT 1000" , qb.getQuery());
        assertEquals(1, qb.getQueryParameters().size());
        assertEquals(QueryBuilder.QueryParameter.INCREMENTING_COLUMN,qb.getQueryParameters().get(0));
    }

    @Test
    public void postgresIncrementingTimestampQueryWithLimit() {
        QueryBuilder qb = QueryBuilderFactory.getQueryBuilder(POSTGRES)
                .withQuoteString(QUOTE)
                .withTableName(TABLE_NAME)
                .withIncrementingColumn(INCREMENTING_COLUMN)
                .withTimestampColumn(TIMESTAMP_COLUMN)
                .withLimit(LIMIT);

        qb.buildQuery();

        assertEquals("SELECT * FROM \"table1\"  WHERE \"ts1\" < CURRENT_TIMESTAMP AND (\"ts1\", \"seq1\") > (?, ?) ORDER BY \"ts1\",\"seq1\" ASC LIMIT 1000", qb.getQuery());
        assertEquals(2, qb.getQueryParameters().size());
        assertEquals(QueryBuilder.QueryParameter.TIMESTAMP_COLUMN,qb.getQueryParameters().get(0));
        assertEquals(QueryBuilder.QueryParameter.INCREMENTING_COLUMN,qb.getQueryParameters().get(1));
    }
}