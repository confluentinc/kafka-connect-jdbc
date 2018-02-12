package io.confluent.connect.jdbc.sink;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTaskContext;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;

public class JdbcRecordConversionTest {

    private final SqliteHelper sqliteHelper = new SqliteHelper(getClass().getSimpleName());

    private static final Schema SCHEMA = SchemaBuilder.struct().name("com.example.PersonToFlatten")
            .field("firstName", Schema.STRING_SCHEMA)
            .field("lastName", Schema.STRING_SCHEMA)
            .field("age", Schema.INT32_SCHEMA)
            .build();

    @Before
    public void setUp() throws IOException, SQLException {
        sqliteHelper.setUp();
    }

    @After
    public void tearDown() throws IOException, SQLException {
        sqliteHelper.tearDown();
    }


    @Test
    public void recordConverterWorks() throws SQLException{
        JdbcSinkTask task = new JdbcSinkTask();
        Map<String, String> props = new HashMap<>();
        props.put("connection.url", sqliteHelper.sqliteUri());
        props.put("pk.mode", "record_value");
        props.put(JdbcSinkConfig.SINK_RECORD_CONVERTER,JdbcRecordExampleConverter.class.getName());
        task.initialize(mock(SinkTaskContext.class));
        final String flattenableTopic = "atopic";
        final Struct struct = new Struct(SCHEMA)
                .put("firstName", "Pippo")
                .put("lastName", "Pluto")
                .put("age", 38);
        sqliteHelper.createTable(
                "CREATE TABLE FLATTENED_PERSON(" +
                        "    firstName  TEXT," +
                        "    lastName  TEXT, " +
                        "PRIMARY KEY (firstName, lastName));"
        );
        sqliteHelper.createTable(
                "CREATE TABLE FLATTENED_PERSON_AGE(" +
                        "    firstName  TEXT," +
                        "    age  INTEGER, " +
                        "PRIMARY KEY (firstName));"
        );
        task.start(props);
        SinkRecord sinkRecord = new SinkRecord(flattenableTopic,1,null,null,SCHEMA, struct,50);
        task.put(Collections.singleton(sinkRecord));

        assertEquals(
                1,
                sqliteHelper.select(
                        "SELECT * FROM FLATTENED_PERSON",
                        new SqliteHelper.ResultSetReadCallback() {
                            @Override
                            public void read(ResultSet rs) throws SQLException {
                                assertEquals(struct.getString("firstName"), rs.getString("firstName"));
                                assertEquals(struct.getString("lastName"), rs.getString("lastName"));
                            }
                        }
                )
        );

        assertEquals(
                1,
                sqliteHelper.select(
                        "SELECT * FROM FLATTENED_PERSON_AGE",
                        new SqliteHelper.ResultSetReadCallback() {
                            @Override
                            public void read(ResultSet rs) throws SQLException {
                                assertEquals(struct.getString("firstName"), rs.getString("firstName"));
                                assertEquals(struct.getInt32("age").intValue(), rs.getInt("age"));
                            }
                        }
                )
        );


    }

}
