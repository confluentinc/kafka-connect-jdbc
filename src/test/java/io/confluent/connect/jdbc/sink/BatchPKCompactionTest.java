package io.confluent.connect.jdbc.sink;

import io.confluent.connect.jdbc.sink.metadata.FieldsMetadata;
import io.confluent.connect.jdbc.sink.metadata.SchemaPair;
import io.confluent.connect.jdbc.sink.metadata.SinkRecordField;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.Test;

import java.util.*;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.Assert.assertEquals;

public class BatchPKCompactionTest {

    private static final String FIELD_ID = "id";
    private static final String FIELD_VALUE = "value";

    @Test
    public void test() {
        final Map<String, SinkRecordField> allFields = new HashMap<>();
        allFields.put(FIELD_ID, new SinkRecordField(Schema.INT64_SCHEMA, FIELD_ID, true));
        allFields.put(FIELD_VALUE, new SinkRecordField(Schema.STRING_SCHEMA, FIELD_VALUE, false));
        BatchPKCompaction compaction = new BatchPKCompaction(
                JdbcSinkConfig.PrimaryKeyMode.RECORD_KEY,
                new FieldsMetadata(
                        new HashSet<>(Arrays.asList(FIELD_ID)),
                        new HashSet<>(Arrays.asList(FIELD_VALUE)),
                        allFields
                ),
                new SchemaPair(Schema.STRING_SCHEMA, schemaIdValue)
        );

        final SinkRecord sinkRecord1 = createRecord(1l, "value1");
        final SinkRecord sinkRecord2 = createRecord(2l, "value2");
        final SinkRecord sinkRecord3 = createRecord(1l, "value3");
        List<SinkRecord> result = compaction.applyCompaction(Arrays.asList(sinkRecord1, sinkRecord2, sinkRecord3));

        assertEquals(2, result.size());
        assertEquals(sinkRecord2, result.get(0));
        assertEquals(sinkRecord3, result.get(1));
    }


    private final Schema schemaIdValue = SchemaBuilder.struct()
            .field("id", Schema.INT64_SCHEMA)
            .field("value", Schema.STRING_SCHEMA);

    private final AtomicLong kafkaOffset = new AtomicLong(0);

    private SinkRecord createRecord(Long id, String value) {
        return new SinkRecord(
                "topic",
                0,
                Schema.STRING_SCHEMA,
                id.toString(),
                schemaIdValue,
                new Struct(schemaIdValue)
                        .put("id", id)
                        .put("value", value),
                kafkaOffset.incrementAndGet()
        );
    }
}