package io.confluent.connect.jdbc.sink;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;

public class JdbcRecordExampleConverter implements  JdbcRecordConverter{

    private static final Schema PERSON_SCHEMA = SchemaBuilder.struct().name("com.example.FlattenPerson")
            .field("firstName", Schema.STRING_SCHEMA)
            .field("lastName", Schema.STRING_SCHEMA)
            .build();

    private static final Schema AGE_SCHEMA = SchemaBuilder.struct().name("com.example.FlattenAge")
            .field("firstName", Schema.STRING_SCHEMA)
            .field("age", Schema.INT32_SCHEMA)
            .build();


    @Override
    public Collection<FixedTableRecord> convert(SinkRecord sinkRecord) {
        Struct struct =  (Struct) sinkRecord.value();
        final Struct personStruct = new Struct(PERSON_SCHEMA)
                .put("firstName", struct.getString("firstName"))
                .put("lastName",  struct.getString("lastName"));
        final Struct ageStruct = new Struct(AGE_SCHEMA)
                .put("firstName", struct.getString("firstName"))
                .put("age",  struct.getInt32("age"));
        SinkRecord newRecord1 = new SinkRecord("irrelevant",1,null,null,PERSON_SCHEMA,personStruct,50);
        SinkRecord newRecord2 = new SinkRecord("irrelevant",1,null,null,AGE_SCHEMA,ageStruct,51);
        FixedTableRecord r1 = FixedTableRecord.newInstance(newRecord1,"FLATTENED_PERSON",new HashSet<>(Arrays.asList("firstName","lastName")));
        FixedTableRecord r2 = FixedTableRecord.newInstance(newRecord2,"FLATTENED_PERSON_AGE",new HashSet<>(Arrays.asList("firstName")));
        return Arrays.asList(r1,r2);

    }
}
