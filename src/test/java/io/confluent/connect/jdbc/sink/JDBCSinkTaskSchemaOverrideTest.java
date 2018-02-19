package io.confluent.connect.jdbc.sink;

import io.confluent.connect.jdbc.source.JdbcSourceConnectorConfig;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class JDBCSinkTaskSchemaOverrideTest {

    @Test
    public void testInsertOnNotPublicSchema() {
        JdbcSinkTask task = new JdbcSinkTask();
        Map<String,String> props = new HashMap<>();
        props.put("connection.url", "jdbc:postgresql://ec2-54-247-81-97.eu-west-1.compute.amazonaws.com:5432/d6btulnv1q0u8k?user=khcadaodnqukph&password=60f3f0166e659af68d9e6fe5429d82d1d5dd0bf14bab5e5318d9823a6e93dda0&sslmode=require");
        props.put("table.name.format","contact");
        props.put(JdbcSinkConfig.SCHEMA_OVERRIDE_CONFIG,"salesforce");
        props.put(JdbcSourceConnectorConfig.SCHEMA_PATTERN_CONFIG,"salesforce");
        task.start(props);

        SchemaBuilder schemaBuilder = new SchemaBuilder(Schema.Type.STRUCT)
                .field("firstname",Schema.STRING_SCHEMA)
                .field("lastname",Schema.STRING_SCHEMA);
        Schema schema = schemaBuilder.build();
        final Struct valueA = new Struct(schema)
                .put("firstname", "avro")
                .put("lastname","avro");
        SinkRecord sinkRecord = new SinkRecord("intermediary-topic",1,null,null, schema,valueA,0);
        List<SinkRecord> list = new ArrayList<>();
        list.add(sinkRecord);
        task.put(list);
    }

    @Test
    public void testUpdateOnNotPublicSchema() {
        JdbcSinkTask task = new JdbcSinkTask();
        Map<String,String> props = new HashMap<>();
        props.put("connection.url", "jdbc:postgresql://ec2-54-247-81-97.eu-west-1.compute.amazonaws.com:5432/d6btulnv1q0u8k?user=khcadaodnqukph&password=60f3f0166e659af68d9e6fe5429d82d1d5dd0bf14bab5e5318d9823a6e93dda0&sslmode=require");
        props.put("table.name.format","contact");
        props.put(JdbcSinkConfig.SCHEMA_OVERRIDE_CONFIG,"salesforce");
        props.put(JdbcSinkConfig.INSERT_MODE, "update");
        props.put(JdbcSourceConnectorConfig.SCHEMA_PATTERN_CONFIG,"salesforce");
        props.put(JdbcSinkConfig.PK_MODE, "record_value");
        props.put(JdbcSinkConfig.PK_FIELDS, "external_id__c");
        task.start(props);

        SchemaBuilder schemaBuilder = new SchemaBuilder(Schema.Type.STRUCT)
                .field("firstname",Schema.STRING_SCHEMA)
                .field("lastname",Schema.STRING_SCHEMA)
                .field("external_id__c", Schema.STRING_SCHEMA);
        Schema schema = schemaBuilder.build();
        final Struct valueA = new Struct(schema)
                .put("firstname", "avro")
                .put("lastname","avro234")
                .put("external_id__c", "a14df100-bdd1-4226-859d-adjk996a6e36");
        SinkRecord sinkRecord = new SinkRecord("read-to-postgres-2",1,null,null, schema,valueA,0);
        List<SinkRecord> list = new ArrayList<>();
        list.add(sinkRecord);
        task.put(list);
    }

    @Test
    public void testUpsertOnNotPublicSchema() {
        JdbcSinkTask task = new JdbcSinkTask();
        Map<String,String> props = new HashMap<>();
        props.put("connection.url", "jdbc:postgresql://ec2-54-247-81-97.eu-west-1.compute.amazonaws.com:5432/d6btulnv1q0u8k?user=khcadaodnqukph&password=60f3f0166e659af68d9e6fe5429d82d1d5dd0bf14bab5e5318d9823a6e93dda0&sslmode=require");
        props.put("table.name.format","contact");
        props.put(JdbcSinkConfig.SCHEMA_OVERRIDE_CONFIG,"salesforce");
        props.put(JdbcSinkConfig.INSERT_MODE, "upsert");
        props.put(JdbcSourceConnectorConfig.SCHEMA_PATTERN_CONFIG,"salesforce");
        props.put(JdbcSinkConfig.PK_MODE, "record_value");
        props.put(JdbcSinkConfig.PK_FIELDS, "external_id__c");
        task.start(props);

        SchemaBuilder schemaBuilder = new SchemaBuilder(Schema.Type.STRUCT)
                .field("firstname",Schema.STRING_SCHEMA)
                .field("lastname",Schema.STRING_SCHEMA)
                .field("external_id__c", Schema.STRING_SCHEMA);
        Schema schema = schemaBuilder.build();
        final Struct valueA = new Struct(schema)
                .put("firstname", "avro")
                .put("lastname","avro234")
                .put("external_id__c", "a14df100-bdd1-4226-859d-adjk996a6e36");
        SinkRecord sinkRecord = new SinkRecord("read-to-postgres-2",1,null,null, schema,valueA,0);
        List<SinkRecord> list = new ArrayList<>();
        list.add(sinkRecord);
        task.put(list);
    }
}
