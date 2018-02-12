package io.confluent.connect.jdbc.sink.metadata;

import io.confluent.connect.jdbc.sink.JdbcSinkConfig;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.errors.ConnectException;

import java.util.*;

public class KafkaPrimaryKeyStrategy implements PrimaryKeyStrategy{

    @Override
    public PrimaryKeyFields get(List<String> configuredPkFields, String tableName) {
        final Set<String> keyFieldNames = new LinkedHashSet<>();
        final Map<String, SinkRecordField> allFields = new HashMap<>();
        if (configuredPkFields.isEmpty()) {
            keyFieldNames.addAll(JdbcSinkConfig.DEFAULT_KAFKA_PK_NAMES);
        } else if (configuredPkFields.size() == 3) {
            // This is a simple check to verify topicFieldName, partitionFieldName and offsetFieldName are already there
            keyFieldNames.addAll(configuredPkFields);
        } else {
            throw new ConnectException(String.format(
                    "PK mode for table '%s' is %s so there should either be no field names defined for defaults %s to be applicable, "
                            + "or exactly 3, defined keyFields are: %s",
                    tableName, JdbcSinkConfig.PrimaryKeyMode.KAFKA, JdbcSinkConfig.DEFAULT_KAFKA_PK_NAMES, configuredPkFields
            ));
        }
        final Iterator<String> it = keyFieldNames.iterator();
        final String topicFieldName = it.next();
        allFields.put(topicFieldName, new SinkRecordField(Schema.STRING_SCHEMA, topicFieldName, true));
        final String partitionFieldName = it.next();
        allFields.put(partitionFieldName, new SinkRecordField(Schema.INT32_SCHEMA, partitionFieldName, true));
        final String offsetFieldName = it.next();
        allFields.put(offsetFieldName, new SinkRecordField(Schema.INT64_SCHEMA, offsetFieldName, true));
        return new PrimaryKeyFields(keyFieldNames, allFields);
    }

    private KafkaPrimaryKeyStrategy(){

    }

    public static PrimaryKeyStrategy Instance = new KafkaPrimaryKeyStrategy();


}
