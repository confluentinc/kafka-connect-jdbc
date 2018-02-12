package io.confluent.connect.jdbc.sink.metadata;

import io.confluent.connect.jdbc.sink.JdbcSinkConfig;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.errors.ConnectException;

import java.util.*;

public class RecordValuePrimaryKeyStrategy implements PrimaryKeyStrategy{

    private final Schema valueSchema;

    public RecordValuePrimaryKeyStrategy(Schema valueSchema) {
        this.valueSchema = valueSchema;
    }

    @Override
    public PrimaryKeyFields get(List<String> configuredPkFields, String tableName) {
        final Set<String> keyFieldNames = new LinkedHashSet<>();
        final Map<String, SinkRecordField> allFields = new HashMap<>();
        if (valueSchema == null) {
            throw new ConnectException(String.format("PK mode for table '%s' is %s, but record value schema is missing", tableName, JdbcSinkConfig.PrimaryKeyMode.RECORD_VALUE));
        }
        if (configuredPkFields.isEmpty()) {
            for (Field keyField : valueSchema.fields()) {
                keyFieldNames.add(keyField.name());
            }
        } else {
            for (String fieldName : configuredPkFields) {
                if (valueSchema.field(fieldName) == null) {
                    throw new ConnectException(String.format(
                            "PK mode for table '%s' is %s with configured PK keyFields %s, but record value schema does not contain field: %s",
                            tableName, JdbcSinkConfig.PrimaryKeyMode.RECORD_VALUE, configuredPkFields, fieldName
                    ));
                }
            }
            keyFieldNames.addAll(configuredPkFields);
        }
        for (String fieldName : keyFieldNames) {
            final Schema fieldSchema = valueSchema.field(fieldName).schema();
            allFields.put(fieldName, new SinkRecordField(fieldSchema, fieldName, true));
        }
        return new PrimaryKeyStrategy.PrimaryKeyFields(keyFieldNames, allFields);
    }
}
