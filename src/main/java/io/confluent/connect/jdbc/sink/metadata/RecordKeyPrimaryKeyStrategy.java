package io.confluent.connect.jdbc.sink.metadata;

import io.confluent.connect.jdbc.sink.JdbcSinkConfig;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.errors.ConnectException;

import java.util.*;

public class RecordKeyPrimaryKeyStrategy implements PrimaryKeyStrategy{

    private final Schema keySchema;

    public RecordKeyPrimaryKeyStrategy(Schema keySchema) {
        this.keySchema = keySchema;
    }

    @Override
    public PrimaryKeyStrategy.PrimaryKeyFields get(List<String> configuredPkFields, String tableName) {
        final Set<String> keyFieldNames = new LinkedHashSet<>();
        final Map<String, SinkRecordField> allFields = new HashMap<>();
        if (keySchema == null) {
            throw new ConnectException(String.format(
                    "PK mode for table '%s' is %s, but record key schema is missing", tableName, JdbcSinkConfig.PrimaryKeyMode.RECORD_KEY
            ));
        }
        final Schema.Type keySchemaType = keySchema.type();
        if (keySchemaType.isPrimitive()) {
            if (configuredPkFields.size() != 1) {
                throw new ConnectException(String.format(
                        "Need exactly one PK column defined since the key schema for records is a primitive type, defined columns are: %s",
                        configuredPkFields
                ));
            }
            final String fieldName = configuredPkFields.get(0);
            keyFieldNames.add(fieldName);
            allFields.put(fieldName, new SinkRecordField(keySchema, fieldName, true));
        } else if (keySchemaType == Schema.Type.STRUCT) {
            if (configuredPkFields.isEmpty()) {
                for (Field keyField : keySchema.fields()) {
                    keyFieldNames.add(keyField.name());
                }
            } else {
                for (String fieldName : configuredPkFields) {
                    final Field keyField = keySchema.field(fieldName);
                    if (keyField == null) {
                        throw new ConnectException(String.format(
                                "PK mode for table '%s' is %s with configured PK keyFields %s, but record key schema does not contain field: %s",
                                tableName, JdbcSinkConfig.PrimaryKeyMode.RECORD_KEY, configuredPkFields, fieldName
                        ));
                    }
                }
                keyFieldNames.addAll(configuredPkFields);
            }
            for (String fieldName : keyFieldNames) {
                final Schema fieldSchema = keySchema.field(fieldName).schema();
                allFields.put(fieldName, new SinkRecordField(fieldSchema, fieldName, true));
            }
        } else {
            throw new ConnectException("Key schema must be primitive type or Struct, but is of type: " + keySchemaType);
        }
        return new PrimaryKeyStrategy.PrimaryKeyFields(keyFieldNames, allFields);
    }
}
