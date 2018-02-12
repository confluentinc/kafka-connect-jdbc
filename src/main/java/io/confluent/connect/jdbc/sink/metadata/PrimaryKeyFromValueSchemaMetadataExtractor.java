package io.confluent.connect.jdbc.sink.metadata;

import io.confluent.connect.jdbc.sink.JdbcSinkConfig;
import org.apache.kafka.connect.data.Field;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public class PrimaryKeyFromValueSchemaMetadataExtractor implements MetadataExtractor{

    private final Set<String> primaryKeys;

    public PrimaryKeyFromValueSchemaMetadataExtractor(Set<String> primaryKeys) {
        this.primaryKeys = primaryKeys;
    }

    @Override
    public FieldsMetadata extract(String tableName, JdbcSinkConfig config, SchemaPair schemaPair) {
        final Map<String, SinkRecordField> allFields = new HashMap<>();
        for(String keyField:primaryKeys){
            Field field = schemaPair.valueSchema.field(keyField);
            allFields.put(field.name(), new SinkRecordField(field.schema(), field.name(), true));
        }
        return FieldsMetadata.extract(tableName,primaryKeys,allFields,config.fieldsWhitelist,schemaPair.valueSchema);
    }
}
