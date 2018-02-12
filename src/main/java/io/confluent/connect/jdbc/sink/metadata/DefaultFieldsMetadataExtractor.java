package io.confluent.connect.jdbc.sink.metadata;

import io.confluent.connect.jdbc.sink.JdbcSinkConfig;

public class DefaultFieldsMetadataExtractor implements MetadataExtractor {

    private DefaultFieldsMetadataExtractor(){

    }

    public static final MetadataExtractor Instance = new DefaultFieldsMetadataExtractor();

    @Override
    public FieldsMetadata extract(String tableName, JdbcSinkConfig config, SchemaPair schemaPair) {
        return FieldsMetadata.extract(tableName, config.pkMode, config.pkFields, config.fieldsWhitelist, schemaPair);
    }
}
