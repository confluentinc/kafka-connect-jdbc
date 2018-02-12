package io.confluent.connect.jdbc.sink.metadata;

import io.confluent.connect.jdbc.sink.JdbcSinkConfig;


public interface MetadataExtractor {

    FieldsMetadata extract(String tableName, JdbcSinkConfig config, SchemaPair schemaPair);
}
