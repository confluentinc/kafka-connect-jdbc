package io.confluent.connect.jdbc.gp;

import io.confluent.connect.jdbc.sink.JdbcSinkConfig;
import io.confluent.connect.jdbc.sink.metadata.FieldsMetadata;
import io.confluent.connect.jdbc.util.TableDefinition;

public abstract class GpDataIngestionService implements IGPDataIngestionService {
    protected final JdbcSinkConfig config;
    protected TableDefinition tableDefinition;
    protected final FieldsMetadata fieldsMetadata;
    protected final String tableName;

    public GpDataIngestionService(JdbcSinkConfig config, TableDefinition tableDefinition, FieldsMetadata fieldsMetadata) {
        this.config = config;
        this.tableDefinition = tableDefinition;
        this.fieldsMetadata = fieldsMetadata;
        this.tableName = tableDefinition.id().tableName();
    }

    public GpDataIngestionService(JdbcSinkConfig config, String tableName, FieldsMetadata fieldsMetadata) {
        this.config = config;
        this.fieldsMetadata = fieldsMetadata;
        this.tableName = tableName;
    }

}
