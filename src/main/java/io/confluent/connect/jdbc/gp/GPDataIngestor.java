package io.confluent.connect.jdbc.gp;

import io.confluent.connect.jdbc.sink.JdbcSinkConfig;
import io.confluent.connect.jdbc.sink.metadata.FieldsMetadata;
import io.confluent.connect.jdbc.util.TableDefinition;

import java.util.List;

public abstract class GPDataIngestor implements IGPDataIngestor{
    protected final JdbcSinkConfig config;
    protected TableDefinition tableDefinition;
    protected final FieldsMetadata fieldsMetadata;
    protected final String tableName;

    public GPDataIngestor(JdbcSinkConfig config, TableDefinition tableDefinition, FieldsMetadata fieldsMetadata) {
        this.config = config;
        this.tableDefinition = tableDefinition;
        this.fieldsMetadata = fieldsMetadata;
        this.tableName = tableDefinition.id().tableName();
    }

    public GPDataIngestor(JdbcSinkConfig config, String tableName, FieldsMetadata fieldsMetadata) {
        this.config = config;
        this.fieldsMetadata = fieldsMetadata;
        this.tableName = tableName;
    }

}
