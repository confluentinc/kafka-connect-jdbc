package io.confluent.connect.jdbc.gp;

import io.confluent.connect.jdbc.dialect.DatabaseDialect;
import io.confluent.connect.jdbc.dialect.PostgreSqlDatabaseDialect;
import io.confluent.connect.jdbc.sink.JdbcSinkConfig;
import io.confluent.connect.jdbc.sink.metadata.FieldsMetadata;
import io.confluent.connect.jdbc.sink.metadata.SinkRecordField;
import io.confluent.connect.jdbc.util.ColumnDefinition;
import io.confluent.connect.jdbc.util.TableDefinition;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public abstract class GpDataIngestionService implements IGPDataIngestionService {
    protected final JdbcSinkConfig config;
    protected final DatabaseDialect dialect;
    protected TableDefinition tableDefinition;
    protected final FieldsMetadata fieldsMetadata;
    protected final String tableName;

    public GpDataIngestionService(JdbcSinkConfig config, DatabaseDialect dialect, TableDefinition tableDefinition, FieldsMetadata fieldsMetadata) {
        this.config = config;
        this.tableDefinition = tableDefinition;
        this.fieldsMetadata = fieldsMetadata;
        this.tableName = tableDefinition.id().tableName();
        this.dialect = dialect;
    }

    public GpDataIngestionService(JdbcSinkConfig config, DatabaseDialect dialect, String tableName, FieldsMetadata fieldsMetadata) {
        this.config = config;
        this.fieldsMetadata = fieldsMetadata;
        this.tableName = tableName;
        this.dialect = dialect;
    }

    protected String getSQLType(SinkRecordField field) {
        if(dialect instanceof PostgreSqlDatabaseDialect)
        return ((PostgreSqlDatabaseDialect)dialect).getSqlType(field).toUpperCase();
        else
        return field.schema().type().getName().toUpperCase();
    }

    protected List<Map<String, String>> createColumnNameDataTypeMapList() {
        List<Map<String, String>> fieldsDataTypeMapList = new ArrayList<>();

        for (Map.Entry entry : fieldsMetadata.allFields.entrySet()) {
            Map<String, String> fieldsDataTypeMap = new HashMap<>();
            ColumnDefinition column = tableDefinition.definitionForColumn(entry.getKey().toString());
            if(column!=null) {
                fieldsDataTypeMap.put(entry.getKey().toString(), column.typeName());
                fieldsDataTypeMapList.add(fieldsDataTypeMap);
            }
        }

//        fieldsMetadata.allFields.forEach((k, v) -> {
//            fieldsDataTypeMap.put(k, getSQLType(v));
//        });
        return fieldsDataTypeMapList;
    }


    protected String createColumnNameDataTypeString(String delimiter) {

        List<String> fieldsDataTypeList = new ArrayList<>();

        for (Map.Entry entry : fieldsMetadata.allFields.entrySet()) {
            ColumnDefinition column = tableDefinition.definitionForColumn(entry.getKey().toString());
            if(column!=null) {
                fieldsDataTypeList.add(entry.getKey().toString() + " " + column.typeName());
            }
        }

        return String.join(delimiter, fieldsDataTypeList);
    }


}
