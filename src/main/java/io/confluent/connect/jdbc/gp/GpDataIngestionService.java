package io.confluent.connect.jdbc.gp;

import io.confluent.connect.jdbc.dialect.DatabaseDialect;
import io.confluent.connect.jdbc.dialect.PostgreSqlDatabaseDialect;
import io.confluent.connect.jdbc.sink.JdbcSinkConfig;
import io.confluent.connect.jdbc.sink.metadata.FieldsMetadata;
import io.confluent.connect.jdbc.sink.metadata.SinkRecordField;
import io.confluent.connect.jdbc.util.ColumnDefinition;
import io.confluent.connect.jdbc.util.CommonUtils;
import io.confluent.connect.jdbc.util.ConnectionURLParser;
import io.confluent.connect.jdbc.util.TableDefinition;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.*;

public abstract class GpDataIngestionService implements IGPDataIngestionService {
    private static final Logger log = LoggerFactory.getLogger(GpDataIngestionService.class);
    protected final JdbcSinkConfig config;
    protected final DatabaseDialect dialect;
    protected TableDefinition tableDefinition;
    protected final FieldsMetadata fieldsMetadata;
    protected final String tableName;
    protected List<String> keyColumns;
    protected List<String> nonKeyColumns;
    protected List<String> allColumns;
    protected List<Map<String, String>> columnsWithDataType;
    protected List<List<String>> data;
    protected int totalColumns;
    protected int totalKeyColumns;
    protected int totalNonKeyColumns;
    protected int totalRecords;
    protected ConnectionURLParser dbConnection;

    public GpDataIngestionService(JdbcSinkConfig config, DatabaseDialect dialect, TableDefinition tableDefinition, FieldsMetadata fieldsMetadata) {
        this.config = config;
        this.tableDefinition = tableDefinition;
        this.fieldsMetadata = fieldsMetadata;
        this.tableName = tableDefinition.id().tableName();
        this.dialect = dialect;
        setupDbConnection();

    }

    public GpDataIngestionService(JdbcSinkConfig config, DatabaseDialect dialect, String tableName, FieldsMetadata fieldsMetadata) {
        this.config = config;
        this.fieldsMetadata = fieldsMetadata;
        this.tableName = tableName;
        this.dialect = dialect;
        setupDbConnection();

    }

    private void setupDbConnection() {
        dbConnection = new ConnectionURLParser(config.connectionUrl);
        if (dbConnection.getSchema() == null) {
            log.warn("Schema not found in jdbc url, getting schema from connector config");
            if (config.dbSchema != null) {
                log.info("Setting schema to {}", config.dbSchema);
                dbConnection.setSchema(config.dbSchema);
            } else {
                log.warn("Schema not found in connector config, using default schema: public");
                dbConnection.setSchema("public");
            }
        }
    }

    protected String getSQLType(SinkRecordField field) {
        if (dialect instanceof PostgreSqlDatabaseDialect)
            return ((PostgreSqlDatabaseDialect) dialect).getSqlType(field).toUpperCase();
        else
            return field.schema().type().getName().toUpperCase();
    }

    protected List<Map<String, String>> createColumnNameDataTypeMapList() {
        List<Map<String, String>> fieldsDataTypeMapList = new ArrayList<>();

        for (Map.Entry entry : fieldsMetadata.allFields.entrySet()) {
            Map<String, String> fieldsDataTypeMap = new HashMap<>();
            ColumnDefinition column = tableDefinition.definitionForColumn(entry.getKey().toString());
            if (column != null) {
                fieldsDataTypeMap.put(entry.getKey().toString(), column.typeName());
                fieldsDataTypeMapList.add(fieldsDataTypeMap);
            }
        }

        return fieldsDataTypeMapList;
    }


    protected String createColumnNameDataTypeString(String delimiter) {

        List<String> fieldsDataTypeList = new ArrayList<>();

        for (Map.Entry entry : fieldsMetadata.allFields.entrySet()) {
            ColumnDefinition column = tableDefinition.definitionForColumn(entry.getKey().toString());
            if (column != null) {
                fieldsDataTypeList.add(entry.getKey().toString() + " " + column.typeName());
            }
        }

        return String.join(delimiter, fieldsDataTypeList);
    }

    @Override
    public void ingest(List<SinkRecord> records) {
        keyColumns = new ArrayList<>(fieldsMetadata.keyFieldNames);
        nonKeyColumns = new ArrayList<>(fieldsMetadata.nonKeyFieldNames);
        allColumns = new ArrayList<>(fieldsMetadata.allFields.keySet());
        totalColumns = allColumns.size();
        totalKeyColumns = keyColumns.size();
        totalNonKeyColumns = nonKeyColumns.size();
        totalRecords = records.size();

        columnsWithDataType = createColumnNameDataTypeMapList();

        // print all counts in one shot
        log.info("Total Columns: {}, Total Key Columns: {}, Total Non Key Columns: {}, Total Records: {}", totalColumns, totalKeyColumns, totalNonKeyColumns, totalRecords);
        log.info("Update mode is {}", config.updateMode.name());

        data = new ArrayList<>();
        if (config.updateMode == JdbcSinkConfig.UpdateMode.DEFAULT) {

            for (SinkRecord record : records) {
                List row = new ArrayList(totalColumns);
                final Struct valueStruct = (Struct) record.value();
                for (int i = 0; i < totalColumns; i++) {
                    String value = String.valueOf(valueStruct.get(allColumns.get(i).toString()));
                    if (value == null) {
                        value = config.nullString;
                    }
                    row.add(i, value);
                }
                if (config.printDebugLogs){
                    log.info("Adding row: {}", row);
            }
                data.add(row);
            }
        } else {

            if (config.updateMode == JdbcSinkConfig.UpdateMode.LAST_ROW_ONLY) {
                Collections.reverse(records);
            }

            List<String> addedKeysList = new ArrayList<>();

            for (SinkRecord record : records) {

                String recordKey = "";
                for (String key : keyColumns) {
                    recordKey += String.valueOf(((Struct) record.key()).get(key));
                }
                if(addedKeysList.contains(recordKey)){
                    continue;
                }
                addedKeysList.add(recordKey);

                List row = new ArrayList(totalColumns);
                final Struct valueStruct = (Struct) record.value();
                for (int i = 0; i < totalColumns; i++) {
                    String value = String.valueOf(valueStruct.get(allColumns.get(i).toString()));
                    if (value == null) {
                        value = config.nullString;
                    }
                    row.add(i, value);
                }
                //update mode
                if (config.printDebugLogs){
                    log.info("Adding row: {}", row);
                }
                data.add(row);
            }

            if (config.updateMode == JdbcSinkConfig.UpdateMode.LAST_ROW_ONLY) {
                Collections.reverse(data);
            }
            log.info("Total records after applying update mode: {}", data.size());
        }
    }

    protected String getGpfDistHost() {
        String localIpOrHost = "localhost";

        if (config.gpfdistHost != null) {
            localIpOrHost = config.gpfdistHost;
        } else {
            localIpOrHost = CommonUtils.getLocalIpOrHost();

        }
        return localIpOrHost;
    }
}
