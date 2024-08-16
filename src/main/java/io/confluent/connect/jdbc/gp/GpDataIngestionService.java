package io.confluent.connect.jdbc.gp;

import io.confluent.connect.jdbc.dialect.DatabaseDialect;
import io.confluent.connect.jdbc.dialect.PostgreSqlDatabaseDialect;
import io.confluent.connect.jdbc.sink.JdbcSinkConfig;
import io.confluent.connect.jdbc.sink.metadata.ColumnDetails;
import io.confluent.connect.jdbc.sink.metadata.FieldsMetadata;
import io.confluent.connect.jdbc.sink.metadata.SchemaPair;
import io.confluent.connect.jdbc.sink.metadata.SinkRecordField;
import io.confluent.connect.jdbc.util.*;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.stream.Collectors;

public abstract class GpDataIngestionService implements IGPDataIngestionService {
    private static final Logger log = LoggerFactory.getLogger(GpDataIngestionService.class);
    protected final JdbcSinkConfig config;
    protected final DatabaseDialect dialect;
    protected SchemaPair schemaPair;
    protected TableDefinition tableDefinition;
    protected final FieldsMetadata fieldsMetadata;
    protected final String tableName;
    protected List<String> keyColumns;
    protected List<String> updateColumnsList;
    protected List<String> insertColumnsList;
    protected List<String> nonKeyColumns;
    protected List<Map<String, String>> columnsWithDataType;
    protected List<List<String>> data;
    protected int totalColumns;
    protected int totalKeyColumns;
    protected int totalNonKeyColumns;
    protected int totalRecords;
    protected ConnectionURLParser dbConnection;

    public GpDataIngestionService(JdbcSinkConfig config, DatabaseDialect dialect, TableDefinition tableDefinition, FieldsMetadata fieldsMetadata, SchemaPair schemaPair) {
        this.config = config;
        this.tableDefinition = tableDefinition;
        this.fieldsMetadata = fieldsMetadata;
        this.tableName = tableDefinition.id().tableName();
        this.dialect = dialect;
        this.schemaPair = schemaPair;
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

    protected List<Map<String, String>> createColumnNameDataTypeMapList(List<String> allColumns) {
        List<Map<String, String>> fieldsDataTypeMapList = new ArrayList<>();

        for (String columnName : allColumns) {
            Map<String, String> fieldsDataTypeMap = new HashMap<>();
            ColumnDefinition column = tableDefinition.definitionForColumn(columnName);
            if (column != null) {
                fieldsDataTypeMap.put(columnName, column.typeName());
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
        updateColumnsList = new ArrayList<>();
        insertColumnsList = new ArrayList<>();
        List<String> allColumns = new ArrayList<>(fieldsMetadata.allFields.keySet());


        if (config.columnSelectionStrategy == JdbcSinkConfig.ColumnSelectionStrategy.SINK_PREFERRED) {
            log.info("Applying column selection strategy {}", config.columnSelectionStrategy.name());
            List<String> sinkTableColumns = tableDefinition.getOrderedColumns().stream().map(ColumnDetails::getColumnName).collect(Collectors.toList());
            //log
            log.info("Sink table columns: {}", sinkTableColumns);
            keyColumns.retainAll(sinkTableColumns);
            // nonKeyColumns.retainAll(sinkTableColumns);
            // allColumns.retainAll(sinkTableColumns);

            // now add columns to nonKeyColumns and allColumns from sinkTableColumn if they are missing, in the same order as they are in sinkTableColumns
            nonKeyColumns.clear();
            allColumns.clear();

            allColumns.addAll(sinkTableColumns);
            nonKeyColumns.addAll(sinkTableColumns);
            nonKeyColumns.removeAll(keyColumns);


        }
        log.info("excluded columns list for update " + config.updateExcludeColumns);

        log.info("excluded columns list for insert " + config.insertExcludeColumns);

        // add all columns except the updateExcludeColumns to updateColumnsList, excluded columns may have fully qualified names like tablename.columnname
        List<String> excludedColumns = config.updateExcludeColumns.stream().filter(
                column -> column.contains(".") ? column.split("\\.")[0].equals(tableName) : true
        ).collect(Collectors.toList());
        log.info("list of excluded columns for update for table: " + tableName + " " + excludedColumns);

        updateColumnsList.addAll(nonKeyColumns);
        log.info("The list of columns before exclusion from update list for table " + tableName + " "+ updateColumnsList );
        updateColumnsList.removeAll(excludedColumns);
        log.info("The list of columns after exclusion from update list for table " + tableName + " " + updateColumnsList );

        // add all columns except the insertExcludeColumns to insertColumnsList, excluded columns may have fully qualified names like tablename.columnname
        excludedColumns = config.insertExcludeColumns.stream().filter(
                column -> column.contains(".") ? column.split("\\.")[0].equals(tableName) : true
        ).collect(Collectors.toList());
        log.info("list of excluded columns for insert for table: " + tableName + " "+ excludedColumns);

        insertColumnsList.addAll(allColumns);
        log.info("The list of columns before exclusion from update list for table " + tableName + " " + insertColumnsList );
        insertColumnsList.removeAll(excludedColumns);
        log.info("The list of columns after exclusion from update list for table " + tableName + " " + insertColumnsList );


        totalColumns = insertColumnsList.size();
        totalKeyColumns = keyColumns.size();
        totalNonKeyColumns = nonKeyColumns.size();
        totalRecords = records.size();

        columnsWithDataType = createColumnNameDataTypeMapList(insertColumnsList);


        if (config.printDebugLogs) {
            log.info("Column Selection::Key columns: {}", keyColumns);
            log.info("Column Selection::Non Key columns: {}", nonKeyColumns);
            log.info("Column Selection::All columns: {}", insertColumnsList);
        }
        // print all counts in one shot
        log.info("Total Columns: {}, Total Key Columns: {}, Total Non Key Columns: {}, Total Records: {}", totalColumns, totalKeyColumns, totalNonKeyColumns, totalRecords);
        log.info("Update mode is {}", config.updateMode.name());

        data = new ArrayList<>();
        if (config.updateMode == JdbcSinkConfig.UpdateMode.DEFAULT) {

            for (SinkRecord record : records) {
                addRow(record);
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
                if (addedKeysList.contains(recordKey)) {
                    continue;
                }
                addedKeysList.add(recordKey);

                addRow(record);
            }

            if (config.updateMode == JdbcSinkConfig.UpdateMode.LAST_ROW_ONLY) {
                Collections.reverse(data);
            }
            log.info("Total records after applying update mode: {}", data.size());
        }
    }

    private void addRow(SinkRecord record) {
        List row = new ArrayList(totalColumns);
        final Struct valueStruct = (Struct) record.value();

//        valueStruct.schema().fields().forEach(field -> {
//            if (config.printDebugLogs) {
//                try {
//                    String value = String.valueOf(valueStruct.get(field.name()));
//                    log.info("==>>>> Field: {} - {} - {}", field.name(), field.schema().type().name(), field.schema().valueSchema().type());
//
//                }catch (Exception e){
//                }
//            }
//        });

        // print tableDefinition.getOrderedColumns()
        tableDefinition.getOrderedColumns().forEach(c -> {
            if (config.printDebugLogs) {
                log.info(">>>>>TableDefinition Column: {} - {}", c.getColumnName(), c.getDataType());
            }
        });


        for (int i = 0; i < totalColumns; i++) {
            String value = null;
            String key = insertColumnsList.get(i).toString();
            try {
                value = String.valueOf(valueStruct.get(key));
            } catch (Exception e) {
                try {

                    String alternateKey = config.columnAlternative.get(key);
                    if (alternateKey != null) {
                        if (alternateKey != null) {
                            if (alternateKey.startsWith("#")) {
                                UniqueIdType sak = UniqueIdType.fromString(alternateKey.substring(1));
                                if (sak != null) {
                                    value = sak.generateUniqueId();
                                } else {
                                    value = alternateKey.substring(1);
                                }
                            } else {
                                value = String.valueOf(valueStruct.get(alternateKey));

                            }
                        }
                    }

                } catch (Exception e1) {
                    if (config.printDebugLogs) {
                        log.error("Error while getting alternative value for column {} from record {}", key, record);
                    }
                }
                if (config.printDebugLogs) {
                    log.error("Error while getting value for column {} from record {}", key, record);
                }
//                        if(tableDefinition.getOrderedColumns()!=null) {
//                            final int j = i;
//                            ColumnDetails column = tableDefinition.getOrderedColumns().stream().filter(c -> c.getColumnName().equals(allColumns.get(j).toString())).findFirst().orElse(null);
//                            if (column != null && column.getColumnDefault() != null) {
//                                value = "DEFAULT";
//                            }
//                        }
            }
            if (value == null) {
                value = config.nullString;
            }

            // date format
            if (config.timestampAutoConvert && value != null && value.length() > 0) {
                ColumnDetails column = tableDefinition.getOrderedColumn(key);
                if (column != null && column.getDateType() != null) {
                    if (config.printDebugLogs) {
                        log.info("Date Value before conversion: {} for column: {}", value, key);
                    }
                    value = column.getDateType().format(config, value);
                    if (config.printDebugLogs) {
                        log.info("Date Value after: {} for column: {}", value, key);
                    }
                }
            }
            if (config.printDebugLogs) {
                log.info("Adding value: {} for column: {}", value, key);
            }
            row.add(i, value);
        }
        if (config.printDebugLogs) {
            log.info("Adding row: {}", row);
        }
        data.add(row);
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


    protected List<ColumnDetails> getSinkColumnDetails() {
        List<ColumnDetails> fieldsDataTypeMapList = new ArrayList<>();

        for (Map.Entry entry : fieldsMetadata.allFields.entrySet()) {
            ColumnDefinition column = tableDefinition.definitionForColumn(entry.getKey().toString());
            if (column != null) {
                fieldsDataTypeMapList.add(new ColumnDetails(entry.getKey().toString(), column.typeName(), null, null));
            }
        }
        return fieldsDataTypeMapList;
    }
}
