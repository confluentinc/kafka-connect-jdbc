package io.confluent.connect.jdbc.gp.gpss;

import io.confluent.connect.jdbc.dialect.DatabaseDialect;
import io.confluent.connect.jdbc.gp.GpDataIngestionService;
import io.confluent.connect.jdbc.sink.JdbcSinkConfig;
import io.confluent.connect.jdbc.sink.metadata.ColumnDetails;
import io.confluent.connect.jdbc.sink.metadata.FieldsMetadata;
import io.confluent.connect.jdbc.sink.metadata.SchemaPair;
import io.confluent.connect.jdbc.util.TableDefinition;
import org.apache.kafka.connect.sink.SinkRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

public class GPSSDataIngestionService extends GpDataIngestionService {

    private static final Logger log = LoggerFactory.getLogger(GPSSDataIngestionService.class);

    public GPSSDataIngestionService(JdbcSinkConfig config, DatabaseDialect dialect, TableDefinition tabDef, FieldsMetadata fieldsMetadata, SchemaPair schemaPair) {
        super(config, dialect, tabDef, fieldsMetadata, schemaPair);
    }

    public GPSSDataIngestionService(JdbcSinkConfig config, DatabaseDialect dialect, String tableName, FieldsMetadata fieldsMetadata) {
        super(config, dialect, tableName, fieldsMetadata);
    }

    @Override
    public void ingest(List<SinkRecord> records) {
        super.ingest(records);
        GPSSWrapper gpssWrapper = new GPSSWrapper(config);
        gpssWrapper.ingestBatch(tableName, allColumns, keyColumns, nonKeyColumns, getSourceColumnDetails(),"", data);
    }


//
//    private void openGpssChannel() {
//        try {
//            channel = ManagedChannelBuilder.forAddress(config.gpssHost, Integer.parseInt(config.gpssPort)).usePlaintext().build();
//            bStub = GpssGrpc.newBlockingStub(channel);
//
//        } catch (Exception e) {
//            log.debug("Exception while connecting to Stream Server", e);
//            throw new RuntimeException("Exception while connecting to Stream Server", e);
//        }
//    }
//
//    private void closeGpssChannel() {
//        try {
//            channel.shutdown().awaitTermination(7, TimeUnit.SECONDS);
//
//        } catch (Exception e) {
//            log.error("Exception while closing channel to Stream Server", e);
//            throw new RuntimeException("Exception while closing channel to Stream Server", e);
//        }
//    }
//
//    private void connectToGP() {
//        try {
//
//            ConnectionURLParser parser = new ConnectionURLParser(config.connectionUrl);
//
//            ConnectRequest connReq = ConnectRequest.newBuilder().setHost(parser.getHost()).setPort(parser.getPort())
//                    .setUsername(parser.getUsername()).setPassword(parser.getPassword()).setDB(parser.getDatabase())
//                    .setUseSSL(false).build();
//            mSession = bStub.connect(connReq);
//        } catch (Exception e) {
//            log.debug("Exception while connecting to Greenplum Server", e);
//            throw new RuntimeException("Exception while connecting to Greenplum Server", e);
//        }
//    }
//
//    private List<String> listSchemas() {
//        try {
//            // create a list schema request builder
//            ListSchemaRequest lsReq = ListSchemaRequest.newBuilder().setSession(mSession).build();
//
//            // use the blocking stub to call the ListSchema service
//            List<Schema> listSchema = bStub.listSchema(lsReq).getSchemasList();
//
//            // extract the name of each schema and save in an array
//            List<String> schemaNameList = new ArrayList<String>();
//            for (Schema s : listSchema) {
//                schemaNameList.add(s.getName());
//            }
//            return schemaNameList;
//        } catch (Exception e) {
//            log.debug("Exception while reading schema(s)", e);
//            throw new RuntimeException("Exception while reading schema(s)", e);
//        }
//    }
//
//    private List<String> listTables(String schemaName) {
//        try {
//            // create a list table request builder
//            ListTableRequest ltReq = ListTableRequest.newBuilder().setSession(mSession).setSchema(schemaName).build();
//
//            // use the blocking stub to call the ListTable service
//            List<TableInfo> tblList = bStub.listTable(ltReq).getTablesList();
//
//            // extract the name of each table only and save in an array
//            List<String> tblNameList = new ArrayList<String>();
//            for (TableInfo ti : tblList) {
//                if (ti.getTypeValue() == RelationType.Table_VALUE) {
//                    tblNameList.add(ti.getName());
//                }
//            }
//            return tblNameList;
//        } catch (Exception e) {
//            log.debug("Exception while reading table(s)", e);
//            throw new RuntimeException("Exception while reading table(s)", e);
//        }
//    }
//
//
//    private Map<String, String> fetchTableInfo(String tableName) {
//        // create a describe table request builder
//        DescribeTableRequest dtReq = DescribeTableRequest.newBuilder()
//                .setSession(mSession)
//                .setSchemaName(config.dbSchema)
//                .setTableName(tableName)
//                .build();
//
//        // use the blocking stub to call the DescribeTable service
//        List<ColumnInfo> columnList = bStub.describeTable(dtReq).getColumnsList();
//        Map<String, String> columnsMap = new HashMap<String, String>();
//        // print the name and type of each column
//        for (ColumnInfo ci : columnList) {
//            String colname = ci.getName();
//            String dbtype = ci.getDatabaseType();
//            columnsMap.put(colname, dbtype);
//        }
//        return columnsMap;
//    }
//
//    private void openTableForWrite(String tableName, List<SinkRecord> records) {
//
//        final List fields = Arrays.asList(fieldsMetadata.allFields.keySet().toArray());
//        Integer errLimit = config.gpErrorsLimit;
//        Integer errPct = config.gpErrorsPercentageLimit;
//        // create an insert option builder
//        InsertOption iOpt = InsertOption.newBuilder()
//                .addAllInsertColumns(columnNames)
//                .setErrorLimitCount(config.gpErrorsLimit)
//                .setErrorLimitPercentage(config.gpErrorsPercentageLimit)
//                .setTruncateTable(false)
//                .build();
//
//        MergeOption mOpt = MergeOption.newBuilder()
//                .addAllInsertColumns(columnNames)
//                .addAllMatchColumns()
//                .build();
//
//                .setErrorLimitCount(config.gpErrorsLimit)
//                .setErrorLimitPercentage(config.gpErrorsPercentageLimit)
//
//                .build();
//
//        FormatCSV csv = FormatCSV.newBuilder().setNull(config.nullString).setDelimiter(config.delimiter).setQuote(config.csvQuote).setEscape("\n").setHeader(false).build();
//
////		 create an open request builder
//        OpenRequest oReq = OpenRequest.newBuilder()
//                .setSession(mSession)
//                .setSchemaName(config.dbSchema)
//                .setTableName(tableName)
//                .setTimeout(30000)
//                .setCsv(csv)
//                .setInsertOption(iOpt)
//                .build();
//
//        // use the blocking stub to call the Open service; it returns nothing
//        bStub.open(oReq);
//    }
//
//    private void writeToTable(List<SinkRecord> records) {
//        try {
//            List<RowData> rows = new ArrayList<RowData>();
//            final List fields = Arrays.asList(fieldsMetadata.allFields.keySet().toArray()); // optimization
//
//            records.forEach(record -> {
//                final Struct valueStruct = (Struct) record.value();
//                Row.Builder builder = Row.newBuilder();
//                for (int i = 0; i < fields.size(); i++) {
//                    Object value = valueStruct.get(fields.get(i).toString());
//                    org.apache.kafka.connect.data.Schema schemaType = valueStruct.schema().field(fields.get(i).toString()).schema();
//
//                    fieldsMetadata.allFields.get(fields.get(i).toString()).schema().type();
//
//                    DBValue.Builder valueBuilder = DBValue.newBuilder();
//
//                    try {
//                        bindField(valueBuilder, schemaType, value);
//                    } catch (SQLException ex) {
//                        log.error("Exception while binding field", ex);
//                        throw new RuntimeException(ex);
//                    }
//
//                    builder.addColumns(valueBuilder.build());
//
//                    builder.addColumns(DBValue.newBuilder().setStringValue(UUID.randomUUID().toString()).build());
//                }
//                RowData.Builder rowbuilder = RowData.newBuilder().setData(builder.build().toByteString());
//                rows.add(rowbuilder.build());
//
//            });
//
//            // create a write request builder
//            WriteRequest wReq = WriteRequest.newBuilder().setSession(mSession).addAllRows(rows).build();
//
//            // use the blocking stub to call the Write service; it returns nothing
//            bStub.write(wReq);
//        } catch (Exception e) {
//            log.debug("Exception while writing to table", e);
//            throw new RuntimeException("Exception while writing to table", e);
//        }
//    }
//
//    private String closeTable() {
//        // create a close request builder
//        TransferStats tStats = null;
//        CloseRequest cReq = CloseRequest.newBuilder()
//                .setSession(mSession)
//                .build();
//
//        // use the blocking stub to call the Close service
//        tStats = bStub.close(cReq);
//        return tStats.toString();
//    }
//
//
//    public void checkEnv() {
//        openGpssChannel();
//        connectToGP();
//        List<String> schemaNameList = listSchemas();
//        schemaNameList.forEach(schema -> log.debug("Schema Name ->" + schema));
//        List<String> tables = listTables(config.dbSchema);
//        tables.forEach(table -> {
//            log.debug("Table Name ->" + table);
//            Map<String, String> columnsMap = fetchTableInfo(table);
//            columnsMap.forEach((colname, dbtype) -> {
//                log.debug("column " + colname + " type: " + dbtype);
//            });
//        });
//
//        closeGpssChannel();
//    }
//
//
////    protected DBValue bindField(
////            DBValue.Builder builder,
////            org.apache.kafka.connect.data.Schema schema,
////            Object value
////    ) throws SQLException {
////
////        switch (schema.type()) {
////            case ARRAY: {
////                Class<?> valueClass = value.getClass();
////                Object newValue = null;
////                Collection<?> valueCollection;
////                if (Collection.class.isAssignableFrom(valueClass)) {
////                    valueCollection = (Collection<?>) value;
////                } else if (valueClass.isArray()) {
////                    valueCollection = Arrays.asList((Object[]) value);
////                } else {
////                    throw new DataException(
////                            String.format("Type '%s' is not supported for Array.", valueClass.getName())
////                    );
////                }
////
////                // All typecasts below are based on pgjdbc's documentation on how to use primitive arrays
////                // - https://jdbc.postgresql.org/documentation/head/arrays.html
////                switch (schema.valueSchema().type()) {
////                    case INT8: {
////                        // Gotta do this the long way, as Postgres has no single-byte integer,
////                        // so we want to cast to short as the next best thing, and we can't do that with
////
////                        builder.addRepeatedField(valueCollection.stream()
////                                .map(o -> ((Byte) o).shortValue())
////                                .toArray(Short[]::new));
////
////                        break;
////                    }
////                    case INT32:
////                        newValue = valueCollection.toArray(new Integer[0]);
////                        break;
////                    case INT16:
////                        newValue = valueCollection.toArray(new Short[0]);
////                        break;
////                    case BOOLEAN:
////                        newValue = valueCollection.toArray(new Boolean[0]);
////                        break;
////                    case STRING:
////                        newValue = valueCollection.toArray(new String[0]);
////                        break;
////                    case FLOAT64:
////                        newValue = valueCollection.toArray(new Double[0]);
////                        break;
////                    case FLOAT32:
////                        newValue = valueCollection.toArray(new Float[0]);
////                        break;
////                    case INT64:
////                        newValue = valueCollection.toArray(new Long[0]);
////                        break;
////                    default:
////                        break;
////                }
////
////                if (newValue != null) {
////                    statement.setObject(index, newValue, Types.ARRAY);
////                    return true;
////                }
////                break;
////            }
////            default:
////                break;
////        }
////    }
//
//
//    protected boolean bindField(
//            DBValue.Builder builder,
//            org.apache.kafka.connect.data.Schema schema,
//            Object value
//    ) throws SQLException {
//        switch (schema.type()) {
//            case INT8:
//                builder.setInt32Value((Byte) value);
//                break;
//            case INT16:
//                builder.setInt32Value((Short) value);
//                break;
//            case INT32:
//                builder.setInt32Value((Integer) value);
//                break;
//            case INT64:
//                builder.setInt64Value((Long) value);
//                break;
//            case FLOAT32:
//                builder.setFloat32Value((Float) value);
//                break;
//            case FLOAT64:
//                builder.setFloat64Value((Double) value);
//                break;
////            case BOOLEAN: TODO: fix this
////                builder.setInt32Value((Boolean) value);
////                break;
//            case STRING:
//                builder.setStringValue((String) value);
//                break;
//            case BYTES:
//                final byte[] bytes;
//                if (value instanceof ByteBuffer) {
//                    final ByteBuffer buffer = ((ByteBuffer) value).slice();
//                    bytes = new byte[buffer.remaining()];
//                    buffer.get(bytes);
//                } else {
//                    bytes = (byte[]) value;
//                }
//                ByteString byteString = ByteString.copyFrom(bytes);
//                builder.setBytesValue(byteString);
//                break;
//            default:
//                return false;
//        }
//        return true;
//    }

}
