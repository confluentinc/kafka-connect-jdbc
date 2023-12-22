package io.confluent.connect.jdbc.gp.gpss;

import com.google.protobuf.ByteString;
import io.confluent.connect.jdbc.gp.gpss.api.*;
import io.confluent.connect.jdbc.sink.JdbcSinkConfig;
import io.confluent.connect.jdbc.util.ConnectionURLParser;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.util.*;
import java.util.concurrent.TimeUnit;

public class GPSSWrapper
{
    static final Log log = LogFactory.getLog(GPSSWrapper.class);
    private final JdbcSinkConfig config;

    GPSSWrapper(JdbcSinkConfig config) {
        this.config = config;
    }

    ManagedChannel channel = null;
    Session mSession = null;
    GpssGrpc.GpssBlockingStub bStub = null;

    /**
     * Verify the basic configuration;
     * 1. connect to GPSS
     * 2. Verify GP database connection
     * 3. Verify we can query the DB metadata to make sure we have access to schema
     */
    public void checkEnv() {
        openGpssChannel();
        connectToGP();
        List<String> schemaNameList = listSchemas();
        schemaNameList.forEach(schema -> log.debug("Schema Name ->" + schema));
        List<String> tables = listTables(config.dbSchema);
        tables.forEach(table -> {
            log.debug("Table Name ->" + table);
            Map<String, String> columnsMap = fetchTableInfo(table);
            columnsMap.forEach( (colname, dbtype) -> {
                log.debug( "column " + colname + " type: " + dbtype );
            });
        });
        closeGpssChannel();
    }
    /**
     * Ingest data to specified database table.
     */
    public void ingestBatch(String tableName, List<String> insertColumns, List<String> matchColumns, List<String> updateColumns, String condition, List<List<String>> data) {
        if(channel == null || channel.isShutdown() || channel.isTerminated()) {
            log.debug("Opening GPSS channel  .......");
            openGpssChannel();
        }
        if(mSession == null || !mSession.isInitialized()) {
            log.debug("Opening GPSS session  .......");
            connectToGP();
        }
        log.debug("Setting up GP table to write .......");
        openTableForWrite(tableName, insertColumns, matchColumns, updateColumns, condition);
        log.debug(" writing events to GP table .......");
        writeToTable(data);
        log.debug("Closing up GP table after write .......");
        String status = closeTable();
        log.info("GP table Write CloseRequest tStats: " + status);
       if(!config.gpssUseStickySession) {
           log.info("GP table Write CloseRequest tStats: " + status);
           log.debug("Closing Channel till next iteration .......");
           closeGpssChannel();
       }
    }

    private void openGpssChannel() {
        try {
            channel = ManagedChannelBuilder.forAddress(config.gpssHost, Integer.parseInt(config.gpssPort)).usePlaintext().build(); // TODO - use tls
            bStub = GpssGrpc.newBlockingStub(channel);

        } catch (Exception e) {
            log.error("Exception while connecting to Stream Server", e);
            throw new RuntimeException("Exception while connecting to Stream Server", e);
        }
    }

    private void closeGpssChannel() {
        try {
            channel.shutdown().awaitTermination(3, TimeUnit.SECONDS); // TODO - make configurable

        } catch (Exception e) {
            log.error("Exception while closing channel to Stream Server", e);
            throw new RuntimeException("Exception while closing channel to Stream Server", e);
        }
    }

    private void connectToGP() {
        try {

            ConnectionURLParser connection = new ConnectionURLParser(config.connectionUrl);

            ConnectRequest connReq = ConnectRequest.newBuilder().setHost(connection.getHost()).setPort(connection.getPort())
                    .setUsername(connection.getUsername()).setPassword(connection.getPassword()).setDB(connection.getDatabase())
                    .setUseSSL(false).build();
            mSession = bStub.connect(connReq);
        } catch (Exception e) {
            log.error("Exception while connecting to Greenplum Server", e);
            throw new RuntimeException("Exception while connecting to Greenplum Server", e);
        }
    }

    private List<String> listSchemas() {
        try {
            // create a list schema request builder
            ListSchemaRequest lsReq = ListSchemaRequest.newBuilder().setSession(mSession).build();

            // use the blocking stub to call the ListSchema service
            List<Schema> listSchema = bStub.listSchema(lsReq).getSchemasList();

            // extract the name of each schema and save in an array
            List<String> schemaNameList = new ArrayList<String>();
            for (Schema s : listSchema) {
                schemaNameList.add(s.getName());
            }
            return schemaNameList;
        } catch (Exception e) {
            log.error("Exception while reading schema(s)", e);
            throw new RuntimeException("Exception while reading schema(s)", e);
        }
    }

    private List<String> listTables(String schemaName) {
        try {
            // create a list table request builder
            ListTableRequest ltReq = ListTableRequest.newBuilder().setSession(mSession).setSchema(schemaName).build();

            // use the blocking stub to call the ListTable service
            List<TableInfo> tblList = bStub.listTable(ltReq).getTablesList();

            // extract the name of each table only and save in an array
            List<String> tblNameList = new ArrayList<String>();
            for (TableInfo ti : tblList) {
                if (ti.getTypeValue() == RelationType.Table_VALUE) {
                    tblNameList.add(ti.getName());
                }
            }
            return tblNameList;
        } catch (Exception e) {
            log.error("Exception while reading table(s)", e);
            throw new RuntimeException("Exception while reading table(s)", e);
        }
    }


    private Map<String, String> fetchTableInfo(String tableName) {
        // create a describe table request builder
        DescribeTableRequest dtReq = DescribeTableRequest.newBuilder()
                .setSession(mSession)
                .setSchemaName(config.dbSchema)
                .setTableName(tableName)
                .build();

        // use the blocking stub to call the DescribeTable service
        List<ColumnInfo> columnList = bStub.describeTable(dtReq).getColumnsList();
        Map<String, String> columnsMap = new HashMap<String, String>();
        // print the name and type of each column
        for(ColumnInfo ci : columnList) {
            String colname = ci.getName();
            String dbtype = ci.getDatabaseType();
            columnsMap.put(colname, dbtype);
        }
        return columnsMap;
    }


//    string Condition = 4;
//    int64 ErrorLimitCount = 5;
//    int32 ErrorLimitPercentage = 6;
    private void openTableForWrite(String tableName, List<String> insertColumns, List<String> matchColumns, List<String> updateColumns, String condition ) {
        // TODO - support multiple formats
        FormatCSV csv = FormatCSV.newBuilder().setNull(config.nullString).setDelimiter(config.delimiter).setQuote(config.csvQuote).setEscape("\"").setHeader(false).build();
        OpenRequest.Builder oReq = OpenRequest.newBuilder()
                .setSession(mSession)
                .setSchemaName(config.dbSchema)
                .setTableName(tableName)
                .setTimeout(30000)
                .setCsv(csv);

        if(config.insertMode == JdbcSinkConfig.InsertMode.MERGE || config.insertMode == JdbcSinkConfig.InsertMode.UPSERT) {
            MergeOption iOpt = MergeOption.newBuilder()
                    .addAllInsertColumns(insertColumns)
                    .addAllUpdateColumns(updateColumns)
                    .addAllMatchColumns(matchColumns)
                    .setCondition(condition)
                    .setErrorLimitCount(config.gpErrorsLimit)
                    .setErrorLimitPercentage(config.gpErrorsPercentageLimit)
                    .build();
            oReq.setMergeOption(iOpt);
        }else if (config.insertMode == JdbcSinkConfig.InsertMode.INSERT) {
            InsertOption iOpt = InsertOption.newBuilder()
                    .addAllInsertColumns(insertColumns)
                    .setErrorLimitCount(config.gpErrorsLimit)
                    .setErrorLimitPercentage(config.gpErrorsPercentageLimit)
                    .build();
            oReq.setInsertOption(iOpt);
        } else if (config.insertMode == JdbcSinkConfig.InsertMode.UPDATE) {
            UpdateOption iOpt = UpdateOption.newBuilder()
                    .addAllUpdateColumns(updateColumns)
                    .addAllMatchColumns(matchColumns)
                    .setCondition(condition)
                    .setErrorLimitCount(config.gpErrorsLimit)
                    .setErrorLimitPercentage(config.gpErrorsPercentageLimit)
                    .build();
            oReq.setUpdateOption(iOpt);

        } else {
            throw new RuntimeException("Unsupported insert mode: " + config.insertMode);
        }

        // use the blocking stub to call the Open service; it returns nothing
        bStub.open(oReq.build());
    }

    private void writeToTable(List<List<String>> data) {
        try {
            List<RowData> rows = new ArrayList<>();
            for (int i = 0; i < data.size(); i++) {
                List<String> row = data.get(i);
                RowData.Builder rowbuilder = RowData.newBuilder().setData(ByteString.copyFromUtf8(String.join(config.delimiter, row)+(i<data.size() ? config.dataLineSeparator:"")));
                rows.add(rowbuilder.build());
            }

            WriteRequest wReq = WriteRequest.newBuilder().setSession(mSession).addAllRows(rows).build();
            // use the blocking stub to call the Write service; it returns nothing
            bStub.write(wReq);
        } catch (Exception e) {
            log.error("Exception while writing to table", e);
            throw new RuntimeException("Exception while writing to table", e);
        }
    }
   private String closeTable() {
        // create a close request builder
        TransferStats tStats = null;
        CloseRequest cReq = CloseRequest.newBuilder()
                .setSession(mSession)
                .build();
        // use the blocking stub to call the Close service
        tStats = bStub.close(cReq);
        return tStats.toString();
    }
}


