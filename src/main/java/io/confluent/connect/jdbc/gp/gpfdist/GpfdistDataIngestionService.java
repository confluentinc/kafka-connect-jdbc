package io.confluent.connect.jdbc.gp.gpfdist;

import io.confluent.connect.jdbc.dialect.DatabaseDialect;
import io.confluent.connect.jdbc.gp.GpDataIngestionService;
import io.confluent.connect.jdbc.gp.gpfdist.framweork.GpfdistSimpleServer;
import io.confluent.connect.jdbc.gp.gpfdist.framweork.GpfdistSinkConfiguration;
import io.confluent.connect.jdbc.gp.gpfdist.framweork.support.GreenplumLoad;
import io.confluent.connect.jdbc.gp.gpfdist.framweork.support.NetworkUtils;
import io.confluent.connect.jdbc.gp.gpfdist.framweork.support.RuntimeContext;
import io.confluent.connect.jdbc.sink.JdbcSinkConfig;
import io.confluent.connect.jdbc.sink.metadata.FieldsMetadata;
import io.confluent.connect.jdbc.sink.metadata.SchemaPair;
import io.confluent.connect.jdbc.util.TableDefinition;
import org.apache.kafka.connect.sink.SinkRecord;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

import static java.util.Collections.emptyList;

public class GpfdistDataIngestionService extends GpDataIngestionService {

    private static final Logger log = LoggerFactory.getLogger(GpfdistDataIngestionService.class);

    public GpfdistDataIngestionService(JdbcSinkConfig config, DatabaseDialect dialect, TableDefinition tabDef, FieldsMetadata fieldsMetadata, SchemaPair schemaPair) {
        super(config, dialect, tabDef, fieldsMetadata, schemaPair);
    }

    public GpfdistDataIngestionService(JdbcSinkConfig config, DatabaseDialect dialect, String tableName, FieldsMetadata fieldsMetadata) {
        super(config, dialect, tableName, fieldsMetadata);
    }

    @Override
    public void ingest(List<SinkRecord> records) {
        super.ingest(records);
        try {
            // convert it as following col1 datatype, col2 datatype, col3 datatype...
            String columnsWithDataType = createColumnNameDataTypeString(",");
            String columns = String.join(",", insertColumnsList); // , added deliberately - don't use config's delimiter
            String externalTableName = "ext_" + tableName + "_" + UUID.randomUUID().toString().replace("-", "_");

            log.info("Ingesting records into external table " + externalTableName);
            GpfdistSinkConfiguration gpfdistSinkConfiguration =
                    new GpfdistSinkConfiguration(config, externalTableName, tableName, columns, columnsWithDataType, keyColumns, nonKeyColumns, "", emptyList(), emptyList());

            GpfdistSimpleServer.getInstance().setRecords(data);
            GreenplumLoad gpload = gpfdistSinkConfiguration.greenplumLoad(dialect);
            gpload.load(getServerContext());

        } catch (Exception e) {
            log.error("Error running gpfdist", e);
        }
    }

    public RuntimeContext getServerContext() {
        final RuntimeContext context = new RuntimeContext(
                NetworkUtils.getGPFDistUri(config.getGpfdistHost(), config.getGpfdistPort()));
        return context;
    }
}
