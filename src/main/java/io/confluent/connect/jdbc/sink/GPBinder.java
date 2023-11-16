package io.confluent.connect.jdbc.sink;

import io.confluent.connect.jdbc.dialect.DatabaseDialect;
import io.confluent.connect.jdbc.dialect.DatabaseDialect.StatementBinder;
import io.confluent.connect.jdbc.gp.GPDataIngestor;
import io.confluent.connect.jdbc.gp.gpload.GPLoadDataIngestor;
import io.confluent.connect.jdbc.gp.gpss.GPSSDataIngestor;
import io.confluent.connect.jdbc.sink.metadata.FieldsMetadata;
import io.confluent.connect.jdbc.sink.metadata.SchemaPair;
import io.confluent.connect.jdbc.util.TableDefinition;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class GPBinder implements StatementBinder {

    private final JdbcSinkConfig.PrimaryKeyMode pkMode;
    private final SchemaPair schemaPair;
    private final FieldsMetadata fieldsMetadata;
    private final JdbcSinkConfig.InsertMode insertMode;
    private final DatabaseDialect dialect;
    private final TableDefinition tabDef;
    private final JdbcSinkConfig config;

    private List<Map<String, Object>> dataRows;

    private List<SinkRecord> records;

    private GPDataIngestor gpDataIngestor;

    private static final Logger log = LoggerFactory.getLogger(GPBinder.class);

    @Deprecated
    public GPBinder(
            DatabaseDialect dialect,
            JdbcSinkConfig.PrimaryKeyMode pkMode,
            SchemaPair schemaPair,
            FieldsMetadata fieldsMetadata,
            JdbcSinkConfig.InsertMode insertMode
    ) {
        this(
                dialect,
                pkMode,
                schemaPair,
                fieldsMetadata,
                null,
                insertMode,
                null
        );
    }

    public GPBinder(
            DatabaseDialect dialect,
            JdbcSinkConfig.PrimaryKeyMode pkMode,
            SchemaPair schemaPair,
            FieldsMetadata fieldsMetadata,
            TableDefinition tabDef,
            JdbcSinkConfig.InsertMode insertMode,
            JdbcSinkConfig config
    ) {
        this.dialect = dialect;
        this.pkMode = pkMode;
        this.schemaPair = schemaPair;
        this.fieldsMetadata = fieldsMetadata;
        this.insertMode = insertMode;
        this.tabDef = tabDef;
        this.dataRows = new ArrayList<>();
        this.records = new ArrayList<>();
        this.config = config;

        if (config.batchInsertMode == JdbcSinkConfig.BatchInsertMode.GPLOAD) {
            log.info("Using GPLOAD to insert records");
            gpDataIngestor = new GPLoadDataIngestor(config, tabDef, this.fieldsMetadata );

        } else if (config.batchInsertMode == JdbcSinkConfig.BatchInsertMode.GPSS) {
            log.info("Using GPSS to insert records");
              gpDataIngestor = new GPSSDataIngestor(config, tabDef, this.fieldsMetadata );
        }
    }

    @Override
    public void bindRecord(SinkRecord record) throws SQLException {
        final Struct valueStruct = (Struct) record.value();
        final boolean isDelete = valueStruct == null;

        if (!isDelete) { // TODO pending delete case and update case

//            Map<String, Object> row = new HashMap<>();
//            tabDef.columnNames().forEach(field -> {
//                row.put(field, valueStruct.get(field));
//            });
//            dataRows.add(row);
            records.add(record);
        }else {
            log.info("Ignoring delete record {}", record);
        }
    }

    public void flush() {
        log.info("Flushing {} records", records.size());
        if (records.isEmpty()) {
            log.info("No records to flush");
            return;
        }

        gpDataIngestor.ingest(records);

    }

}