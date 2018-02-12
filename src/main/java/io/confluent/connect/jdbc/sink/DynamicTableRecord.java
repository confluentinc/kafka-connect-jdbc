package io.confluent.connect.jdbc.sink;

import io.confluent.connect.jdbc.sink.metadata.DefaultFieldsMetadataExtractor;
import io.confluent.connect.jdbc.sink.metadata.MetadataExtractor;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.SinkRecord;

public class DynamicTableRecord implements WriteableRecord{

    public final SinkRecord sinkRecord;

    public DynamicTableRecord(SinkRecord sinkRecord) {
        this.sinkRecord = sinkRecord;
    }

    @Override
    public SinkRecord getRecord() {
        return this.sinkRecord;
    }

    @Override
    public String getTableName(JdbcSinkConfig config) {
        String topic = sinkRecord.topic();
        final String tableName = config.tableNameFormat.replace("${topic}", topic);
        if (tableName.isEmpty()) {
            throw new ConnectException(String.format("Destination table name for topic '%s' is empty using the format string '%s'", topic, config.tableNameFormat));
        }
        return tableName;
    }

    @Override
    public MetadataExtractor getMetadataExtractor() {
        return DefaultFieldsMetadataExtractor.Instance;
    }
}
