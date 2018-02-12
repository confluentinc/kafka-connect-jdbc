package io.confluent.connect.jdbc.sink;

import io.confluent.connect.jdbc.sink.metadata.MetadataExtractor;
import org.apache.kafka.connect.sink.SinkRecord;

public interface WriteableRecord {

    SinkRecord getRecord();

    String getTableName(JdbcSinkConfig config);

    MetadataExtractor getMetadataExtractor();

}
