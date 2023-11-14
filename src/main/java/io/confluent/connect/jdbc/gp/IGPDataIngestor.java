package io.confluent.connect.jdbc.gp;

import org.apache.kafka.connect.sink.SinkRecord;

import java.util.List;

public interface IGPDataIngestor {
    void ingest(List<SinkRecord> records);

}
