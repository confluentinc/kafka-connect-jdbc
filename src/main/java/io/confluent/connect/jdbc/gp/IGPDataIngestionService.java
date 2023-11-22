package io.confluent.connect.jdbc.gp;

import org.apache.kafka.connect.sink.SinkRecord;

import java.util.List;

public interface IGPDataIngestionService {
    void ingest(List<SinkRecord> records);

}
