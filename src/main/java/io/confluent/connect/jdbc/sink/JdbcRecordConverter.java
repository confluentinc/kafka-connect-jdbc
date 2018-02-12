package io.confluent.connect.jdbc.sink;

import org.apache.kafka.connect.sink.SinkRecord;

import java.util.Collection;

public interface JdbcRecordConverter {

    Collection<FixedTableRecord>  convert(SinkRecord sinkRecord);

}
