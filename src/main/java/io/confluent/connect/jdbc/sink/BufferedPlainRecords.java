package io.confluent.connect.jdbc.sink;

import org.apache.kafka.connect.sink.SinkRecord;

import java.sql.SQLException;
import java.util.List;

public interface BufferedPlainRecords {

    List<SinkRecord> add(SinkRecord record) throws SQLException, TableAlterOrCreateException;

    List<SinkRecord> flush() throws SQLException;

    void close() throws SQLException;

}
