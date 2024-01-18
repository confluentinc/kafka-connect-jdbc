package io.confluent.connect.jdbc.sink;

import com.jayway.jsonpath.JsonPath;
import io.confluent.connect.jdbc.dialect.DatabaseDialect;
import io.confluent.connect.jdbc.util.QueryUtils;
import org.apache.kafka.connect.sink.SinkRecord;

import java.sql.BatchUpdateException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static java.util.Objects.nonNull;

public class BufferedQueryRecords implements BufferedPlainRecords {

  private final JdbcSinkConfig config;
  private final RecordValidator recordValidator;
  private final PreparedStatement queryPreparedStatement;
  private List<SinkRecord> records = new ArrayList<>();
  private final Map.Entry<String, Map<Integer, String>> queryMapping;

  public BufferedQueryRecords(JdbcSinkConfig config, Connection connection, DatabaseDialect dbDialect) {
    this.config = config;
    this.recordValidator = RecordValidator.create(config);
    this.queryMapping = QueryUtils.parseQuery(config.query);
    try {
      this.queryPreparedStatement = dbDialect.createPreparedStatement(connection, this.queryMapping.getKey());
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public List<SinkRecord> add(SinkRecord record) throws SQLException, TableAlterOrCreateException {
    recordValidator.validate(record);
    records.add(record);
    if (records.size() >= config.batchSize) {
      return flush();
    }
    return Collections.emptyList();
  }

  @Override
  public List<SinkRecord> flush() throws SQLException {
    for (SinkRecord record : records) {
      String json = QueryUtils.toJson(record);
      Map<Integer, String> mapping = queryMapping.getValue();
      for (Map.Entry<Integer, String> entry : mapping.entrySet()) {
        Object value = null;
        try {
          value = JsonPath.read(json, entry.getValue());
        } catch (Exception ignored) {
          // ignored
        }
        queryPreparedStatement.setObject(entry.getKey(), value);
      }
      queryPreparedStatement.addBatch();
    }
    executeUpdates();
    final List<SinkRecord> flushedRecords = records;
    records = new ArrayList<>();
    return flushedRecords;
  }

  private void executeUpdates() throws SQLException {
    int[] batchStatus = queryPreparedStatement.executeBatch();
    for (int updateCount : batchStatus) {
      if (updateCount == Statement.EXECUTE_FAILED) {
        throw new BatchUpdateException(
            "Execution failed for part of the batch update", batchStatus);
      }
    }
  }

  @Override
  public void close() throws SQLException {
    if (nonNull(queryPreparedStatement)) {
      queryPreparedStatement.close();
    }
  }
}
