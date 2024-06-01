/*
 * Copyright 2018 Confluent Inc.
 *
 * Licensed under the Confluent Community License (the "License"); you may not use
 * this file except in compliance with the License.  You may obtain a copy of the
 * License at
 *
 * http://www.confluent.io/confluent-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OF ANY KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations under the License.
 */

package io.confluent.connect.jdbc.source;

import com.mockrunner.mock.jdbc.MockResultSet;
import com.mockrunner.mock.jdbc.MockResultSetMetaData;
import oracle.jdbc.internal.OracleCallableStatement;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.source.SourceRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Collections;
import java.util.Map;

import io.confluent.connect.jdbc.dialect.DatabaseDialect;
import io.confluent.connect.jdbc.source.SchemaMapping.FieldSetter;
import io.confluent.connect.jdbc.util.ExpressionBuilder;

/**
 * BulkTableQuerier always returns the entire table.
 */
public class BulkTableQuerier extends TableQuerier {
  private static final Logger log = LoggerFactory.getLogger(BulkTableQuerier.class);
  private static final String RECORDSET_NAME = "Usage Data Record";
  private static final String STORED_PROCEDURE_OUT_PARAMETER = "udr";

  public BulkTableQuerier(
      DatabaseDialect dialect,
      QueryMode mode,
      String name,
      String topicPrefix,
      String suffix
  ) {
    super(dialect, mode, name, topicPrefix, suffix);
  }

  @Override
  protected void createPreparedStatement(Connection db) throws SQLException {
    log.trace("storedProcedure is: {}", storedProcedure);
    ExpressionBuilder builder = dialect.expressionBuilder();
    builder = builder
            .append("{CALL ")
            .append(storedProcedure)
            .append("(?, ?)}");

    String queryStr = builder.toString();

    log.trace("{} prepared SQL query: {}", this, queryStr);
    recordQuery(queryStr);
    stmt = dialect.createPreparedCall(db, queryStr);
  }

  @Override
  protected ResultSet executeQuery() throws SQLException {
    stmt.execute();
    Clob clob = ((OracleCallableStatement)stmt).getClob(2);
    String value = clob.getSubString(1, (int) clob.length());
    log.trace(value);

    MockResultSet mockResultSet = new MockResultSet(RECORDSET_NAME);
    mockResultSet.addRow(Collections.singletonList(value));

    MockResultSetMetaData mockResultSetMetaData = new MockResultSetMetaData();
    mockResultSetMetaData.setColumnCount(1);
    mockResultSetMetaData.setColumnName(1, STORED_PROCEDURE_OUT_PARAMETER);
    mockResultSetMetaData.setColumnType(1, 12); //Varchar
    mockResultSet.setResultSetMetaData(mockResultSetMetaData);

    return mockResultSet;
  }

  @Override
  public SourceRecord extractRecord() throws SQLException {
    Struct record = new Struct(schemaMapping.schema());
    for (FieldSetter setter : schemaMapping.fieldSetters()) {
      try {
        setter.setField(record, resultSet);
      } catch (IOException e) {
        log.warn("Error mapping fields into Connect record", e);
        throw new ConnectException(e);
      } catch (SQLException e) {
        log.warn("SQL error mapping fields into Connect record", e);
        throw new DataException(e);
      }
    }

    final String topic;
    final Map<String, String> partition;
    partition = Collections.singletonMap(
            JdbcSourceConnectorConstants.STORED_PROCEDURE,
            storedProcedure);
    topic = topicPrefix;

    return new SourceRecord(partition, null, topic, record.schema(), record);
  }

  @Override
  public String toString() {
    return "StoredProcedureQuerier{" + "storedProcedure='" + storedProcedure
            + ", topicPrefix='" + topicPrefix + '\'' + '}';
  }

}
