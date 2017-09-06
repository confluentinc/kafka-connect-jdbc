/**
 * Copyright 2017 TouK.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 **/
package io.confluent.connect.jdbc.source;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Collections;
import java.util.Map;
import org.apache.kafka.connect.source.SourceRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

public class LoggingTableQuerier extends TableQuerier {

  private TableQuerier delegate;

  private int currentResultSetSize = 0;

  private String name;

  private Logger infoLogger = LoggerFactory.getLogger(LoggingTableQuerier.class);

  public LoggingTableQuerier(TableQuerier delegate, String name) {
    super(QueryMode.QUERY, null, null, null, false);
    this.delegate = delegate;
    this.name = name;
  }

  @Override
  public boolean next() throws SQLException {
    boolean next = delegate.next();
    if (next) {
      currentResultSetSize++;
    }
    return next;
  }

  @Override
  public void reset(long now) {
    printLoggingInfo();
    currentResultSetSize = 0;
    delegate.reset(now);
  }

  private void printLoggingInfo() {
    //this should be enough?
    try {
      MDC.clear();
      MDC.put("size", "" + currentResultSetSize);
      MDC.put("name", name);
      offsetAsMap().forEach((key, value) ->
          //it cannot be offset, because it's used e.g. by logstash :|
          MDC.put("offsets." + key, value.toString()));
      infoLogger.info("Import finished");
    } finally {
      MDC.clear();
    }
  }

  //TODO: can it be done in more elegant way?
  private Map<String, Object> offsetAsMap() {
    if (delegate instanceof TimestampIncrementingTableQuerier) {
      return ((TimestampIncrementingTableQuerier) delegate).getOffset().toMap();
    } else {
      return Collections.emptyMap();
    }
  }

  @Override
  public SourceRecord extractRecord() throws SQLException {
    return delegate.extractRecord();
  }

  //methods below simply delegate invocations...
  @Override
  public long getLastUpdate() {
    return delegate.getLastUpdate();
  }

  @Override
  public PreparedStatement getOrCreatePreparedStatement(Connection db) throws SQLException {
    return delegate.getOrCreatePreparedStatement(db);
  }

  @Override
  public boolean querying() {
    return delegate.querying();
  }

  @Override
  public void maybeStartQuery(Connection db) throws SQLException {
    delegate.maybeStartQuery(db);
  }

  @Override
  public int compareTo(TableQuerier other) {
    return delegate.compareTo(other);
  }

  @Override
  protected void createPreparedStatement(Connection db) throws SQLException {
    delegate.createPreparedStatement(db);
  }

  @Override
  protected ResultSet executeQuery() throws SQLException {
    return delegate.executeQuery();
  }

}
