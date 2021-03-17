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

package io.confluent.connect.jdbc.sink;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Collection;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Arrays;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collector;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.confluent.connect.jdbc.dialect.DatabaseDialect;
import io.confluent.connect.jdbc.util.CachedConnectionProvider;
import io.confluent.connect.jdbc.util.TableId;

public class JdbcDbWriter {
  private static final Logger log = LoggerFactory.getLogger(JdbcDbWriter.class);

  private static final List<String> ignoreQuery = Arrays.asList(
    "rename table"
  );

  private static final List<String> ddlKeywords = Arrays.asList(
    "add ", "modify ", "change ", "rename ", "drop "
  );

  private static final List<String> blacklistedQueries = Arrays.asList(
    "add index",
    "add unique key",
    "add unique index",
    "add unique"
  );

  private static final List<String> whitelistedQueries = Arrays.asList(
    "add column",
    "drop column",
    "modify column",
    "add ",
    "change column",
    "change ",
    "rename column"
  );

  private static Pattern alterTablePattern = Pattern.compile("ALTER TABLE (\\S+)", Pattern.CASE_INSENSITIVE);

  private final JdbcSinkConfig config;
  private final DatabaseDialect dbDialect;
  private final DbStructure dbStructure;
  final CachedConnectionProvider cachedConnectionProvider;

  JdbcDbWriter(final JdbcSinkConfig config, DatabaseDialect dbDialect, DbStructure dbStructure) {
    this.config = config;
    this.dbDialect = dbDialect;
    this.dbStructure = dbStructure;

    this.cachedConnectionProvider = connectionProvider(
        config.connectionAttempts,
        config.connectionBackoffMs
    );
  }

  protected CachedConnectionProvider connectionProvider(int maxConnAttempts, long retryBackoff) {
    return new CachedConnectionProvider(this.dbDialect, maxConnAttempts, retryBackoff) {
      @Override
      protected void onConnect(final Connection connection) throws SQLException {
        log.info("JdbcDbWriter Connected");
        connection.setAutoCommit(false);
      }
    };
  }

  void write(final Collection<SinkRecord> records) throws SQLException {
    final Connection connection = cachedConnectionProvider.getConnection();

    final Map<TableId, BufferedRecords> bufferByTable = new HashMap<>();
    for (SinkRecord record : records) {

      // apply ddl statements
      if (applyDdlStatementsIfApplicable(record, connection)) {
        // skip this sink record from buffer
        continue;
      }

      final TableId tableId = destinationTable(record.topic());
      BufferedRecords buffer = bufferByTable.get(tableId);
      if (buffer == null) {
        buffer = new BufferedRecords(config, tableId, dbDialect, dbStructure, connection);
        bufferByTable.put(tableId, buffer);
      }
      buffer.add(record);
    }
    for (Map.Entry<TableId, BufferedRecords> entry : bufferByTable.entrySet()) {
      TableId tableId = entry.getKey();
      BufferedRecords buffer = entry.getValue();
      log.debug("Flushing records in JDBC Writer for table ID: {}", tableId);
      buffer.flush();
      buffer.close();
    }
    connection.commit();
  }

  void closeQuietly() {
    cachedConnectionProvider.close();
  }

  TableId destinationTable(String topic) {
    final String tableName = config.tableNameFormat.replace("${topic}", topic);
    if (tableName.isEmpty()) {
      throw new ConnectException(String.format(
          "Destination table name for topic '%s' is empty using the format string '%s'",
          topic,
          config.tableNameFormat
      ));
    }
    return dbDialect.parseTableIdentifier(tableName);
  }

  boolean applyDdlStatementsIfApplicable(SinkRecord record, Connection connection) {
    if (record.valueSchema() == null) {
      return false;
    }

    String valueSchemaName = record.valueSchema().name();

    if ((valueSchemaName != null) && (valueSchemaName.equalsIgnoreCase("io.debezium.connector.mysql.SchemaChangeValue"))) {
      Struct valueStruct = (Struct) record.value();
      String ddl = valueStruct.get("ddl").toString().trim();
      
      log.info("DDL Statement: {}", ddl);

      if (ddl.toLowerCase().startsWith("create") || ddl.toLowerCase().startsWith("alter")) {
        try {
          Statement stmt = connection.createStatement();
          for (String s : getSplitDdlStatements(ddl)) {
            stmt.executeQuery(s);
          }
          stmt.close();
        } catch(java.sql.SQLException e) {
          log.error("Failed to apply DDL statement {}: {}", ddl, e);
        }
      }

      // return true since we also need to skip other ddl statements present in this topic
      return true;
    }

    return false;
  }

  List<String> getSplitDdlStatements(String ddlStatement) {
    List<String> ddlStatements = new ArrayList<String>();

    if (ignoreQuery.stream().anyMatch(ddlStatement.toLowerCase()::contains)) {
      return ddlStatements;
    }

    List<String> splitQueries = splitDdlStatement(ddlStatement);
    if (ddlStatement.toLowerCase().startsWith("alter")) {
      String suffix = "ALTER TABLE " + getTableNameFromDdlStatement(ddlStatement);

      // remove all blacklisted queries
      List<String> filteredQueries = splitQueries.stream()
        .filter((q) -> !blacklistedQueries.stream().anyMatch(q.toLowerCase()::contains) && whitelistedQueries.stream().anyMatch(q.toLowerCase()::contains))
        .collect(Collectors.toList());
        
      if (filteredQueries.size() > 0) {
        ddlStatements.add(filteredQueries.get(0));
        ddlStatements.addAll(filteredQueries.stream().skip(1).map((q) -> String.format("%s %s", suffix, q)).collect(Collectors.toList()));
      }
    } else if (ddlStatement.toLowerCase().startsWith("create table")) {
      String updatedDdlStatement = ddlStatement.replaceFirst("(?i)create table( if not exists)*", "create table if not exists");
      ddlStatements.add(updatedDdlStatement);
    }

    return ddlStatements;
  }

  List<String> splitDdlStatement(String ddlStatement) {
    List<String> queries = new ArrayList<String>();
    
    String[] splitQueries = ddlStatement.split(",");

    if (splitQueries.length > 0) {
      int idx = 0;
      while(idx < splitQueries.length) {
        int updatedPos = idx + 1;
        String query = splitQueries[idx];

        while(updatedPos < splitQueries.length && !ddlKeywords.stream().anyMatch(splitQueries[updatedPos].toLowerCase()::contains)) {
          query = String.join(",", Arrays.asList(query, splitQueries[updatedPos]));
          updatedPos++;
        }
        idx = updatedPos;
        queries.add(query);
      }
    }

    return queries;
  }

  String getTableNameFromDdlStatement(String ddlStatement) {
    String tableName = "";

    Matcher matcher = alterTablePattern.matcher(ddlStatement);
    if (matcher.find()) {
      tableName = matcher.group(1);
    }

    return tableName;
  }
}
