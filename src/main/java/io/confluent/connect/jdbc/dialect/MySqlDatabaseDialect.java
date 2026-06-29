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

package io.confluent.connect.jdbc.dialect;

import io.confluent.connect.jdbc.sink.JdbcSinkConfig;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.connect.data.Date;
import org.apache.kafka.connect.data.Decimal;
import org.apache.kafka.connect.data.Time;
import org.apache.kafka.connect.data.Timestamp;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import org.apache.kafka.connect.errors.ConnectException;

import io.confluent.connect.jdbc.dialect.DatabaseDialectProvider.SubprotocolBasedProvider;
import io.confluent.connect.jdbc.sink.metadata.SinkRecordField;
import io.confluent.connect.jdbc.util.ColumnId;
import io.confluent.connect.jdbc.util.ExpressionBuilder;
import io.confluent.connect.jdbc.util.ExpressionBuilder.Transform;
import io.confluent.connect.jdbc.util.IdentifierRules;
import io.confluent.connect.jdbc.util.JdbcCredentials;
import io.confluent.connect.jdbc.util.TableId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static io.confluent.connect.jdbc.source.JdbcSourceConnectorConfig.CONNECTION_USER_CONFIG;

/**
 * A {@link DatabaseDialect} for MySQL.
 */
public class MySqlDatabaseDialect extends GenericDatabaseDialect {

  private final Logger log = LoggerFactory.getLogger(MySqlDatabaseDialect.class);

  /**
   * MySQL/MariaDB JDBC connection properties that must never be set by tenants.
   *
   * <ul>
   *   <li>{@code allowLoadLocalInfile} / {@code allowLocalInfile} — server-driven
   *       {@code LOAD DATA LOCAL INFILE} reads arbitrary worker files.</li>
   *   <li>{@code allowUrlInLocalInfile} — same mechanism via arbitrary URLs → SSRF.</li>
   *   <li>{@code autoDeserialize} — BLOBs auto-deserialized as Java objects; a malicious
   *       server can craft a gadget chain → RCE (see CVE-2019-2692).</li>
   *   <li>{@code queryInterceptors} / {@code statementInterceptors} — custom interceptor
   *       classes run per query; combined with {@code autoDeserialize} → RCE on connect.</li>
   *   <li>{@code allowMultiQueries} — permits multiple {@code ;}-separated statements,
   *       amplifying any SQL-injection impact.</li>
   * </ul>
   */
  private static final Set<String> MYSQL_BLOCKED_JDBC_PROPERTIES =
      Collections.unmodifiableSet(new HashSet<>(Arrays.asList(
          // file read / SSRF
          "allowLoadLocalInfile",
          "allowLocalInfile",       // MariaDB Connector/J spelling
          "allowUrlInLocalInfile",
          // RCE via Java deserialization
          "autoDeserialize",
          "queryInterceptors",
          "statementInterceptors",  // Connector/J 5.x name for queryInterceptors
          // SQL injection amplification
          "allowMultiQueries"
      )));

  /**
   * Safe values pinned for properties whose defaults are dangerous in old Connector/J 5.x
   * (e.g. allowLoadLocalInfile was true by default). These are applied unconditionally after
   * user-provided connection.* props are processed, so a driver default can never be dangerous
   * even if the blocklist above is somehow bypassed.
   */
  private static final Map<String, String> MYSQL_SAFE_PROPERTY_PINS;
  static {
    Map<String, String> pins = new HashMap<>();
    pins.put("allowLoadLocalInfile", "false");
    pins.put("allowLocalInfile", "false");
    pins.put("allowUrlInLocalInfile", "false");
    pins.put("autoDeserialize", "false");
    MYSQL_SAFE_PROPERTY_PINS = Collections.unmodifiableMap(pins);
  }

  /**
   * The provider for {@link MySqlDatabaseDialect}.
   */
  public static class Provider extends SubprotocolBasedProvider {
    public Provider() {
      super(MySqlDatabaseDialect.class.getSimpleName(), "mariadb", "mysql");
    }

    @Override
    public DatabaseDialect create(AbstractConfig config) {
      return new MySqlDatabaseDialect(config);
    }
  }

  /**
   * Create a new dialect instance with the given connector configuration.
   *
   * @param config the connector configuration; may not be null
   */
  public MySqlDatabaseDialect(AbstractConfig config) {
    super(config, new IdentifierRules(".", "`", "`"));
  }

  @Override
  protected Properties buildAuthenticationProperties(JdbcCredentials jdbcCredentials) {
    Properties properties = new Properties();

    // For Azure MySQL with Entra ID authentication, username is required
    // The provider integration currently returns username as null.
    // If the provider does not return a username, attempt to get it from the connector config
    String username = jdbcCredentials.getUsername();
    if (username == null) {
      username = config.getString(CONNECTION_USER_CONFIG);
    }
    if (username != null) {
      properties.setProperty("user", username);
    }

    // For MySQL, the access token goes in the password field (not a separate property)
    if (jdbcCredentials.getPassword() != null) {
      properties.setProperty("password", jdbcCredentials.getPassword());
    }

    return properties;
  }

  /**
   * Perform any operations on a {@link PreparedStatement} before it is used. This is called from
   * the {@link #createPreparedStatement(Connection, String)} method after the statement is
   * created but before it is returned/used.
   *
   * <p>This method sets the {@link PreparedStatement#setFetchDirection(int) fetch direction}
   * to {@link ResultSet#FETCH_FORWARD forward} as an optimization for the driver to allow it to
   * scroll more efficiently through the result set and prevent out of memory errors.
   *
   * @param stmt the prepared statement; never null
   * @throws SQLException the error that might result from initialization
   */
  @Override
  protected void initializePreparedStatement(PreparedStatement stmt) throws SQLException {
    super.initializePreparedStatement(stmt);

    log.trace(
        "Initializing PreparedStatement fetch direction to FETCH_FORWARD for '{}'",
        shouldRedactSensitiveLogs(stmt.toString()));
    stmt.setFetchDirection(ResultSet.FETCH_FORWARD);
  }

  @Override
  protected String getSqlType(SinkRecordField field) {
    if (field.schemaName() != null) {
      switch (field.schemaName()) {
        case Decimal.LOGICAL_NAME:
          // Maximum precision supported by MySQL is 65
          int scale = Integer.parseInt(field.schemaParameters().get(Decimal.SCALE_FIELD));
          return "DECIMAL(65," + scale + ")";
        case Date.LOGICAL_NAME:
          return "DATE";
        case Time.LOGICAL_NAME:
          return "TIME(3)";
        case Timestamp.LOGICAL_NAME:
          return "DATETIME(3)";
        default:
          // pass through to primitive types
      }
    }
    switch (field.schemaType()) {
      case INT8:
        return "TINYINT";
      case INT16:
        return "SMALLINT";
      case INT32:
        return "INT";
      case INT64:
        if (config instanceof JdbcSinkConfig
            && config.getList(JdbcSinkConfig.TIMESTAMP_FIELDS_LIST).contains(field.name())) {
          return "DATETIME(6)";
        }
        return "BIGINT";
      case FLOAT32:
        return "FLOAT";
      case FLOAT64:
        return "DOUBLE";
      case BOOLEAN:
        return "TINYINT";
      case STRING:
        if (config instanceof JdbcSinkConfig
            && config.getList(JdbcSinkConfig.TIMESTAMP_FIELDS_LIST).contains(field.name())) {
          return "DATETIME(6)";
        }
        return "TEXT";
      case BYTES:
        return "VARBINARY(1024)";
      default:
        return super.getSqlType(field);
    }
  }

  @Override
  public String buildUpsertQueryStatement(
      TableId table,
      Collection<ColumnId> keyColumns,
      Collection<ColumnId> nonKeyColumns
  ) {
    //MySql doesn't support SQL 2003:merge so here how the upsert is handled
    final Transform<ColumnId> transform = (builder, col) -> {
      builder.appendColumnName(col.name());
      builder.append("=values(");
      builder.appendColumnName(col.name());
      builder.append(")");
    };

    ExpressionBuilder builder = expressionBuilder();
    builder.append("insert into ");
    builder.append(table);
    builder.append("(");
    builder.appendList()
        .delimitedBy(",")
        .transformedBy(ExpressionBuilder.columnNames())
        .of(keyColumns, nonKeyColumns);
    builder.append(") values(");
    builder.appendMultiple(",", "?", keyColumns.size() + nonKeyColumns.size());
    builder.append(") on duplicate key update ");
    builder.appendList()
        .delimitedBy(",")
        .transformedBy(transform)
        .of(nonKeyColumns.isEmpty() ? keyColumns : nonKeyColumns);
    return builder.toString();
  }

  @Override
  protected Set<String> getBlockedJdbcConnectionProperties() {
    return MYSQL_BLOCKED_JDBC_PROPERTIES;
  }

  @Override
  protected Properties addConnectionProperties(Properties properties) {
    // Super checks user-provided connection.* props against the blocklist and passes safe ones.
    Properties result = super.addConnectionProperties(properties);
    // Pin safe values last — guards against old Connector/J 5.x driver defaults
    // (allowLoadLocalInfile was true by default pre-8.x) even if the URL or blocklist
    // is somehow bypassed. URL params take precedence over Properties, so validateJdbcUrlParams
    // must reject blocked params in URLs; these pins cover the Properties vector only.
    MYSQL_SAFE_PROPERTY_PINS.forEach(result::setProperty);
    return result;
  }

  /**
   * Rejects MySQL JDBC URLs that carry blocked query-string parameters or URL metacharacters
   * in the host/authority portion.
   *
   * <p>MySQL Connector/J uses {@code ?key=value&key=value} syntax after the database name.
   * Injecting {@code allowLoadLocalInfile=true} here lets a malicious server issue
   * {@code LOAD DATA LOCAL INFILE} requests and read arbitrary files from the worker.
   *
   * <p>An attacker may also inject metacharacters directly into {@code connection.host} so that
   * the assembled URL {@code jdbc:mysql://<host>:<port>/<db>} smuggles extra parameters.
   * We reject any {@code &} that appears before the {@code ?} query separator, and any {@code #}
   * fragment identifier, neither of which has a legitimate place in a MySQL JDBC URL.
   */
  @Override
  protected void validateJdbcUrlParams(String url) {
    int queryStart = url.indexOf('?');
    // & must not appear before the query-string delimiter — that would mean a metacharacter
    // was injected into the host/path portion (e.g. connection.host="host&allowLoadLocalInfile=true")
    String preQuery = queryStart >= 0 ? url.substring(0, queryStart) : url;
    if (preQuery.indexOf('&') >= 0) {
      throw new ConnectException(
          "JDBC URL contains '&' outside of the query-string — possible metacharacter injection "
              + "in connection host.");
    }
    // Fragment identifiers (#) are never valid in JDBC URLs
    if (url.indexOf('#') >= 0) {
      throw new ConnectException(
          "JDBC URL contains invalid character '#' which is not permitted.");
    }
    if (queryStart == -1) {
      return;
    }
    String queryString = url.substring(queryStart + 1);
    for (String param : queryString.split("&")) {
      if (param.isEmpty()) {
        continue;
      }
      String key = param.contains("=") ? param.substring(0, param.indexOf('=')) : param;
      if (MYSQL_BLOCKED_JDBC_PROPERTIES.contains(key)) {
        throw new ConnectException(
            "JDBC URL contains blocked connection parameter '" + key
                + "' which is not permitted for security reasons.");
      }
    }
  }

  @Override
  protected String sanitizedUrl(String url) {
    // MySQL can also have "username:password@" at the beginning of the host list and
    // in parenthetical properties
    return super.sanitizedUrl(url)
        .replaceAll("(?i)([(,]password=)[^,)]*", "$1****")
        .replaceAll("(://[^:]*:)([^@]*)@", "$1****@");
  }

  @Override
  public String resolveSynonym(Connection connection, String synonymName) throws SQLException {
    throw new SQLException("MySQL does not support synonyms. Please use views instead.");
  }

}
