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

import java.net.URLDecoder;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.TreeSet;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

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
   * Stored in a case-insensitive set because MySQL Connector/J normalises property names
   * via {@code PropertyKey.normalizeCase()} before applying them, so {@code ALLOWLOADLOCALINFILE}
   * and {@code allowLoadLocalInfile} are treated identically by the driver.
   *
   * <p><b>Currently-exploitable properties:</b>
   * <ul>
   *   <li>{@code allowLoadLocalInfile} / {@code allowLocalInfile} — server-driven
   *       {@code LOAD DATA LOCAL INFILE} reads arbitrary worker files.</li>
   *   <li>{@code allowUrlInLocalInfile} — same mechanism via arbitrary URLs → SSRF.</li>
   *   <li>{@code allowLoadLocalInfileInPath} — path-restricted variant;
   *       setting to {@code /} is equivalent to {@code allowLoadLocalInfile=true}.</li>
   *   <li>{@code autoDeserialize} — BLOBs auto-deserialized as Java objects; a malicious
   *       server can craft a gadget chain → RCE (see CVE-2019-2692).</li>
   *   <li>{@code queryInterceptors} / {@code statementInterceptors} — custom interceptor
   *       classes loaded per query; combined with {@code autoDeserialize} → RCE on connect.</li>
   *   <li>{@code allowMultiQueries} — permits multiple {@code ;}-separated statements,
   *       amplifying any SQL-injection impact.</li>
   * </ul>
   *
   * <p><b>Class-loading properties (defence-in-depth):</b> not weaponisable on
   * mysql-connector-j 8.0.33 (driver uses {@code forName(initialize=false)} +
   * {@code isAssignableFrom} before construction; no gadget on the worker classpath),
   * but a future Connector/J upgrade or new classpath dependency could change that.
   * Blocked now so the denylist does not silently regress.
   * <ul>
   *   <li>{@code socketFactory} — arbitrary socket factory class → SSRF / traffic
   *       interception.</li>
   *   <li>{@code authenticationPlugins} / {@code defaultAuthenticationPlugin} — custom auth
   *       class loaded on every connect.</li>
   *   <li>{@code clientInfoProvider} — custom client-info class loaded on connect.</li>
   *   <li>{@code propertiesTransform} — class that rewrites all connection properties before
   *       they are applied; could undo safe-value pins.</li>
   *   <li>{@code serverRSAPublicKeyFile} — file path read by the driver (file-read vector).</li>
   * </ul>
   */
  private static final Set<String> MYSQL_BLOCKED_JDBC_PROPERTIES;

  static {
    // TreeSet with CASE_INSENSITIVE_ORDER so blocked.contains() matches regardless of casing
    Set<String> set = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);
    // Known Connector/J aliases for each blocked property are listed explicitly.
    // Connector/J also has a PropertyKey enum that can canonicalise unknown aliases; if
    // new aliases are discovered in a future driver version, resolve them through that enum.
    set.addAll(Arrays.asList(
        // file read / SSRF
        "allowLoadLocalInfile",
        "allowLocalInfile",             // MariaDB Connector/J spelling
        "allowUrlInLocalInfile",
        "allowLoadLocalInfileInPath",   // path-restricted variant; / == full access
        // RCE via Java deserialization
        "autoDeserialize",
        "queryInterceptors",
        "statementInterceptors",        // Connector/J 5.x name for queryInterceptors
        // SQL injection amplification
        "allowMultiQueries",
        // class-loading / file-read (defence-in-depth; not weaponisable today but
        // could regress with a Connector/J upgrade or new worker classpath entry)
        "socketFactory",
        "authenticationPlugins",
        "defaultAuthenticationPlugin",
        "clientInfoProvider",
        "propertiesTransform",
        "serverRSAPublicKeyFile"
    ));
    MYSQL_BLOCKED_JDBC_PROPERTIES = Collections.unmodifiableSet(set);
  }

  // Matches parenthetical property bags in the MySQL JDBC URL authority section:
  // jdbc:mysql://(host=h,port=p,allowLoadLocalInfile=true)/db
  // jdbc:mysql://[(host=h,...),(host=h2,...)]/db
  private static final Pattern AUTHORITY_PROPERTY_BAG_PATTERN = Pattern.compile("\\(([^)]+)\\)");

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
    // Pin safe values last. Connector/J applies Properties AFTER URL params, so these pins
    // take effect even when a dangerous property appears in the URL (defence-in-depth on top
    // of validateJdbcUrlParams). Also guards against old Connector/J 5.x driver defaults
    // (allowLoadLocalInfile defaulted to true pre-8.x).
    MYSQL_SAFE_PROPERTY_PINS.forEach(result::setProperty);
    return result;
  }

  /**
   * Rejects MySQL JDBC URLs that carry blocked connection parameters, covering all three
   * locations where MySQL Connector/J accepts properties:
   *
   * <ol>
   *   <li><b>Query string</b>: {@code jdbc:mysql://host/db?allowLoadLocalInfile=true}</li>
   *   <li><b>Authority property bags</b>:
   *       {@code jdbc:mysql://(host=h,allowLoadLocalInfile=true)/db}</li>
   *   <li><b>Host metacharacter injection</b>: {@code connection.host="h&prop=v"} produces
   *       {@code &} before {@code ?} in the assembled URL.</li>
   * </ol>
   *
   * <p>Keys are URL-decoded and compared case-insensitively because MySQL Connector/J normalises
   * property names via {@code PropertyKey.normalizeCase()} and decodes {@code %xx} sequences
   * before applying them — so {@code ALLOWLOADLOCALINFILE} and {@code allow%4CoadLocalInfile}
   * are treated identically by the driver.
   */
  @Override
  protected void validateJdbcUrlParams(String url) {
    if (url == null) {
      return;
    }
    // # is never valid in a JDBC URL
    if (url.indexOf('#') >= 0) {
      throw new ConnectException(
          "JDBC URL contains invalid character '#' which is not permitted.");
    }

    int queryStart = url.indexOf('?');
    String preQuery = queryStart >= 0 ? url.substring(0, queryStart) : url;

    // & before ? means a metacharacter was injected into the host/path portion
    if (preQuery.indexOf('&') >= 0) {
      throw new ConnectException(
          "JDBC URL contains '&' outside of the query-string — "
              + "possible metacharacter injection in connection host.");
    }

    checkAuthorityBags(preQuery);
    if (queryStart >= 0) {
      checkQueryString(url.substring(queryStart + 1));
    }
  }

  private void checkAuthorityBags(String preQuery) {
    Matcher bagMatcher = AUTHORITY_PROPERTY_BAG_PATTERN.matcher(preQuery);
    while (bagMatcher.find()) {
      for (String prop : bagMatcher.group(1).split(",")) {
        int eq = prop.indexOf('=');
        if (eq > 0) {
          checkBlockedKey(prop.substring(0, eq).trim());
        }
      }
    }
  }

  private void checkQueryString(String queryString) {
    for (String param : queryString.split("&")) {
      if (param.isEmpty()) {
        continue;
      }
      int eq = param.indexOf('=');
      checkBlockedKey(eq > 0 ? param.substring(0, eq) : param);
    }
  }

  /**
   * Normalises a raw JDBC property key (URL-decoded, trimmed, lower-cased) and throws
   * {@link ConnectException} if it names a blocked property.
   */
  private void checkBlockedKey(String rawKey) {
    String key;
    try {
      key = URLDecoder.decode(rawKey.trim(), "UTF-8");
    } catch (Exception e) {
      key = rawKey.trim();
    }
    if (MYSQL_BLOCKED_JDBC_PROPERTIES.contains(key)) {
      throw new ConnectException(
          "JDBC URL contains blocked connection parameter '" + rawKey.trim()
              + "' which is not permitted for security reasons.");
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
