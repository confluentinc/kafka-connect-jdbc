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

import io.confluent.connect.jdbc.data.Json;
import io.confluent.connect.jdbc.data.VariableScaleDecimal;
import io.confluent.connect.jdbc.data.ZonedTimestamp;
import io.confluent.connect.jdbc.dialect.DatabaseDialectProvider.SubprotocolBasedProvider;
import io.confluent.connect.jdbc.sink.JdbcSinkConfig;
import io.confluent.connect.jdbc.sink.metadata.SinkRecordField;
import io.confluent.connect.jdbc.source.ColumnMapping;
import io.confluent.connect.jdbc.util.ColumnDefinition;
import io.confluent.connect.jdbc.util.ColumnId;
import io.confluent.connect.jdbc.util.DateTimeUtils;
import io.confluent.connect.jdbc.util.ExpressionBuilder;
import io.confluent.connect.jdbc.util.ExpressionBuilder.Transform;
import io.confluent.connect.jdbc.util.IdentifierRules;
import io.confluent.connect.jdbc.util.JdbcCredentials;
import io.confluent.connect.jdbc.util.JsonConverter;
import io.confluent.connect.jdbc.util.QuoteMethod;
import io.confluent.connect.jdbc.util.TableDefinition;
import io.confluent.connect.jdbc.util.TableId;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.connect.data.Date;
import org.apache.kafka.connect.data.Decimal;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.data.Time;
import org.apache.kafka.connect.data.Timestamp;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.errors.DataException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.sql.Array;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;

import static io.confluent.connect.jdbc.source.JdbcSourceConnectorConfig.CONNECTION_USER_CONFIG;

/**
 * A {@link DatabaseDialect} for PostgreSQL.
 */
public class PostgreSqlDatabaseDialect extends GenericDatabaseDialect {

  private static final Logger log = LoggerFactory.getLogger(PostgreSqlDatabaseDialect.class);

  // Visible for testing
  volatile int maxIdentifierLength = 0;

  /**
   * The provider for {@link PostgreSqlDatabaseDialect}.
   */
  public static class Provider extends SubprotocolBasedProvider {
    public Provider() {
      super(PostgreSqlDatabaseDialect.class.getSimpleName(), "postgresql");
    }

    @Override
    public DatabaseDialect create(AbstractConfig config) {
      return new PostgreSqlDatabaseDialect(config);
    }
  }

  static final String JSON_TYPE_NAME = "json";
  static final String JSONB_TYPE_NAME = "jsonb";
  static final String UUID_TYPE_NAME = "uuid";
  static final String NUMERIC_TYPE_NAME = "numeric";
  static final String DATE_TYPE_NAME = "date";
  static final String TIME_TYPE_NAME = "time";
  static final String TIMESTAMP_TYPE_NAME = "timestamp";
  static final String TIMESTAMPTZ_TYPE_NAME = "timestamptz";
  static final String HSTORE_TYPE_NAME = "hstore";

  private static final String MULTI_DIMENSIONAL_ARRAY_WARNING =
      "Skipping unsupported multi-dimensional array at column index {}; only single-dimension "
          + "arrays are supported";

  /**
   * Define the PG datatypes that require casting upon insert/update statements.
   */
  private static final Set<String> CAST_TYPES = Collections.unmodifiableSet(
      new HashSet<>(Arrays.asList(
          JSON_TYPE_NAME,
          JSONB_TYPE_NAME,
          UUID_TYPE_NAME
      ))
  );

  /**
   * Create a new dialect instance with the given connector configuration.
   *
   * @param config the connector configuration; may not be null
   */
  public PostgreSqlDatabaseDialect(AbstractConfig config) {
    super(config, new IdentifierRules(".", "\"", "\""));
  }

  @Override
  protected Properties buildAuthenticationProperties(JdbcCredentials jdbcCredentials) {
    Properties properties = new Properties();

    // For Azure PostgreSQL with Entra ID authentication, username is required
    // If username is null, use provider.integration.id (Client/Application ID)
    String username = jdbcCredentials.getUsername();
    if (username == null) {
      username = config.getString(CONNECTION_USER_CONFIG);
    }
    if (username != null) {
      properties.setProperty("user", username);
    }

    // For PostgreSQL, the access token goes in the password field (not a separate property)
    if (jdbcCredentials.getPassword() != null) {
      properties.setProperty("password", jdbcCredentials.getPassword());
    }

    return properties;
  }

  /**
   * {@inheritDoc}
   *
   * <p>A PostgreSQL connection sees exactly one database, so the catalog the driver reports
   * only echoes the connected database and adds no information. pgjdbc 42.7.5+ reports that
   * name where older drivers returned {@code null}, which turned discovered identifiers into
   * three-part {@code db.schema.table} names — breaking two-part {@code table.include.list}
   * matching and changing source-offset partition keys. Dropping the driver-reported catalog
   * keeps discovered identifiers two-part on any driver version. Configured catalogs are not
   * affected: they arrive via {@link #parseTableIdentifier(String)}, which does not route
   * through this seam, so a user-supplied database in {@code table.name.format} is preserved.
   */
  @Override
  protected TableId createTableId(String catalogName, String schemaName, String tableName) {
    return new TableId(null, schemaName, tableName);
  }

  @Override
  public String resolveSynonym(Connection connection, String synonymName) throws SQLException {
    throw new SQLException("PostgreSQL does not support synonyms. Please use views instead.");
  }

  @Override
  public Connection getConnection() throws SQLException {
    Connection result = super.getConnection();
    synchronized (this) {
      if (maxIdentifierLength <= 0) {
        maxIdentifierLength = computeMaxIdentifierLength(result);
      }
    }
    return result;
  }

  static int computeMaxIdentifierLength(Connection connection) {
    String warningMessage = "Unable to query database for maximum table name length; "
        + "the connector may fail to write to tables with long names";
    // https://stackoverflow.com/questions/27865770/how-long-can-postgresql-table-names-be/27865772#27865772
    String nameLengthQuery = "SELECT length(repeat('1234567890', 1000)::NAME);";

    int result;
    try (ResultSet rs = connection.createStatement().executeQuery(nameLengthQuery)) {
      if (rs.next()) {
        result = rs.getInt(1);
        if (result <= 0) {
          log.warn(
              "Cannot accommodate maximum table name length of {} as it is not positive; "
                  + "table name truncation will be disabled, "
                  + "and the connector may fail to write to tables with long names",
              result);
          result = Integer.MAX_VALUE;
        } else {
          log.info(
              "Maximum table name length for database is {} bytes",
              result
          );
        }
      } else {
        log.warn(warningMessage);
        result = Integer.MAX_VALUE;
      }
    } catch (SQLException e) {
      log.warn(warningMessage, e);
      result = Integer.MAX_VALUE;
    }
    return result;
  }

  @Override
  public TableId parseTableIdentifier(String fqn) {
    TableId result = super.parseTableIdentifier(fqn);
    if (maxIdentifierLength > 0 && result.tableName().length() > maxIdentifierLength) {
      String newTableName = result.tableName().substring(0, maxIdentifierLength);
      log.debug(
          "Truncating table name from {} to {} in order to respect maximum name length",
          result.tableName(),
          newTableName
      );
      result = new TableId(
          result.catalogName(),
          result.schemaName(),
          newTableName
      );
    }
    if (quoteSqlIdentifiers == QuoteMethod.NEVER) {
      result = new TableId(
          result.catalogName(),
          result.schemaName(),
          result.tableName().toLowerCase()
      );
    }
    return result;
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
  public String addFieldToSchema(
      ColumnDefinition columnDefn,
      SchemaBuilder builder
  ) {
    // Add the PostgreSQL-specific types first
    final String fieldName = fieldNameFor(columnDefn);
    switch (columnDefn.type()) {
      case Types.BIT: {
        // PostgreSQL allows variable length bit strings, but when length is 1 then the driver
        // returns a 't' or 'f' string value to represent the boolean value, so we need to handle
        // this as well as lengths larger than 8.
        boolean optional = columnDefn.isOptional();
        int numBits = columnDefn.precision();
        Schema schema;
        if (numBits <= 1) {
          schema = optional ? Schema.OPTIONAL_BOOLEAN_SCHEMA : Schema.BOOLEAN_SCHEMA;
        } else if (numBits <= 8) {
          // For consistency with what the connector did before ...
          schema = optional ? Schema.OPTIONAL_INT8_SCHEMA : Schema.INT8_SCHEMA;
        } else {
          schema = optional ? Schema.OPTIONAL_BYTES_SCHEMA : Schema.BYTES_SCHEMA;
        }
        builder.field(fieldName, schema);
        return fieldName;
      }
      case Types.OTHER: {
        // Some of these types will have fixed size, but we drop this from the schema conversion
        // since only fixed byte arrays can have a fixed size
        if (isJsonType(columnDefn)) {
          builder.field(fieldName, jsonSchema(columnDefn));
          return fieldName;
        }

        if (complexTypesEnabled() && isHstoreType(columnDefn)) {
          builder.field(fieldName, hstoreSchema(columnDefn));
          return fieldName;
        }

        if (UUID.class.getName().equals(columnDefn.classNameForType())) {
          builder.field(
              fieldName,
              columnDefn.isOptional()
                  ?
                  Schema.OPTIONAL_STRING_SCHEMA :
                  Schema.STRING_SCHEMA
          );
          return fieldName;
        }

        break;
      }
      case Types.ARRAY: {
        if (complexTypesEnabled()) {
          Schema elementSchema = arrayElementSchemaFor(columnDefn);
          if (elementSchema != null) {
            SchemaBuilder arrayBuilder = SchemaBuilder.array(elementSchema);
            if (columnDefn.isOptional()) {
              arrayBuilder.optional();
            }
            builder.field(fieldName, arrayBuilder.build());
            return fieldName;
          }
        }
        break;
      }
      default:
        break;
    }

    // Delegate for the remaining logic
    return super.addFieldToSchema(columnDefn, builder);
  }

  @Override
  protected ColumnConverter columnConverterFor(
      ColumnMapping mapping,
      ColumnDefinition defn,
      int col,
      boolean isJdbc4
  ) {
    // First handle any PostgreSQL-specific types
    ColumnDefinition columnDefn = mapping.columnDefn();
    switch (columnDefn.type()) {
      case Types.BIT: {
        // PostgreSQL allows variable length bit strings, but when length is 1 then the driver
        // returns a 't' or 'f' string value to represent the boolean value, so we need to handle
        // this as well as lengths larger than 8.
        final int numBits = columnDefn.precision();
        if (numBits <= 1) {
          return rs -> rs.getBoolean(col);
        } else if (numBits <= 8) {
          // Do this for consistency with earlier versions of the connector
          return rs -> rs.getByte(col);
        }
        return rs -> rs.getBytes(col);
      }
      case Types.OTHER: {
        if (isJsonType(columnDefn)) {
          if (complexTypesEnabled() && !jsonAsString()) {
            return rs -> JsonConverter.jsonStringToMap(rs.getString(col));
          }
          return rs -> rs.getString(col);
        }

        if (complexTypesEnabled() && isHstoreType(columnDefn)) {
          if (hstoreAsJson()) {
            return rs -> {
              Object value = rs.getObject(col);
              return value == null
                  ? null
                  : JsonConverter.connectValueToJson(null, value);
            };
          }
          return rs -> rs.getObject(col);
        }

        if (UUID.class.getName().equals(columnDefn.classNameForType())) {
          return rs -> rs.getString(col);
        }
        break;
      }
      case Types.ARRAY: {
        ColumnConverter arrayConverter = arrayColumnConverter(columnDefn, col);
        if (arrayConverter != null) {
          return arrayConverter;
        }
        break;
      }
      default:
        break;
    }

    // Delegate for the remaining logic
    return super.columnConverterFor(mapping, defn, col, isJdbc4);
  }

  protected boolean isJsonType(ColumnDefinition columnDefn) {
    String typeName = columnDefn.typeName();
    return JSON_TYPE_NAME.equalsIgnoreCase(typeName) || JSONB_TYPE_NAME.equalsIgnoreCase(typeName);
  }

  protected boolean isHstoreType(ColumnDefinition columnDefn) {
    return HSTORE_TYPE_NAME.equalsIgnoreCase(columnDefn.typeName());
  }

  /**
   * Map a PostgreSQL array column's element JDBC type to a Connect element {@link Schema}.
   * Returns null when the element type isn't covered by this MVP (callers fall through to the
   * generic dialect, which will skip the column with a WARN).
   */
  private Schema arrayElementSchemaFor(ColumnDefinition columnDefn) {
    String base = arrayElementBaseType(columnDefn);
    if (base == null) {
      return null;
    }
    switch (base) {
      case "text":
      case "varchar":
      case "bpchar":
      case "char":
        return Schema.OPTIONAL_STRING_SCHEMA;
      case "int2":
        return Schema.OPTIONAL_INT16_SCHEMA;
      case "int4":
        return Schema.OPTIONAL_INT32_SCHEMA;
      case "int8":
        return Schema.OPTIONAL_INT64_SCHEMA;
      case "float4":
        return Schema.OPTIONAL_FLOAT32_SCHEMA;
      case "float8":
        return Schema.OPTIONAL_FLOAT64_SCHEMA;
      case "bool":
      case "boolean":
        return Schema.OPTIONAL_BOOLEAN_SCHEMA;
      case "numeric":
      case "decimal":
        // Per-value scale carried via VariableScaleDecimal (array typmod is -1 in JDBC metadata).
        return VariableScaleDecimal.optionalSchema();
      case "json":
      case "jsonb":
        // Raw JSON text via the Json logical type; round-trips to a native jsonb[] sink column.
        return Json.optionalSchema();
      case "date":
        return Date.builder().optional().build();
      case "time":
        return Time.builder().optional().build();
      case "timestamp":
        return Timestamp.builder().optional().build();
      case "timestamptz":
        // ZonedTimestamp STRING (built-in Timestamp drops the zone); round-trips to timestamptz[].
        return ZonedTimestamp.optionalSchema();
      default:
        return null;
    }
  }

  /**
   * Lowercased element type name of a PostgreSQL array column (strips the leading {@code _}).
   */
  private static String arrayElementBaseType(ColumnDefinition columnDefn) {
    String typeName = columnDefn.typeName();
    if (typeName == null) {
      return null;
    }
    String base = typeName.startsWith("_") ? typeName.substring(1) : typeName;
    return base.toLowerCase();
  }

  /**
   * Select the value converter for a PostgreSQL array column, routing by the element's Connect
   * logical type (as decided by {@link #arrayElementSchemaFor}). Returns null when the feature is
   * disabled or the element type is not supported.
   */
  private ColumnConverter arrayColumnConverter(ColumnDefinition columnDefn, int col) {
    if (!complexTypesEnabled()) {
      return null;
    }
    final Schema elementSchema = arrayElementSchemaFor(columnDefn);
    if (elementSchema == null) {
      return null;
    }
    String elementName = elementSchema.name();
    if (elementName != null) {
      switch (elementName) {
        case VariableScaleDecimal.LOGICAL_NAME:
          return rs -> readJdbcArray(rs, col, value ->
              VariableScaleDecimal.fromLogical(elementSchema, (BigDecimal) value));
        case Json.LOGICAL_NAME:
          // Element is the raw JSON text; toString() covers both String and PGobject.
          return rs -> readJdbcArray(rs, col, Object::toString);
        case ZonedTimestamp.LOGICAL_NAME:
          return rs -> readZonedTimestampArray(rs, col);
        case Date.LOGICAL_NAME:
        case Time.LOGICAL_NAME:
        case Timestamp.LOGICAL_NAME:
          return rs -> readTemporalArray(rs, col, elementName);
        default:
          break;
      }
    }
    // Primitive elements pass through directly: the driver already returns the matching Java type.
    return rs -> readJdbcArray(rs, col, value -> value);
  }

  /**
   * Read a {@code timestamptz[]} array into ISO-8601 UTC strings ({@code ZonedTimestamp}), decoding
   * each element via the element {@link ResultSet} with a UTC Calendar.
   */
  private static List<Object> readZonedTimestampArray(ResultSet rs, int col)
      throws SQLException {
    Array arr = rs.getArray(col);
    if (arr == null) {
      return null;
    }
    try {
      Object raw = arr.getArray();
      if (raw instanceof Object[] && isMultiDimensional((Object[]) raw)) {
        log.warn(MULTI_DIMENSIONAL_ARRAY_WARNING, col);
        return null;
      }
      Calendar utc =
          DateTimeUtils.getZoneIdCalendar(ZoneOffset.UTC);
      try (ResultSet elementRs = arr.getResultSet()) {
        List<Object> out = new ArrayList<>();
        while (elementRs.next()) {
          java.sql.Timestamp ts = elementRs.getTimestamp(2, utc);
          out.add(elementRs.wasNull()
              ? null
              : ZonedTimestamp.toIsoString(ts));
        }
        return out;
      }
    } finally {
      arr.free();
    }
  }

  /**
   * Read a temporal array into Connect Date/Time/Timestamp values, decoding each element in UTC via
   * the element {@link ResultSet} ({@code getArray()} would parse in the JVM zone).
   */
  private static List<Object> readTemporalArray(ResultSet rs, int col, String elementName)
      throws SQLException {
    Array arr = rs.getArray(col);
    if (arr == null) {
      return null;
    }
    try {
      Object raw = arr.getArray();
      if (raw instanceof Object[] && isMultiDimensional((Object[]) raw)) {
        log.warn(MULTI_DIMENSIONAL_ARRAY_WARNING, col);
        return null;
      }
      Calendar utc =
          DateTimeUtils.getZoneIdCalendar(ZoneOffset.UTC);
      try (ResultSet elementRs = arr.getResultSet()) {
        List<Object> out = new ArrayList<>();
        while (elementRs.next()) {
          Object converted;
          if (Date.LOGICAL_NAME.equals(elementName)) {
            converted = elementRs.getDate(2, utc);
          } else if (Time.LOGICAL_NAME.equals(elementName)) {
            converted = elementRs.getTime(2, utc);
          } else {
            converted = elementRs.getTimestamp(2, utc);
          }
          out.add(elementRs.wasNull() ? null : converted);
        }
        return out;
      }
    } finally {
      arr.free();
    }
  }

  private static List<Object> readJdbcArray(
      ResultSet rs, int col, java.util.function.Function<Object, Object> elementMapper)
      throws SQLException {
    Array arr = rs.getArray(col);
    if (arr == null) {
      return null;
    }
    try {
      Object raw = arr.getArray();
      if (raw == null) {
        return null;
      }
      Object[] elements = (Object[]) raw;
      if (isMultiDimensional(elements)) {
        log.warn(MULTI_DIMENSIONAL_ARRAY_WARNING, col);
        return null;
      }
      List<Object> out = new ArrayList<>(elements.length);
      for (Object element : elements) {
        out.add(element == null ? null : elementMapper.apply(element));
      }
      return out;
    } finally {
      arr.free();
    }
  }

  /**
   * Whether the array elements are themselves arrays (a multi-dimensional column like
   * {@code int[][]}), which reports the same type name as 1-D and is only detectable from values.
   */
  private static boolean isMultiDimensional(Object[] elements) {
    for (Object element : elements) {
      if (element != null) {
        return element.getClass().isArray();
      }
    }
    return false;
  }

  private boolean isJsonBindCandidate(Schema schema) {
    Schema.Type type = schema.type();
    if (type != Schema.Type.STRUCT && type != Schema.Type.MAP) {
      return false;
    }
    return config instanceof JdbcSinkConfig
        && ((JdbcSinkConfig) config).sqlComplexTypesEnable;
  }

  private boolean maybeBindJson(
      PreparedStatement statement,
      int index,
      Schema schema,
      Object value
  ) throws SQLException {
    if (!isJsonBindCandidate(schema)) {
      return false;
    }
    String json = JsonConverter.connectValueToJson(schema, value);
    if (json == null) {
      statement.setNull(index, Types.OTHER);
    } else {
      // Bind as text; the ::jsonb cast (see valueTypeCast) parses it into jsonb server-side.
      statement.setString(index, json);
    }
    return true;
  }

  private boolean complexTypesEnabled() {
    if (config instanceof JdbcSinkConfig) {
      return ((JdbcSinkConfig) config).sqlComplexTypesEnable;
    }
    if (config instanceof io.confluent.connect.jdbc.source.JdbcSourceConnectorConfig) {
      return config.getBoolean(
          io.confluent.connect.jdbc.source.JdbcSourceConnectorConfig
              .SQL_COMPLEX_TYPES_ENABLE_CONFIG);
    }
    return false;
  }

  /**
   * Whether PostgreSQL hstore columns should be emitted as a JSON-object STRING (mode
   * {@code json}) rather than a Connect Map (mode {@code map}, the default). Only the source
   * connector exposes this; the sink path always uses the Map representation.
   */
  private boolean hstoreAsJson() {
    if (config instanceof io.confluent.connect.jdbc.source.JdbcSourceConnectorConfig) {
      return io.confluent.connect.jdbc.source.JdbcSourceConnectorConfig.HSTORE_HANDLING_MODE_JSON
          .equalsIgnoreCase(config.getString(
              io.confluent.connect.jdbc.source.JdbcSourceConnectorConfig
                  .HSTORE_HANDLING_MODE_CONFIG));
    }
    return false;
  }

  /**
   * Whether PostgreSQL json/jsonb columns should be emitted as a logical JSON STRING (mode
   * {@code string}) rather than a Connect Map (mode {@code map}, the default). Only the source
   * connector exposes this.
   */
  private boolean jsonAsString() {
    if (config instanceof io.confluent.connect.jdbc.source.JdbcSourceConnectorConfig) {
      return io.confluent.connect.jdbc.source.JdbcSourceConnectorConfig.JSON_HANDLING_MODE_STRING
          .equalsIgnoreCase(config.getString(
              io.confluent.connect.jdbc.source.JdbcSourceConnectorConfig
                  .JSON_HANDLING_MODE_CONFIG));
    }
    return false;
  }

  /**
   * Build the Connect schema for a PostgreSQL json/jsonb column. When complex types are disabled
   * the column stays a plain STRING. When enabled it is a logical JSON STRING (mode
   * {@code string}) or a Map&lt;String,String&gt; (mode {@code map}, the default).
   */
  private Schema jsonSchema(ColumnDefinition columnDefn) {
    boolean optional = columnDefn.isOptional();
    if (!complexTypesEnabled()) {
      return optional ? Schema.OPTIONAL_STRING_SCHEMA : Schema.STRING_SCHEMA;
    }
    if (jsonAsString()) {
      SchemaBuilder jsonBuilder = Json.builder();
      if (optional) {
        jsonBuilder.optional();
      }
      return jsonBuilder.build();
    }
    SchemaBuilder mapBuilder = SchemaBuilder.map(
        Schema.STRING_SCHEMA, Schema.OPTIONAL_STRING_SCHEMA);
    if (optional) {
      mapBuilder.optional();
    }
    return mapBuilder.build();
  }

  /**
   * Build the Connect schema for a PostgreSQL hstore column, honoring
   * {@code hstore.handling.mode}: a JSON-object STRING when {@code json}, otherwise a
   * Map&lt;String,String&gt;.
   */
  private Schema hstoreSchema(ColumnDefinition columnDefn) {
    if (hstoreAsJson()) {
      return columnDefn.isOptional() ? Schema.OPTIONAL_STRING_SCHEMA : Schema.STRING_SCHEMA;
    }
    SchemaBuilder mapBuilder = SchemaBuilder.map(
        Schema.STRING_SCHEMA, Schema.OPTIONAL_STRING_SCHEMA);
    if (columnDefn.isOptional()) {
      mapBuilder.optional();
    }
    return mapBuilder.build();
  }

  @Override
  protected String getSqlType(SinkRecordField field) {
    if (field.schemaName() != null) {
      switch (field.schemaName()) {
        case Decimal.LOGICAL_NAME:
          return "DECIMAL";
        case Date.LOGICAL_NAME:
          return "DATE";
        case Time.LOGICAL_NAME:
          return "TIME";
        case Timestamp.LOGICAL_NAME:
          return "TIMESTAMP";
        case ZonedTimestamp.LOGICAL_NAME:
          // Native TIMESTAMP WITH TIME ZONE (and TIMESTAMP WITH TIME ZONE[] as an array element).
          return "TIMESTAMP WITH TIME ZONE";
        case Json.LOGICAL_NAME:
          // Logical JSON STRING -> native JSONB; text binds via the existing ::jsonb cast.
          return JSONB_TYPE_NAME.toUpperCase();
        case VariableScaleDecimal.LOGICAL_NAME:
          // Per-value scale -> unconstrained NUMERIC (and NUMERIC[] as an array element).
          return NUMERIC_TYPE_NAME.toUpperCase();
        default:
          // fall through to normal types
      }
    }
    switch (field.schemaType()) {
      case INT8:
      case INT16:
        return "SMALLINT";
      case INT32:
        return "INT";
      case INT64:
        if (config instanceof JdbcSinkConfig
            && config.getList(JdbcSinkConfig.TIMESTAMP_FIELDS_LIST).contains(field.name())) {
          return "TIMESTAMP";
        }
        return "BIGINT";
      case FLOAT32:
        return "REAL";
      case FLOAT64:
        return "DOUBLE PRECISION";
      case BOOLEAN:
        return "BOOLEAN";
      case STRING:
        if (config instanceof JdbcSinkConfig
            && config.getList(JdbcSinkConfig.TIMESTAMP_FIELDS_LIST).contains(field.name())) {
          return "TIMESTAMP";
        }
        return "TEXT";
      case BYTES:
        return "BYTEA";
      case ARRAY:
        SinkRecordField childField = new SinkRecordField(
            field.schema().valueSchema(),
            field.name(),
            field.isPrimaryKey()
        );
        return getSqlType(childField) + "[]";
      case STRUCT:
      case MAP:
        if (config instanceof JdbcSinkConfig
            && ((JdbcSinkConfig) config).sqlComplexTypesEnable) {
          return JSONB_TYPE_NAME.toUpperCase();
        }
        return super.getSqlType(field);
      default:
        return super.getSqlType(field);
    }
  }

  @Override
  public String buildInsertStatement(
      TableId table,
      Collection<ColumnId> keyColumns,
      Collection<ColumnId> nonKeyColumns,
      TableDefinition definition
  ) {
    ExpressionBuilder builder = expressionBuilder();
    builder.append("INSERT INTO ");
    builder.append(table);
    builder.append(" (");
    builder.appendList()
        .delimitedBy(",")
        .transformedBy(ExpressionBuilder.columnNames())
        .of(keyColumns, nonKeyColumns);
    builder.append(") VALUES (");
    builder.appendList()
        .delimitedBy(",")
        .transformedBy(this.columnValueVariables(definition))
        .of(keyColumns, nonKeyColumns);
    builder.append(")");
    return builder.toString();
  }

  @Override
  public String buildUpdateStatement(
      TableId table,
      Collection<ColumnId> keyColumns,
      Collection<ColumnId> nonKeyColumns,
      TableDefinition definition
  ) {
    ExpressionBuilder builder = expressionBuilder();
    builder.append("UPDATE ");
    builder.append(table);
    builder.append(" SET ");
    builder.appendList()
        .delimitedBy(", ")
        .transformedBy(this.columnNamesWithValueVariables(definition))
        .of(nonKeyColumns);
    if (!keyColumns.isEmpty()) {
      builder.append(" WHERE ");
      builder.appendList()
          .delimitedBy(" AND ")
          .transformedBy(ExpressionBuilder.columnNamesWith(" = ?"))
          .of(keyColumns);
    }
    return builder.toString();
  }

  @Override
  public String buildUpsertQueryStatement(
      TableId table,
      Collection<ColumnId> keyColumns,
      Collection<ColumnId> nonKeyColumns,
      TableDefinition definition
  ) {
    final Transform<ColumnId> transform = (builder, col) -> {
      builder.appendColumnName(col.name())
          .append("=EXCLUDED.")
          .appendColumnName(col.name());
    };

    ExpressionBuilder builder = expressionBuilder();
    builder.append("INSERT INTO ");
    builder.append(table);
    builder.append(" (");
    builder.appendList()
        .delimitedBy(",")
        .transformedBy(ExpressionBuilder.columnNames())
        .of(keyColumns, nonKeyColumns);
    builder.append(") VALUES (");
    builder.appendList()
        .delimitedBy(",")
        .transformedBy(this.columnValueVariables(definition))
        .of(keyColumns, nonKeyColumns);
    builder.append(") ON CONFLICT (");
    builder.appendList()
        .delimitedBy(",")
        .transformedBy(ExpressionBuilder.columnNames())
        .of(keyColumns);
    if (nonKeyColumns.isEmpty()) {
      builder.append(") DO NOTHING");
    } else {
      builder.append(") DO UPDATE SET ");
      builder.appendList()
          .delimitedBy(",")
          .transformedBy(transform)
          .of(nonKeyColumns);
    }
    return builder.toString();
  }

  @Override
  protected void formatColumnValue(
      ExpressionBuilder builder,
      String schemaName,
      Map<String, String> schemaParameters,
      Schema.Type type,
      Object value
  ) {
    if (schemaName == null) {
      switch (type) {
        case BOOLEAN:
          builder.append((Boolean) value ? "TRUE" : "FALSE");
          return;
        case ARRAY:
          formatArrayValue(builder, value);
          return;
        default:
          // Fall through to base implementation
          break;
      }
    }
    super.formatColumnValue(builder, schemaName, schemaParameters, type, value);
  }

  private void formatArrayValue(ExpressionBuilder builder, Object value) {
    if (value == null) {
      builder.append("NULL");
      return;
    }

    builder.append("ARRAY[");

    Collection<?> valueCollection;
    if (value instanceof Collection) {
      valueCollection = (Collection<?>) value;
    } else {
      throw new ConnectException("Unsupported type for array value: " + value.getClass().getName());
    }
    builder.appendList()
        .delimitedBy(",")
        .transformedBy(PostgreSqlDatabaseDialect::formatArrayItem)
        .of(valueCollection);
    builder.append("]");
  }

  private static void formatArrayItem(ExpressionBuilder builder, Object item) {
    if (item == null) {
      builder.append("NULL");
    } else if (item instanceof String) {
      String escapedString = ((String) item).replace("'", "''");
      builder.appendStringQuoted(escapedString);
    } else if (item instanceof Number) {
      builder.append(item.toString());
    } else if (item instanceof Boolean) {
      builder.append((Boolean) item ? "TRUE" : "FALSE");
    } else {
      throw new ConnectException("Unsupported type for array item: " + item.getClass().getName());
    }
  }


  @Override
  protected boolean maybeBindPrimitive(
      PreparedStatement statement,
      int index,
      Schema schema,
      Object value,
      String fieldName
  ) throws SQLException {
    if (maybeBindJson(statement, index, schema, value)) {
      return true;
    }
    if (schema.type() == Schema.Type.ARRAY && bindArray(statement, index, schema, value)) {
      return true;
    }
    return super.maybeBindPrimitive(statement, index, schema, value, fieldName);
  }

  /**
   * Bind a Connect ARRAY value to a native PostgreSQL array parameter by dispatching on the element
   * type. Each {@code maybeBind...Array} helper handles a single element type and returns true once
   * it binds; returns false if the element type is not handled here.
   */
  private boolean bindArray(PreparedStatement statement, int index, Schema schema, Object value)
      throws SQLException {
    Collection<?> values = arrayValueCollection(value);
    Schema elementSchema = schema.valueSchema();
    String elementName = elementSchema != null ? elementSchema.name() : null;
    if (maybeBindJsonArray(statement, index, elementName, values)) {
      return true;
    }
    if (maybeBindZonedTimestampArray(statement, index, elementName, values)) {
      return true;
    }
    if (maybeBindVariableScaleDecimalArray(statement, index, elementName, values)) {
      return true;
    }
    if (maybeBindTemporalArray(statement, index, elementName, values)) {
      return true;
    }
    return maybeBindPrimitiveArray(statement, index, elementSchema, values);
  }

  /**
   * Coerce a Connect ARRAY value into a {@link Collection} of elements.
   */
  private static Collection<?> arrayValueCollection(Object value) {
    Class<?> valueClass = value.getClass();
    if (Collection.class.isAssignableFrom(valueClass)) {
      return (Collection<?>) value;
    } else if (valueClass.isArray()) {
      return Arrays.asList((Object[]) value);
    }
    throw new DataException(
        String.format("Type '%s' is not supported for Array.", valueClass.getName()));
  }

  /**
   * Bind an array of the Json logical type as a native {@code jsonb[]} (each element is raw JSON
   * text). Returns false if the element type is not Json.
   */
  private boolean maybeBindJsonArray(PreparedStatement statement, int index, String elementName,
      Collection<?> values) throws SQLException {
    if (!Json.LOGICAL_NAME.equals(elementName)) {
      return false;
    }
    Array array = statement.getConnection()
        .createArrayOf(JSONB_TYPE_NAME, values.toArray());
    statement.setArray(index, array);
    return true;
  }

  /**
   * Bind an array of the ZonedTimestamp logical type as a native {@code timestamptz[]} (each
   * element is an ISO-8601 offset string). Returns false if the element type is not ZonedTimestamp.
   */
  private boolean maybeBindZonedTimestampArray(PreparedStatement statement, int index,
      String elementName, Collection<?> values) throws SQLException {
    if (!ZonedTimestamp.LOGICAL_NAME.equals(elementName)) {
      return false;
    }
    Array array = statement.getConnection()
        .createArrayOf(TIMESTAMPTZ_TYPE_NAME, values.toArray());
    statement.setArray(index, array);
    return true;
  }

  /**
   * Bind an array of VariableScaleDecimal as a native {@code numeric[]}, decoding each
   * {@code {scale,value}} struct back to its exact BigDecimal. Returns false if the element type is
   * not VariableScaleDecimal.
   */
  private boolean maybeBindVariableScaleDecimalArray(PreparedStatement statement, int index,
      String elementName, Collection<?> values) throws SQLException {
    if (!VariableScaleDecimal.LOGICAL_NAME.equals(elementName)) {
      return false;
    }
    Array array = statement.getConnection()
        .createArrayOf(NUMERIC_TYPE_NAME, bigDecimalArrayFor(values));
    statement.setArray(index, array);
    return true;
  }

  /**
   * Bind an array of the temporal logical types as a native {@code date[]}/{@code time[]}/
   * {@code timestamp[]}, rendering each element as a UTC text literal (timezone-safe regardless of
   * the JVM default zone). Returns false if the element type is not a non-zoned temporal type.
   */
  private boolean maybeBindTemporalArray(PreparedStatement statement, int index, String elementName,
      Collection<?> values) throws SQLException {
    String temporalType = temporalArrayType(elementName);
    if (temporalType == null) {
      return false;
    }
    Array array = statement.getConnection()
        .createArrayOf(temporalType, temporalArrayFor(elementName, values));
    statement.setArray(index, array);
    return true;
  }

  /**
   * Bind an array of a primitive element type per pgjdbc's documented array mapping. Returns false
   * if the element type is not a supported primitive.
   */
  private boolean maybeBindPrimitiveArray(PreparedStatement statement, int index,
      Schema elementSchema, Collection<?> values) throws SQLException {
    Object newValue =
        elementSchema == null ? null : primitiveArrayFor(elementSchema.type(), values);
    if (newValue == null) {
      return false;
    }
    statement.setObject(index, newValue, Types.ARRAY);
    return true;
  }

  /**
   * Decode a collection of VariableScaleDecimal structs back into a {@code BigDecimal[]}, each with
   * its own scale, for binding into a native {@code numeric[]} column. Null elements are preserved.
   */
  private static BigDecimal[] bigDecimalArrayFor(Collection<?> valueCollection) {
    return valueCollection.stream()
        .map(o -> o == null ? null
            : VariableScaleDecimal.toLogical(
                (Struct) o))
        .toArray(BigDecimal[]::new);
  }

  /**
   * Map a temporal logical element name to its PostgreSQL array element type name, or null if the
   * element is not one of the temporal logical types handled here.
   */
  private static String temporalArrayType(String elementName) {
    if (Date.LOGICAL_NAME.equals(elementName)) {
      return DATE_TYPE_NAME;
    } else if (Time.LOGICAL_NAME.equals(elementName)) {
      return TIME_TYPE_NAME;
    } else if (Timestamp.LOGICAL_NAME.equals(elementName)) {
      return TIMESTAMP_TYPE_NAME;
    }
    return null;
  }

  /**
   * Render each temporal element (a {@code java.util.Date} carrying a logical Date/Time/Timestamp)
   * as a UTC text literal for binding into a native date[]/time[]/timestamp[] column. Formatting in
   * UTC keeps the value timezone-safe irrespective of the JVM default zone. Nulls are preserved.
   */
  private static Object[] temporalArrayFor(String elementName, Collection<?> valueCollection) {
    DateTimeFormatter fmt = temporalFormatter(elementName);
    return valueCollection.stream()
        .map(o -> o == null ? null
            : fmt.format(((java.util.Date) o).toInstant().atZone(ZoneOffset.UTC)))
        .toArray(String[]::new);
  }

  /**
   * Return the UTC text format for a temporal logical element name (date, time, or timestamp).
   */
  private static DateTimeFormatter temporalFormatter(String elementName) {
    if (Date.LOGICAL_NAME.equals(elementName)) {
      return DateTimeFormatter.ofPattern("yyyy-MM-dd");
    } else if (Time.LOGICAL_NAME.equals(elementName)) {
      return DateTimeFormatter.ofPattern("HH:mm:ss.SSS");
    }
    return DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS");
  }

  /**
   * Convert a collection into a typed Java array for a primitive Connect element type, following
   * pgjdbc's documented array mapping. Returns null for unhandled element types.
   */
  private static Object primitiveArrayFor(Schema.Type elementType, Collection<?> valueCollection) {
    switch (elementType) {
      case INT8:
        // PostgreSQL has no single-byte integer; widen to short.
        return valueCollection.stream().map(o -> ((Byte) o).shortValue()).toArray(Short[]::new);
      case INT16:
        return valueCollection.toArray(new Short[0]);
      case INT32:
        return valueCollection.toArray(new Integer[0]);
      case INT64:
        return valueCollection.toArray(new Long[0]);
      case FLOAT32:
        return valueCollection.toArray(new Float[0]);
      case FLOAT64:
        return valueCollection.toArray(new Double[0]);
      case BOOLEAN:
        return valueCollection.toArray(new Boolean[0]);
      case STRING:
        return valueCollection.toArray(new String[0]);
      default:
        return null;
    }
  }

  /**
   * Return the transform that produces an assignment expression each with the name of one of the
   * columns and the prepared statement variable. PostgreSQL may require the variable to have a
   * type suffix, such as {@code ?::uuid}.
   *
   * @param defn the table definition; may be null if unknown
   * @return the transform that produces the assignment expression for use within a prepared
   *         statement; never null
   */
  protected Transform<ColumnId> columnNamesWithValueVariables(TableDefinition defn) {
    return (builder, columnId) -> {
      builder.appendColumnName(columnId.name());
      builder.append(" = ?");
      builder.append(valueTypeCast(defn, columnId));
    };
  }

  /**
   * Return the transform that produces a prepared statement variable for each of the columns.
   * PostgreSQL may require the variable to have a type suffix, such as {@code ?::uuid}.
   *
   * @param defn the table definition; may be null if unknown
   * @return the transform that produces the variable expression for each column; never null
   */
  protected Transform<ColumnId> columnValueVariables(TableDefinition defn) {
    return (builder, columnId) -> {
      builder.append("?");
      builder.append(valueTypeCast(defn, columnId));
    };
  }

  /**
   * Return the typecast expression that can be used as a suffix for a value variable of the
   * given column in the defined table.
   *
   * <p>This method returns a blank string except for those column types that require casting
   * when set with literal values. For example, a column of type {@code uuid} must be cast when
   * being bound with with a {@code varchar} literal, since a UUID value cannot be bound directly.
   *
   * @param tableDefn the table definition; may be null if unknown
   * @param columnId  the column within the table; may not be null
   * @return the cast expression, or an empty string; never null
   */
  protected String valueTypeCast(TableDefinition tableDefn, ColumnId columnId) {
    if (tableDefn != null) {
      ColumnDefinition defn = tableDefn.definitionForColumn(columnId.name());
      if (defn != null) {
        String typeName = defn.typeName(); // database-specific
        if (typeName != null) {
          typeName = typeName.toLowerCase();
          if (CAST_TYPES.contains(typeName)) {
            return "::" + typeName;
          }
        }
      }
    }
    return "";
  }

  @Override
  protected int decimalScale(ColumnDefinition defn) {
    if (defn.scale() == NUMERIC_TYPE_SCALE_UNSET) {
      return NUMERIC_TYPE_SCALE_HIGH;
    }

    // Postgres requires DECIMAL/NUMERIC columns to have a precision greater than zero
    // If the precision appears to be zero, it's because the user didn't define a fixed precision
    // for the column.
    if (defn.precision() == 0) {
      // In that case, a scale of zero indicates that there also isn't a fixed scale defined for
      // the column. Instead of treating that column as if its scale is actually zero (which can
      // cause issues since it may contain values that aren't possible with a scale of zero, like
      // 12.12), we fall back on NUMERIC_TYPE_SCALE_HIGH to try to avoid loss of precision
      if (defn.scale() == 0) {
        log.debug(
            "Column {} does not appear to have a fixed scale defined; defaulting to {}",
            defn.id(),
            NUMERIC_TYPE_SCALE_HIGH
        );
        return NUMERIC_TYPE_SCALE_HIGH;
      } else {
        // Should never happen, but if it does may signal an edge case
        // that we need to add new logic for
        log.warn(
            "Column {} has a precision of zero, but a non-zero scale of {}",
            defn.id(),
            defn.scale()
        );
      }
    }

    return defn.scale();
  }

}
