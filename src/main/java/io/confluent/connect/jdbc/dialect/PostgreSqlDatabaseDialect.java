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
import io.confluent.connect.jdbc.dialect.DatabaseDialectProvider.SubprotocolBasedProvider;
import io.confluent.connect.jdbc.sink.JdbcSinkConfig;
import io.confluent.connect.jdbc.sink.metadata.SinkRecordField;
import io.confluent.connect.jdbc.source.ColumnMapping;
import io.confluent.connect.jdbc.source.JdbcSourceConnectorConfig;
import io.confluent.connect.jdbc.source.JdbcSourceConnectorConfig.HstoreHandlingMode;
import io.confluent.connect.jdbc.util.ColumnDefinition;
import io.confluent.connect.jdbc.util.ColumnId;
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
import org.apache.kafka.connect.data.Time;
import org.apache.kafka.connect.data.Timestamp;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.errors.DataException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.Properties;

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
  static final String HSTORE_TYPE_NAME = "hstore";

  private static final String HSTORE_OFF_SEARCH_PATH_ERROR =
      "Cannot map hstore column %s: the driver reports its type as %s, so the hstore extension is "
          + "not on this connection's search_path. Add the schema owning the extension to the "
          + "search_path, for example with ?currentSchema=ext,public on connection.url, or set "
          + "hstore.handling.mode=none to skip hstore columns.";

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
          return rs -> rs.getString(col);
        }

        if (UUID.class.getName().equals(columnDefn.classNameForType())) {
          return rs -> rs.getString(col);
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

  /**
   * Whether the schema is a {@code MAP<STRING, STRING>}, the only Connect container mapped to a
   * native {@code jsonb} column. That is the shape a PostgreSQL {@code hstore} column takes on the
   * topic; STRUCT values and other map shapes are not supported.
   */
  private static boolean isStringToStringMap(Schema schema) {
    return schema.type() == Schema.Type.MAP
        && schema.keySchema().type() == Schema.Type.STRING
        && schema.valueSchema().type() == Schema.Type.STRING;
  }

  private boolean isJsonBindCandidate(Schema schema) {
    return isStringToStringMap(schema) && complexTypesEnabled();
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
    String json = JsonConverter.connectMapToJson(value);
    if (json == null) {
      statement.setNull(index, Types.OTHER);
    } else {
      // Bind as text; the ::jsonb cast (see valueTypeCast) parses it into jsonb server-side.
      statement.setString(index, json);
    }
    return true;
  }

  /**
   * The configured {@code hstore.handling.mode}. Only the source connector selects a
   * representation; the sink reads whichever shape arrives on the topic.
   */
  protected HstoreHandlingMode hstoreHandlingMode() {
    return config instanceof JdbcSourceConnectorConfig
        ? ((JdbcSourceConnectorConfig) config).hstoreHandlingMode()
        : HstoreHandlingMode.MAP;
  }

  /**
   * Whether {@code hstore.handling.mode} selects a representation, i.e. is not {@code none}, the
   * default. Paired with the complex types flag at every hstore call site, so hstore is opted into
   * independently of json/jsonb and arrays and {@link #hstoreSchema(boolean)} is only ever reached
   * for a real representation.
   */
  protected boolean hstoreMappingSelected() {
    return hstoreHandlingMode() != HstoreHandlingMode.NONE;
  }

  /**
   * The local, unquoted PostgreSQL type name: {@code hstore} and {@code "ext"."hstore"} both yield
   * {@code hstore}, and {@code "ext"."_hstore"} yields {@code _hstore}. pgjdbc renders an extension
   * type bare only while its schema is on the connection's {@code search_path}, and
   * schema-qualifies it otherwise.
   */
  protected static String localTypeName(String typeName) {
    if (typeName == null) {
      return null;
    }
    int lastDot = typeName.lastIndexOf('.');
    return (lastDot < 0 ? typeName : typeName.substring(lastDot + 1)).replace("\"", "");
  }

  /**
   * Whether a type name is an hstore the driver could not resolve, i.e. one that arrived
   * schema-qualified because the extension is off the {@code search_path}. Shared by the scalar
   * column path and the array element path, which must agree.
   */
  protected static boolean isUnresolvedHstoreType(String typeName) {
    return typeName != null
        && !HSTORE_TYPE_NAME.equalsIgnoreCase(typeName)
        && HSTORE_TYPE_NAME.equalsIgnoreCase(localTypeName(typeName));
  }

  /**
   * The failure for an hstore column that a selected mapping mode cannot honour. Failing beats
   * silently dropping the column, since a mode was explicitly asked for.
   */
  protected ConnectException hstoreOffSearchPathError(ColumnId column, String typeName) {
    return new ConnectException(String.format(HSTORE_OFF_SEARCH_PATH_ERROR, column, typeName));
  }

  /**
   * The on-topic schema for an hstore value under {@code hstore.handling.mode}: a {@code Json}
   * STRING, or a {@code MAP<STRING, STRING>} whose values are optional since an hstore value may
   * be NULL. Shared so a scalar column (optionality from the column) and an array element (always
   * optional) cannot drift apart. Only valid once {@link #hstoreMappingEnabled()} holds; the map
   * branch is the fallback, so {@code none} would otherwise be read as {@code map}.
   */
  protected Schema hstoreSchema(boolean optional) {
    if (hstoreHandlingMode() == HstoreHandlingMode.JSON) {
      return optional ? Json.optionalSchema() : Json.schema();
    }
    SchemaBuilder mapBuilder =
        SchemaBuilder.map(Schema.STRING_SCHEMA, Schema.OPTIONAL_STRING_SCHEMA);
    if (optional) {
      mapBuilder.optional();
    }
    return mapBuilder.build();
  }

  private boolean complexTypesEnabled() {
    if (config instanceof JdbcSinkConfig) {
      return ((JdbcSinkConfig) config).sqlComplexTypesEnable;
    }
    if (config instanceof JdbcSourceConnectorConfig) {
      return ((JdbcSourceConnectorConfig) config).sqlComplexTypesEnabled();
    }
    return false;
  }

  /**
   * Build the Connect schema for a PostgreSQL json/jsonb column. When complex types are disabled
   * the column stays a plain STRING; when enabled it is a logical JSON STRING (raw text, aligned
   * with Debezium's {@code io.debezium.data.Json}).
   */
  private Schema jsonSchema(ColumnDefinition columnDefn) {
    boolean optional = columnDefn.isOptional();
    if (!complexTypesEnabled()) {
      return optional ? Schema.OPTIONAL_STRING_SCHEMA : Schema.STRING_SCHEMA;
    }
    return optional ? Json.optionalSchema() : Json.schema();
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
        case Json.LOGICAL_NAME:
          if (complexTypesEnabled()) {
            return JSONB_TYPE_NAME.toUpperCase();
          }
          break;
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
      case MAP:
        if (isStringToStringMap(field.schema()) && complexTypesEnabled()) {
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
    switch (schema.type()) {
      case ARRAY: {
        Class<?> valueClass = value.getClass();
        Collection<?> valueCollection;
        if (Collection.class.isAssignableFrom(valueClass)) {
          valueCollection = (Collection<?>) value;
        } else if (valueClass.isArray()) {
          valueCollection = Arrays.asList((Object[]) value);
        } else {
          throw new DataException(
              String.format("Type '%s' is not supported for Array.", valueClass.getName())
          );
        }
        Object newValue = primitiveArrayFor(schema.valueSchema().type(), valueCollection);
        if (newValue != null) {
          statement.setObject(index, newValue, Types.ARRAY);
          return true;
        }
        break;
      }
      case MAP:
        if (maybeBindJson(statement, index, schema, value)) {
          return true;
        }
        break;
      default:
        break;
    }
    return super.maybeBindPrimitive(statement, index, schema, value, fieldName);
  }

  /**
   * Convert a collection into a typed Java array for a primitive Connect element type, following
   * pgjdbc's array mapping (https://jdbc.postgresql.org/documentation/head/arrays.html). Returns
   * null for unhandled element types.
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
