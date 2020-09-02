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

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.connect.data.Date;
import org.apache.kafka.connect.data.Decimal;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Time;
import org.apache.kafka.connect.data.Timestamp;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Calendar;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.TimeZone;

import io.confluent.connect.jdbc.dialect.DatabaseDialectProvider.SubprotocolBasedProvider;
import io.confluent.connect.jdbc.sink.metadata.SinkRecordField;
import io.confluent.connect.jdbc.source.ColumnMapping;
import io.confluent.connect.jdbc.util.ColumnDefinition;
import io.confluent.connect.jdbc.util.ColumnId;
import io.confluent.connect.jdbc.util.DateTimeUtils;
import io.confluent.connect.jdbc.util.ExpressionBuilder;
import io.confluent.connect.jdbc.util.IdentifierRules;
import io.confluent.connect.jdbc.util.TableId;
import io.confluent.connect.jdbc.util.ColumnDefinition.Mutability;
import io.confluent.connect.jdbc.util.ColumnDefinition.Nullability;

/**
 * A {@link DatabaseDialect} for SQL Server.
 */
public class SqlServerDatabaseDialect extends GenericDatabaseDialect {

  /**
   * JDBC Type constant for SQL Server's custom data types.
   */
  static final int DATETIMEOFFSET = -155;

  /**
   * This is the format of the string form of DATETIMEOFFSET values, and used to parse such
   * string values into {@link java.sql.Timestamp} values.
   * https://docs.microsoft.com/en-us/sql/t-sql/data-types/datetimeoffset-transact-sql
   */
  private static final String DATE_TIME_FORMAT = "yyyy-MM-dd HH:mm:ss.SSSSSSS ZZZZZ";
  private static final DateTimeFormatter DATE_TIME_FORMATTER =
      DateTimeFormatter.ofPattern(DATE_TIME_FORMAT);

  /**
   * The provider for {@link SqlServerDatabaseDialect}.
   */
  public static class Provider extends SubprotocolBasedProvider {
    public Provider() {
      super(SqlServerDatabaseDialect.class.getSimpleName(), "microsoft:sqlserver", "sqlserver",
            "jtds:sqlserver");
    }

    @Override
    public DatabaseDialect create(AbstractConfig config) {
      return new SqlServerDatabaseDialect(config);
    }
  }

  private final boolean jtdsDriver;

  /**
   * Create a new dialect instance with the given connector configuration.
   *
   * @param config the connector configuration; may not be null
   */
  public SqlServerDatabaseDialect(AbstractConfig config) {
    super(config, new IdentifierRules(".", "[", "]"));
    jtdsDriver = jdbcUrlInfo == null ? false : jdbcUrlInfo.subprotocol().matches("jtds");
  }

  @Override
  protected boolean useCatalog() {
    // SQL Server uses JDBC's catalog to represent the database,
    // and JDBC's schema to represent the owner (e.g., "dbo")
    return true;
  }

  @Override
  protected String addFieldToSchema(
      ColumnDefinition columnDefn,
      SchemaBuilder builder,
      String fieldName,
      int sqlType,
      boolean optional
  ) {
    // Handle SQL Server specific types first
    switch (sqlType) {
      case DATETIMEOFFSET:
        // Use the same schema definition as a standard timestamp
        return super.addFieldToSchema(columnDefn, builder, fieldName, Types.TIMESTAMP, optional);
      default:
        break;
    }

    // Delegate for the remaining logic to handle the standard types
    return super.addFieldToSchema(columnDefn, builder, fieldName, sqlType, optional);
  }

  @Override
  protected ColumnConverter columnConverterFor(
      ColumnMapping mapping,
      ColumnDefinition defn,
      int col,
      boolean isJdbc4
  ) {
    // Handle any SQL Server specific data types first
    switch (defn.type()) {
      case DATETIMEOFFSET:
        if (jtdsDriver) {
          return rs -> convertDateTimeOffsetFromString(rs, col);
        } else {
          return rs -> convertDateTimeOffset(rs, col);
        }
      default:
        break;
    }

    // Delegate for the remaining logic to handle the standard types
    return super.columnConverterFor(mapping, defn, col, isJdbc4);
  }

  /**
   * Get the {@link java.sql.Timestamp} for the DATETIMEOFFSET column. This requires that the
   * JDBC driver supports SQL Server's DATETIMEOFFSET data type and converting to a
   * {@link java.sql.Timestamp} via {@link ResultSet#getTimestamp(int, Calendar)}.
   *
   * @param rs  the result set; never null
   * @param col the column index
   * @return the {@link java.sql.Timestamp} value; may be null
   * @throws SQLException if there is a problem getting the value
   */
  protected Object convertDateTimeOffset(ResultSet rs, int col) throws SQLException {
    return rs.getTimestamp(col, DateTimeUtils.getTimeZoneCalendar(timeZone()));
  }

  /**
   * Get the {@link java.sql.Timestamp} for the DATETIMEOFFSET column.
   * The jTDS driver doesn't support DATETIMEOFFSET, so the recommended approach for legacy driver
   * (see https://docs.microsoft.com/en-us/sql/t-sql/data-types/datetimeoffset-transact-sql) is to
   * get the value in string form and then parse it into a timestamp.
   *
   * @param rs  the result set; never null
   * @param col the column index
   * @return the {@link java.sql.Timestamp} value; may be null
   * @throws SQLException if there is a problem getting the value
   */
  protected Object convertDateTimeOffsetFromString(
      ResultSet rs,
      int col
  ) throws SQLException {
    String value = rs.getString(col);
    return value == null ? null : dateTimeOffsetFrom(rs.getString(col), timeZone());
  }

  /**
   * Utility method to parse the string form of a SQL Server DATETIMEOFFSET value into a
   * {@link java.sql.Timestamp} value.
   *
   * @param value    the string DATETIMEOFFSET value; never null
   * @param timeZone the timezone in which the {@link java.sql.Timestamp} should be defined; may
   *                 not be null
   * @return the equivalent {@link java.sql.Timestamp}; never null
   */
  protected static java.sql.Timestamp dateTimeOffsetFrom(String value, TimeZone timeZone) {
    ZonedDateTime zdt = ZonedDateTime.parse(value, DATE_TIME_FORMATTER);
    zdt = zdt.withZoneSameInstant(timeZone.toZoneId());
    return java.sql.Timestamp.from(zdt.toInstant());
  }

  @Override
  protected String getSqlType(SinkRecordField field) {
    if (field.schemaName() != null) {
      switch (field.schemaName()) {
        case Decimal.LOGICAL_NAME:
          return "decimal(38," + field.schemaParameters().get(Decimal.SCALE_FIELD) + ")";
        case Date.LOGICAL_NAME:
          return "date";
        case Time.LOGICAL_NAME:
          return "time";
        case Timestamp.LOGICAL_NAME:
          return "datetime2";
        default:
          // pass through to normal types
      }
    }
    switch (field.schemaType()) {
      case INT8:
        return "tinyint";
      case INT16:
        return "smallint";
      case INT32:
        return "int";
      case INT64:
        return "bigint";
      case FLOAT32:
        return "real";
      case FLOAT64:
        return "float";
      case BOOLEAN:
        return "bit";
      case STRING:
        if (field.isPrimaryKey()) {
          // Should be no more than 900 which is the MSSQL constraint
          return "varchar(900)";
        } else {
          return "varchar(max)";
        }
      case BYTES:
        return "varbinary(max)";
      default:
        return super.getSqlType(field);
    }
  }

  @Override
  public String buildDropTableStatement(
      TableId table,
      DropOptions options
  ) {
    ExpressionBuilder builder = expressionBuilder();

    if (options.ifExists()) {
      builder.append("IF OBJECT_ID('");
      builder.append(table);
      builder.append(", 'U') IS NOT NULL");
    }
    // SQL Server 2016 supports IF EXISTS
    builder.append("DROP TABLE ");
    builder.append(table);
    if (options.cascade()) {
      builder.append(" CASCADE");
    }
    return builder.toString();
  }

  @Override
  public List<String> buildAlterTable(
      TableId table,
      Collection<SinkRecordField> fields
  ) {
    ExpressionBuilder builder = expressionBuilder();
    builder.append("ALTER TABLE ");
    builder.append(table);
    builder.append(" ADD");
    writeColumnsSpec(builder, fields);
    return Collections.singletonList(builder.toString());
  }

  @Override
  public String buildUpsertQueryStatement(
      TableId table,
      Collection<ColumnId> keyColumns,
      Collection<ColumnId> nonKeyColumns
  ) {
    ExpressionBuilder builder = expressionBuilder();
    builder.append("merge into ");
    builder.append(table);
    builder.append(" with (HOLDLOCK) AS target using (select ");
    builder.appendList()
           .delimitedBy(", ")
           .transformedBy(ExpressionBuilder.columnNamesWithPrefix("? AS "))
           .of(keyColumns, nonKeyColumns);
    builder.append(") AS incoming on (");
    builder.appendList()
           .delimitedBy(" and ")
           .transformedBy(this::transformAs)
           .of(keyColumns);
    builder.append(")");
    if (nonKeyColumns != null && !nonKeyColumns.isEmpty()) {
      builder.append(" when matched then update set ");
      builder.appendList()
             .delimitedBy(",")
             .transformedBy(this::transformUpdate)
             .of(nonKeyColumns);
    }
    builder.append(" when not matched then insert (");
    builder.appendList()
           .delimitedBy(", ")
           .transformedBy(ExpressionBuilder.columnNames())
           .of(nonKeyColumns, keyColumns);
    builder.append(") values (");
    builder.appendList()
           .delimitedBy(",")
           .transformedBy(ExpressionBuilder.columnNamesWithPrefix("incoming."))
           .of(nonKeyColumns, keyColumns);
    builder.append(");");
    return builder.toString();
  }

  @Override
  protected ColumnDefinition columnDefinition(
      ResultSet resultSet,
      ColumnId id,
      int jdbcType,
      String typeName,
      String classNameForType,
      Nullability nullability,
      Mutability mutability,
      int precision,
      int scale,
      Boolean signedNumbers,
      Integer displaySize,
      Boolean autoIncremented,
      Boolean caseSensitive,
      Boolean searchable,
      Boolean currency,
      Boolean isPrimaryKey
  ) {
    try {
      String isAutoIncremented = resultSet.getString(22);

      if ("yes".equalsIgnoreCase(isAutoIncremented)) {
        autoIncremented = Boolean.TRUE;
      } else if ("no".equalsIgnoreCase(isAutoIncremented)) {
        autoIncremented = Boolean.FALSE;
      }
    } catch (SQLException e) {
      log.warn("Unable to get auto incrementing column information", e);
    }

    return super.columnDefinition(
      resultSet,
      id,
      jdbcType,
      typeName,
      classNameForType,
      nullability,
      mutability,
      precision,
      scale,
      signedNumbers,
      displaySize,
      autoIncremented,
      caseSensitive,
      searchable,
      currency,
      isPrimaryKey
    );
  }

  private void transformAs(ExpressionBuilder builder, ColumnId col) {
    builder.append("target.")
           .appendColumnName(col.name())
           .append("=incoming.")
           .appendColumnName(col.name());
  }

  private void transformUpdate(ExpressionBuilder builder, ColumnId col) {
    builder.appendColumnName(col.name())
           .append("=incoming.")
           .appendColumnName(col.name());
  }

  @Override
  protected String sanitizedUrl(String url) {
    // SQL Server has semicolon delimited property name-value pairs, and several properties
    // that contain secrets
    return super.sanitizedUrl(url)
                .replaceAll("(?i)(;password=)[^;]*", "$1****")
                .replaceAll("(?i)(;keyStoreSecret=)[^;]*", "$1****")
                .replaceAll("(?i)(;gsscredential=)[^;]*", "$1****");
  }
}
