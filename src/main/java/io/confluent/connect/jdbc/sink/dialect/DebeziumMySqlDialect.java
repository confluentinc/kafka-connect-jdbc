package io.confluent.connect.jdbc.sink.dialect;

import io.confluent.connect.jdbc.sink.metadata.FieldsMetadata;
import io.confluent.connect.jdbc.sink.metadata.SinkRecordField;
import org.apache.kafka.common.protocol.types.SchemaException;
import org.apache.kafka.connect.data.*;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import static io.confluent.connect.jdbc.sink.dialect.StringBuilderUtil.copiesToBuilder;
import static io.confluent.connect.jdbc.sink.dialect.StringBuilderUtil.joinToBuilder;


public class DebeziumMySqlDialect extends DbDialect {

  public DebeziumMySqlDialect() {
    super("`", "`");
  }

  public static final HashMap<String, String> SCHEMA_NAME_CASTING_MAP;

  static {
    SCHEMA_NAME_CASTING_MAP = new HashMap<>();
    SCHEMA_NAME_CASTING_MAP.put("io.debezium.data.Bits", "?");
    SCHEMA_NAME_CASTING_MAP.put("io.debezium.data.Json", "?");
    SCHEMA_NAME_CASTING_MAP.put("io.debezium.data.Enum", "?");
    SCHEMA_NAME_CASTING_MAP.put("io.debezium.data.EnumSet", "?");
    SCHEMA_NAME_CASTING_MAP.put("io.debezium.time.Date", "MAKEDATE(1970, ?)");
    SCHEMA_NAME_CASTING_MAP.put("io.debezium.time.Timestamp", "FROM_UNIXTIME(? / 1000)");
    SCHEMA_NAME_CASTING_MAP.put("io.debezium.time.ZonedTimestamp", "?");
    SCHEMA_NAME_CASTING_MAP.put("io.debezium.time.MicroTimestamp", "FROM_UNIXTIME(FLOOR(? / 1000))");
    SCHEMA_NAME_CASTING_MAP.put("io.debezium.time.MicroTime", "SEC_TO_TIME(FLOOR(? / 1000))");
    SCHEMA_NAME_CASTING_MAP.put("io.debezium.time.Time", "SEC_TO_TIME(?)");
    SCHEMA_NAME_CASTING_MAP.put("io.debezium.time.Year", "?");
  }

  public static final HashMap<String, String> SCHEMA_NAME_DATATYPE_MAP;

  static {
    // mappings were taken from the documentation at http://debezium.io/docs/connectors/mysql/
    SCHEMA_NAME_DATATYPE_MAP = new HashMap<>();
    SCHEMA_NAME_DATATYPE_MAP.put("io.debezium.data.Bits", "VARBINARY(1024)");
    SCHEMA_NAME_DATATYPE_MAP.put("io.debezium.data.Json", "TEXT");
    // constraint for enums is enforced by the source data
    SCHEMA_NAME_DATATYPE_MAP.put("io.debezium.data.Enum", "TEXT");
    SCHEMA_NAME_DATATYPE_MAP.put("io.debezium.data.EnumSet", "TEXT");
    SCHEMA_NAME_DATATYPE_MAP.put("io.debezium.time.Date", "DATE");
    SCHEMA_NAME_DATATYPE_MAP.put("io.debezium.time.Timestamp", "DATETIME");
    SCHEMA_NAME_DATATYPE_MAP.put("io.debezium.time.ZonedTimestamp", "DATETIME");
    SCHEMA_NAME_DATATYPE_MAP.put("io.debezium.time.MicroTimestamp", "DATETIME");
    SCHEMA_NAME_DATATYPE_MAP.put("io.debezium.time.MicroTime", "TIME");
    SCHEMA_NAME_DATATYPE_MAP.put("io.debezium.time.Time", "TIME");
    SCHEMA_NAME_DATATYPE_MAP.put("io.debezium.time.Year", "YEAR");
  }

  @Override
  protected String getSqlType(String schemaName, Map<String, String> parameters, Schema.Type type) {

    if (schemaName != null && schemaName.startsWith("io.debezium")) {
      // TODO maybe throw new SchemaException("There is no debezium data type mapping for schema name " + schemaName); ?
      return SCHEMA_NAME_DATATYPE_MAP.get(schemaName);
    }
    return new MySqlDialect().getSqlType(schemaName, parameters, type);

  }

  private String makeUpsertPlaceholders(
      final Collection<String> keyCols,
      final Collection<String> cols,
      final FieldsMetadata fieldsMetadata
  ) {
    final StringBuilder builder = new StringBuilder();
    this.addUpsertPlaceholders(keyCols, fieldsMetadata, builder);
    this.addUpsertPlaceholders(cols, fieldsMetadata, builder);
    builder.setLength(builder.length() - 2); // remove trailing ", "
    return builder.toString();
  }

  public void addUpsertPlaceholders(
      final Collection<String> cols,
      final FieldsMetadata fieldsMetadata,
      StringBuilder builder
  ) {
    for (String col : cols) {
      String schemaName = fieldsMetadata.allFields.get(col).schemaName();
      if (schemaName == null) {
        builder.append("?, ");
        continue;
      }
      // FIXME are there schemata whose names don't start with io.debezium??
      builder.append(SCHEMA_NAME_CASTING_MAP.get(schemaName));
      builder.append(", ");
    }
  }


  @Override
  public String getUpsertQuery(
      final String table,
      final Collection<String> keyCols,
      final Collection<String> cols,
      final FieldsMetadata fieldsMetadata

  ) {
    //MySql doesn't support SQL 2003:merge so here how the upsert is handled

    final StringBuilder builder = new StringBuilder();
    builder.append("INSERT INTO ");
    builder.append(escaped(table));
    builder.append("(");
    joinToBuilder(builder, ",", keyCols, cols, escaper());
    builder.append(") VALUES (");

    builder.append(this.makeUpsertPlaceholders(keyCols, cols, fieldsMetadata));

    builder.append(") ON DUPLICATE KEY UPDATE ");
     // TODO FIXME: Do we also need to apply conversions to the ON DUPLICATE KEY statements or will they be taken from
    // the initial columns we provide?
    joinToBuilder(
        builder,
        ",",
        cols.isEmpty() ? keyCols : cols,
        new StringBuilderUtil.Transform<String>() {
          @Override
          public void apply(StringBuilder builder, String col) {
            builder.append(escaped(col)).append("=VALUES(").append(escaped(col)).append(")");
          }
        }
    );
    return builder.toString();
  }

}
