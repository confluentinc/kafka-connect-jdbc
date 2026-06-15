/*
 * Copyright 2026 Confluent Inc.
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
import io.confluent.connect.jdbc.sink.metadata.SinkRecordField;
import io.confluent.connect.jdbc.source.ColumnMapping;
import io.confluent.connect.jdbc.source.JdbcSourceConnectorConfig;
import io.confluent.connect.jdbc.util.ColumnDefinition;
import io.confluent.connect.jdbc.util.ColumnDefinition.Mutability;
import io.confluent.connect.jdbc.util.ColumnDefinition.Nullability;
import io.confluent.connect.jdbc.util.ColumnId;
import io.confluent.connect.jdbc.util.TableId;

import org.apache.kafka.connect.data.Date;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.data.Time;
import org.apache.kafka.connect.data.Timestamp;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

import java.math.BigDecimal;
import java.sql.Array;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Unit tests for the PostgreSQL complex-type support gated behind
 * {@code sql.complex.types.enable}: native array element mapping (source schema, sink DDL, and
 * value binding), the JSON/HSTORE handling modes, and the multi-dimensional-array guard.
 *
 * <p>Deliberately standalone (does not extend {@code BaseDialectTest}) so that only the behavior
 * added by this feature is exercised, with no re-run of the generic dialect test suite.
 */
public class PostgreSqlComplexTypesDialectTest {

  // ---------------------------------------------------------------------------
  // Source: array element schema mapping (addFieldToSchema)
  // ---------------------------------------------------------------------------

  @Test
  public void shouldMapSupportedArrayElementTypesToSourceSchema() {
    assertArrayElement("_text", Schema.Type.STRING, null);
    assertArrayElement("_varchar", Schema.Type.STRING, null);
    assertArrayElement("_bpchar", Schema.Type.STRING, null);
    assertArrayElement("_int2", Schema.Type.INT16, null);
    assertArrayElement("_int4", Schema.Type.INT32, null);
    assertArrayElement("_int8", Schema.Type.INT64, null);
    assertArrayElement("_float4", Schema.Type.FLOAT32, null);
    assertArrayElement("_float8", Schema.Type.FLOAT64, null);
    assertArrayElement("_bool", Schema.Type.BOOLEAN, null);
    assertArrayElement("_numeric", Schema.Type.STRUCT, VariableScaleDecimal.LOGICAL_NAME);
    assertArrayElement("_json", Schema.Type.STRING, Json.LOGICAL_NAME);
    assertArrayElement("_jsonb", Schema.Type.STRING, Json.LOGICAL_NAME);
    assertArrayElement("_date", Schema.Type.INT32, Date.LOGICAL_NAME);
    assertArrayElement("_time", Schema.Type.INT32, Time.LOGICAL_NAME);
    assertArrayElement("_timestamp", Schema.Type.INT64, Timestamp.LOGICAL_NAME);
    // timestamptz carries the zone via the ZonedTimestamp logical STRING (not the built-in Timestamp)
    assertArrayElement("_timestamptz", Schema.Type.STRING, ZonedTimestamp.LOGICAL_NAME);
  }

  @Test
  public void shouldSkipUnsupportedArrayElementTypes() {
    // uuid[]/inet[]/money[] are not in the supported element set, so the column is skipped entirely.
    assertNull(sourceFieldSchema(complexTypesDialect(), Types.ARRAY, "_uuid"));
    assertNull(sourceFieldSchema(complexTypesDialect(), Types.ARRAY, "_inet"));
    assertNull(sourceFieldSchema(complexTypesDialect(), Types.ARRAY, "_money"));
  }

  @Test
  public void shouldDropArraysWhenComplexTypesDisabled() {
    // Backward compatible: with the feature off the column is skipped, as before the change.
    assertNull(sourceFieldSchema(featureDisabledDialect(), Types.ARRAY, "_int4"));
  }

  // ---------------------------------------------------------------------------
  // Source: json.handling.mode / hstore.handling.mode schema selection
  // ---------------------------------------------------------------------------

  @Test
  public void jsonHandlingModeShouldSelectSourceSchema() {
    // Default is "string": a logical JSON STRING tagged with the Json logical name.
    Schema stringMode = sourceFieldSchema(complexTypesDialect(), Types.OTHER, "jsonb");
    assertEquals(Schema.Type.STRING, stringMode.type());
    assertEquals(Json.LOGICAL_NAME, stringMode.name());

    // "map": a shallow Map<String,String>.
    PostgreSqlDatabaseDialect mapDialect = complexTypesDialect(
        JdbcSourceConnectorConfig.JSON_HANDLING_MODE_CONFIG,
        JdbcSourceConnectorConfig.JSON_HANDLING_MODE_MAP);
    Schema mapMode = sourceFieldSchema(mapDialect, Types.OTHER, "json");
    assertEquals(Schema.Type.MAP, mapMode.type());
  }

  @Test
  public void hstoreHandlingModeShouldSelectSourceSchema() {
    // Default is "map": a Map<String,String>.
    Schema mapMode = sourceFieldSchema(complexTypesDialect(), Types.OTHER, "hstore");
    assertEquals(Schema.Type.MAP, mapMode.type());

    // "json": a plain (untagged) STRING, which the sink lands in a TEXT column.
    PostgreSqlDatabaseDialect jsonDialect = complexTypesDialect(
        JdbcSourceConnectorConfig.HSTORE_HANDLING_MODE_CONFIG,
        JdbcSourceConnectorConfig.HSTORE_HANDLING_MODE_JSON);
    Schema jsonMode = sourceFieldSchema(jsonDialect, Types.OTHER, "hstore");
    assertEquals(Schema.Type.STRING, jsonMode.type());
    assertNull(jsonMode.name());
  }

  // ---------------------------------------------------------------------------
  // Sink: getSqlType DDL for logical types and arrays
  // ---------------------------------------------------------------------------

  @Test
  public void shouldMapLogicalTypesToSqlTypes() {
    assertSqlType("JSONB", Json.schema());
    assertSqlType("NUMERIC", VariableScaleDecimal.schema());
    assertSqlType("TIMESTAMP WITH TIME ZONE", ZonedTimestamp.schema());
  }

  @Test
  public void shouldMapArrayTypesToNativeSqlArrayTypes() {
    assertSqlType("TEXT[]", arraySchema(Schema.STRING_SCHEMA));
    assertSqlType("SMALLINT[]", arraySchema(Schema.INT16_SCHEMA));
    assertSqlType("INT[]", arraySchema(Schema.INT32_SCHEMA));
    assertSqlType("BIGINT[]", arraySchema(Schema.INT64_SCHEMA));
    assertSqlType("REAL[]", arraySchema(Schema.FLOAT32_SCHEMA));
    assertSqlType("DOUBLE PRECISION[]", arraySchema(Schema.FLOAT64_SCHEMA));
    assertSqlType("BOOLEAN[]", arraySchema(Schema.BOOLEAN_SCHEMA));
    assertSqlType("JSONB[]", arraySchema(Json.optionalSchema()));
    assertSqlType("NUMERIC[]", arraySchema(VariableScaleDecimal.optionalSchema()));
    assertSqlType("DATE[]", arraySchema(Date.builder().optional().build()));
    assertSqlType("TIME[]", arraySchema(Time.builder().optional().build()));
    assertSqlType("TIMESTAMP[]", arraySchema(Timestamp.builder().optional().build()));
    assertSqlType("TIMESTAMP WITH TIME ZONE[]", arraySchema(ZonedTimestamp.optionalSchema()));
  }

  // ---------------------------------------------------------------------------
  // Sink: bindArray builds native typed arrays via createArrayOf
  // ---------------------------------------------------------------------------

  @Test
  public void shouldBindJsonArrayAsNativeJsonbArray() throws Exception {
    verifyArrayBind(
        Json.optionalSchema(),
        Arrays.asList("{\"k\":\"v\"}", "{\"a\":1}"),
        "jsonb",
        new Object[]{"{\"k\":\"v\"}", "{\"a\":1}"});
  }

  @Test
  public void shouldBindNumericArrayAsNativeNumericArray() throws Exception {
    Schema element = VariableScaleDecimal.optionalSchema();
    verifyArrayBind(
        element,
        Arrays.asList(
            VariableScaleDecimal.fromLogical(element, new BigDecimal("1.50")),
            VariableScaleDecimal.fromLogical(element, new BigDecimal("3.14159"))),
        "numeric",
        new Object[]{new BigDecimal("1.50"), new BigDecimal("3.14159")});
  }

  @Test
  public void shouldBindTemporalArraysAsNativeArraysInUtc() throws Exception {
    // epoch 0 == 1970-01-01T00:00:00Z; each element is rendered as a UTC text literal.
    java.util.Date epoch = new java.util.Date(0L);
    verifyArrayBind(Date.builder().optional().build(),
        Collections.singletonList(epoch), "date", new Object[]{"1970-01-01"});
    verifyArrayBind(Time.builder().optional().build(),
        Collections.singletonList(epoch), "time", new Object[]{"00:00:00.000"});
    verifyArrayBind(Timestamp.builder().optional().build(),
        Collections.singletonList(epoch), "timestamp", new Object[]{"1970-01-01 00:00:00.000"});
  }

  @Test
  public void shouldBindZonedTimestampArrayAsNativeTimestamptzArray() throws Exception {
    // ZonedTimestamp elements are already ISO-8601 offset strings; they bind straight into a
    // native timestamptz[] column.
    verifyArrayBind(
        ZonedTimestamp.optionalSchema(),
        Collections.singletonList("2025-06-10T13:00:00Z"),
        "timestamptz",
        new Object[]{"2025-06-10T13:00:00Z"});
  }

  // ---------------------------------------------------------------------------
  // Source: multi-dimensional array guard
  // ---------------------------------------------------------------------------

  @Test
  public void shouldSkipMultiDimensionalArraysWithoutFailing() throws Exception {
    DatabaseDialect.ColumnConverter converter = arrayColumnConverter("_int4");

    // Single-dimension int[] is read into a list of elements.
    ResultSet single = arrayResultSet(new Object[]{1, 2, 3});
    assertEquals(Arrays.asList(1, 2, 3), converter.convert(single));

    // Multi-dimension int[][] is skipped (null) rather than crashing the task.
    ResultSet multi = arrayResultSet(new Object[]{new Integer[]{1, 2}, new Integer[]{3}});
    assertNull(converter.convert(multi));
  }

  @Test
  public void arrayConverterRoutesByElementLogicalType() throws Exception {
    // numeric[] -> each element decoded into a VariableScaleDecimal struct (per-value scale).
    Object num = ((List<?>) arrayColumnConverter("_numeric")
        .convert(arrayResultSet(new Object[]{new BigDecimal("1.5")}))).get(0);
    assertEquals(0, new BigDecimal("1.5").compareTo(
        VariableScaleDecimal.toLogical((Struct) num)));

    // jsonb[] -> each element carried as its raw JSON text.
    Object json = ((List<?>) arrayColumnConverter("_jsonb")
        .convert(arrayResultSet(new Object[]{"{\"k\":1}"}))).get(0);
    assertEquals("{\"k\":1}", json);
  }

  // ---------------------------------------------------------------------------
  // Helpers
  // ---------------------------------------------------------------------------

  private PostgreSqlDatabaseDialect complexTypesDialect(String... extraProps) {
    Map<String, String> props = baseSourceProps();
    props.put(JdbcSourceConnectorConfig.SQL_COMPLEX_TYPES_ENABLE_CONFIG, "true");
    for (int i = 0; i < extraProps.length; i += 2) {
      props.put(extraProps[i], extraProps[i + 1]);
    }
    return new PostgreSqlDatabaseDialect(new JdbcSourceConnectorConfig(props));
  }

  private PostgreSqlDatabaseDialect featureDisabledDialect() {
    return new PostgreSqlDatabaseDialect(new JdbcSourceConnectorConfig(baseSourceProps()));
  }

  private static Map<String, String> baseSourceProps() {
    Map<String, String> props = new HashMap<>();
    props.put(JdbcSourceConnectorConfig.CONNECTION_URL_CONFIG, "jdbc:postgresql://something");
    props.put(JdbcSourceConnectorConfig.MODE_CONFIG, JdbcSourceConnectorConfig.MODE_BULK);
    props.put(JdbcSourceConnectorConfig.TOPIC_PREFIX_CONFIG, "test-");
    return props;
  }

  private static Schema arraySchema(Schema elementSchema) {
    return SchemaBuilder.array(elementSchema).build();
  }

  private void assertArrayElement(String pgArrayType, Schema.Type elementType, String elementName) {
    Schema schema = sourceFieldSchema(complexTypesDialect(), Types.ARRAY, pgArrayType);
    assertNotNull("Array column " + pgArrayType + " should produce a field", schema);
    assertEquals(Schema.Type.ARRAY, schema.type());
    assertEquals(elementType, schema.valueSchema().type());
    assertEquals(elementName, schema.valueSchema().name());
  }

  private Schema sourceFieldSchema(PostgreSqlDatabaseDialect dialect, int jdbcType, String typeName) {
    ColumnDefinition column = column(jdbcType, typeName);
    SchemaBuilder builder = SchemaBuilder.struct();
    String fieldName = dialect.addFieldToSchema(column, builder);
    return fieldName == null ? null : builder.build().field(fieldName).schema();
  }

  private void assertSqlType(String expected, Schema schema) {
    SinkRecordField field = new SinkRecordField(schema, "col", schema.isOptional());
    assertEquals(expected, complexTypesDialect().getSqlType(field));
  }

  private ColumnDefinition column(int jdbcType, String typeName) {
    return new ColumnDefinition(
        new ColumnId(new TableId(null, null, "t"), "col"),
        jdbcType, typeName, Object.class.getName(),
        Nullability.NULL, Mutability.UNKNOWN,
        0, 0, false, 1, false, false, false, false, false);
  }

  private DatabaseDialect.ColumnConverter arrayColumnConverter(String pgArrayType) {
    ColumnDefinition column = column(Types.ARRAY, pgArrayType);
    ColumnMapping mapping = new ColumnMapping(
        column, 1, new Field("col", 0, arraySchema(Schema.OPTIONAL_INT32_SCHEMA)));
    DatabaseDialect.ColumnConverter converter =
        complexTypesDialect().columnConverterFor(mapping, column, 1, true);
    assertNotNull(converter);
    return converter;
  }

  private static ResultSet arrayResultSet(Object[] elements) throws SQLException {
    ResultSet resultSet = mock(ResultSet.class);
    Array array = mock(Array.class);
    when(resultSet.getArray(1)).thenReturn(array);
    when(array.getArray()).thenReturn(elements);
    return resultSet;
  }

  private void verifyArrayBind(
      Schema elementSchema,
      List<?> value,
      String expectedPgType,
      Object[] expectedElements
  ) throws SQLException {
    PreparedStatement statement = mock(PreparedStatement.class);
    Connection connection = mock(Connection.class);
    Array boundArray = mock(Array.class);
    ColumnDefinition colDef = mock(ColumnDefinition.class);
    when(colDef.type()).thenReturn(Types.ARRAY);
    when(statement.getConnection()).thenReturn(connection);
    when(connection.createArrayOf(eq(expectedPgType), any())).thenReturn(boundArray);

    Schema arraySchema = SchemaBuilder.array(elementSchema).optional().build();
    complexTypesDialect().bindField(statement, 7, arraySchema, value, colDef, "field");

    ArgumentCaptor<Object[]> captor = ArgumentCaptor.forClass(Object[].class);
    verify(connection).createArrayOf(eq(expectedPgType), captor.capture());
    assertEquals(Arrays.asList(expectedElements), Arrays.asList(captor.getValue()));
    verify(statement).setArray(7, boundArray);
  }
}
