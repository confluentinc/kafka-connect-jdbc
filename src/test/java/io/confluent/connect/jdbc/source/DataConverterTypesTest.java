/**
 * Copyright 2015 Confluent Inc.
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

import org.apache.kafka.connect.data.Decimal;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnit;
import org.mockito.junit.MockitoRule;

import javax.sql.rowset.RowSetMetaDataImpl;
import java.math.BigDecimal;
import java.sql.Clob;
import java.sql.Blob;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.Arrays;
import java.util.Collection;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.when;

@RunWith(Parameterized.class)
public class DataConverterTypesTest {

  private static final int TEST_SCALE = 2;
  private Schema schemaType;
  private int javaSqlType;
  private Object value;

  @Parameterized.Parameters
  public static Collection<Object[]> data() {
    return Arrays.asList(new Object[][]{
            {Schema.BOOLEAN_SCHEMA, Types.BOOLEAN, true, false},
            {Schema.OPTIONAL_BOOLEAN_SCHEMA, Types.BOOLEAN, true, true},

            {Schema.INT8_SCHEMA, Types.BIT, Byte.MAX_VALUE, false},
            {Schema.OPTIONAL_INT8_SCHEMA, Types.BIT, Byte.MAX_VALUE, true},

            {Schema.INT8_SCHEMA, Types.TINYINT, Byte.MAX_VALUE, false},
            {Schema.OPTIONAL_INT8_SCHEMA, Types.TINYINT, Byte.MAX_VALUE, true},

            {Schema.INT16_SCHEMA, Types.SMALLINT, Short.MAX_VALUE, false},
            {Schema.OPTIONAL_INT16_SCHEMA, Types.SMALLINT, Short.MAX_VALUE, true},

            {Schema.INT32_SCHEMA, Types.INTEGER, 12, false},
            {Schema.OPTIONAL_INT32_SCHEMA, Types.INTEGER, 12, true},

            {Schema.INT64_SCHEMA, Types.BIGINT, 14L, true},
            {Schema.OPTIONAL_INT64_SCHEMA, Types.BIGINT, 14L, true},

            {Schema.FLOAT32_SCHEMA, Types.REAL, 99f, false},
            {Schema.OPTIONAL_FLOAT32_SCHEMA, Types.REAL, 99f, true},

            {Schema.FLOAT64_SCHEMA, Types.DOUBLE, 100d, false},
            {Schema.OPTIONAL_FLOAT64_SCHEMA, Types.DOUBLE, 100d, true},

            {Decimal.builder(TEST_SCALE).build(), Types.NUMERIC, new BigDecimal(17), false},
            {Decimal.builder(TEST_SCALE).optional().build(), Types.NUMERIC, new BigDecimal(17), true},

            {Decimal.builder(TEST_SCALE).build(), Types.DECIMAL, new BigDecimal(19), false},
            {Decimal.builder(TEST_SCALE).optional().build(), Types.DECIMAL, new BigDecimal(19), true},

            {Schema.STRING_SCHEMA, Types.CHAR, "test", false},
            {Schema.OPTIONAL_STRING_SCHEMA, Types.CHAR, "test", true},

            {Schema.STRING_SCHEMA, Types.VARCHAR, "test", false},
            {Schema.OPTIONAL_STRING_SCHEMA, Types.VARCHAR, "test", true},

            {Schema.STRING_SCHEMA, Types.LONGVARCHAR, "test", false},
            {Schema.OPTIONAL_STRING_SCHEMA, Types.LONGVARCHAR, "test", true}
    });
  }

  private static final int FIRST_COLUMN_INDEX = 1;
  private static final String FIRST_COLUMN_NAME = "column1";
  private static final int COLUMN_COUNT = 1;


  @Rule
  public MockitoRule rule = MockitoJUnit.rule();

  @Mock
  private ResultSet resultSet;

  public DataConverterTypesTest(Schema schemaType, int javaSqlType, Object value, boolean nullable) {
    this.schemaType = schemaType;
    this.javaSqlType = javaSqlType;
    this.value = value;
  }

  @Test
  public void test() throws SQLException {
    Schema schema = SchemaBuilder.struct()
            .field(FIRST_COLUMN_NAME, schemaType)
            .build();
    RowSetMetaDataImpl resultSetMetaData = new RowSetMetaDataImpl();
    resultSetMetaData.setColumnCount(COLUMN_COUNT);
    //BOOLEAN
    resultSetMetaData.setColumnType(FIRST_COLUMN_INDEX, javaSqlType);
    resultSetMetaData.setColumnLabel(FIRST_COLUMN_INDEX, FIRST_COLUMN_NAME);


    mockResultSet(resultSetMetaData);
    Struct struct = DataConverter.convertRecord(schema, resultSet);
    Struct expected = new Struct(schema);
    expected.put(FIRST_COLUMN_NAME, value);
    assertEquals(expected, struct);

  }

  private void mockResultSet(RowSetMetaDataImpl resultSetMetaData) throws SQLException {
    when(resultSet.getMetaData()).thenReturn(resultSetMetaData);

    when(resultSet.getObject(FIRST_COLUMN_INDEX)).thenReturn(value);
    if (value.getClass().isAssignableFrom(Boolean.class)) {
      when(resultSet.getBoolean(FIRST_COLUMN_INDEX)).thenReturn((Boolean) value);
    }
    if (value.getClass().isAssignableFrom(Integer.class)) {
      when(resultSet.getInt(FIRST_COLUMN_INDEX)).thenReturn((Integer) value);
    }
    if (value.getClass().isAssignableFrom(Float.class)) {
      when(resultSet.getDouble(FIRST_COLUMN_INDEX)).thenReturn(((Float) value).doubleValue());
      when(resultSet.getFloat(FIRST_COLUMN_INDEX)).thenReturn((Float) value);
    }
    if (value.getClass().isAssignableFrom(Long.class)) {
      when(resultSet.getLong(FIRST_COLUMN_INDEX)).thenReturn((Long) value);
    }
    if (value.getClass().isAssignableFrom(BigDecimal.class)) {
      when(resultSet.getBigDecimal(FIRST_COLUMN_INDEX)).thenReturn((BigDecimal) value);
    }
    if (value.getClass().isAssignableFrom(Byte.class)) {
      when(resultSet.getByte(FIRST_COLUMN_INDEX)).thenReturn((byte) value);
    }
    if (value.getClass().isAssignableFrom(Clob.class)) {
      when(resultSet.getClob(FIRST_COLUMN_INDEX)).thenReturn((Clob) value);
    }
    if (value.getClass().isAssignableFrom(Blob.class)) {
      when(resultSet.getBlob(FIRST_COLUMN_INDEX)).thenReturn((Blob) value);
    }
    if (value.getClass().isAssignableFrom(Double.class)) {
      when(resultSet.getDouble(FIRST_COLUMN_INDEX)).thenReturn((Double) value);
    }
    if (value.getClass().isAssignableFrom(java.lang.reflect.Array.class)) {
      when(resultSet.getBytes(FIRST_COLUMN_INDEX)).thenReturn((byte[]) this.value);
    }
    if (value.getClass().isAssignableFrom(String.class)) {
      when(resultSet.getString(FIRST_COLUMN_INDEX)).thenReturn((String) value);
    }
    if (value.getClass().isAssignableFrom(Time.class)) {
      when(resultSet.getTime(FIRST_COLUMN_INDEX)).thenReturn((Time) value);
    }
    if (value.getClass().isAssignableFrom(Timestamp.class)) {
      when(resultSet.getTimestamp(FIRST_COLUMN_INDEX)).thenReturn((Timestamp) value);
    }
    if (value.getClass().isAssignableFrom(Short.class)) {
      when(resultSet.getShort(FIRST_COLUMN_INDEX)).thenReturn((Short) value);
    }
  }
}