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

package io.confluent.connect.jdbc;

import org.apache.kafka.connect.data.*;
import org.apache.kafka.connect.data.Schema.Type;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import javax.sql.rowset.serial.SerialBlob;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.util.Calendar;
import java.util.GregorianCalendar;
import java.util.List;
import java.util.TimeZone;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

// Tests conversion of Oracle specific data types, namely {@code NUMBER}.
public class JdbcSourceTaskConversionOracleTest extends JdbcSourceTaskTestOracleBase {

  @Before
  public void setup() throws Exception {
    super.setup();
    task.start(singleTableConfig());
  }

  @After
  public void tearDown() throws Exception {
    task.stop();
    super.tearDown();
  }

  @Test
  public void testLong() throws Exception {
    typeConversion("NUMBER(1,0)", false, 1, Schema.INT64_SCHEMA, 1l);
    typeConversion("NUMBER(2,0)", false, 1, Schema.INT64_SCHEMA, 1l);
    typeConversion("NUMBER(4,0)", false, 1, Schema.INT64_SCHEMA, 1l);
    typeConversion("NUMBER(8,0)", false, 1, Schema.INT64_SCHEMA, 1l);
    typeConversion("NUMBER(16,0)", false, 1, Schema.INT64_SCHEMA, 1l);
    typeConversion("NUMBER(32,0)", false, 1, Schema.INT64_SCHEMA, 1l);
    typeConversion("NUMBER(38,0)", false, 1, Schema.INT64_SCHEMA, 1l);
  }

  @Test
  public void testNullableLong() throws Exception {
    typeConversion("NUMBER(38,0)", true, 1, Schema.OPTIONAL_INT64_SCHEMA, 1l);
    typeConversion("NUMBER(38,0)", true, null, Schema.OPTIONAL_INT64_SCHEMA, null);
  }

  // Derby has an XML type, but the JDBC driver doesn't implement any of the type bindings,

  private void typeConversion(String sqlType, boolean nullable,
                              Object sqlValue, Schema convertedSchema,
                              Object convertedValue) throws Exception {
    try
    {
      String sqlColumnSpec = sqlType;
      if (!nullable)
      {
        sqlColumnSpec += " NOT NULL";
      }
      db.createTable(SINGLE_TABLE_NAME, "id", sqlColumnSpec);
      db.insert(SINGLE_TABLE_NAME, "id", sqlValue);
      List<SourceRecord> records = task.poll();
      validateRecords(records, convertedSchema, convertedValue);
    } finally
    {
      db.dropTable(SINGLE_TABLE_NAME);
    }
  }

  /**
   * Validates schema and type of returned record data. Assumes single-field values since this is
   * only used for validating type information.
   */
  private void validateRecords(List<SourceRecord> records, Schema expectedFieldSchema,
                               Object expectedValue) {
    // Validate # of records and object type
    assertEquals(1, records.size());
    Object objValue = records.get(0).value();
    assertTrue(objValue instanceof Struct);
    Struct value = (Struct) objValue;

    // Validate schema
    Schema schema = value.schema();
    assertEquals(Type.STRUCT, schema.type());
    List<Field> fields = schema.fields();

    assertEquals(1, fields.size());

    Schema fieldSchema = fields.get(0).schema();
    assertEquals(expectedFieldSchema, fieldSchema);
    if (expectedValue instanceof byte[]) {
      assertTrue(value.get(fields.get(0)) instanceof byte[]);
      assertEquals(ByteBuffer.wrap((byte[])expectedValue),
                   ByteBuffer.wrap((byte[])value.get(fields.get(0))));
    } else {
      assertEquals(expectedValue, value.get(fields.get(0)));
    }
  }

}
