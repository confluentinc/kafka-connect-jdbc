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

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnit;
import org.mockito.junit.MockitoRule;

import javax.sql.rowset.RowSetMetaDataImpl;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.when;

public class DataConverterTest {


  @Rule
  public MockitoRule rule = MockitoJUnit.rule();

  @Mock
  private ResultSet resultSet;

  @Test
  public void convertRecordEmptyResultsetMetadata() throws Exception {
    Schema schema = SchemaBuilder.struct()
            .field("name", Schema.STRING_SCHEMA)
            .build();
    ResultSetMetaData resultSetMetaData = new RowSetMetaDataImpl();
    when(resultSet.getMetaData()).thenReturn(resultSetMetaData);
    Struct struct = DataConverter.convertRecord(schema, resultSet);
    assertEquals(new Struct(schema), struct);

  }

  @Test(expected = NullPointerException.class)
  public void convertRecordNullArgs() throws Exception {
    DataConverter.convertRecord(null, null);
  }

  @Test(expected = NullPointerException.class)
  public void convertRecordResultSetIsNull() throws Exception {
    Schema schema = SchemaBuilder.struct()
            .build();
    DataConverter.convertRecord(schema, null);
  }

  @Test(expected = NullPointerException.class)
  public void convertRecordSchemaIsNull() throws Exception {
    DataConverter.convertRecord(null, resultSet);
  }



}