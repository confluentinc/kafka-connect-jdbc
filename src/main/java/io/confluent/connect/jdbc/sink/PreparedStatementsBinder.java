/*
 * Copyright 2016 Confluent Inc.
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
 */

package io.confluent.connect.jdbc.sink;

import org.apache.kafka.connect.data.Date;
import org.apache.kafka.connect.data.Decimal;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.data.Time;
import org.apache.kafka.connect.data.Timestamp;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.SinkRecord;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.sql.PreparedStatement;
import java.sql.SQLException;

import io.confluent.connect.jdbc.sink.metadata.FieldsMetadata;
import io.confluent.connect.jdbc.util.DateTimeUtils;

public class PreparedStatementsBinder {

  public void bindRecord(
      JdbcSinkConfig config,
      PreparedStatement statement,
      PreparedStatement deleteStatement,
      FieldsMetadata fieldsMetadata,
      SinkRecord record
  ) throws SQLException {
    final Struct valueStruct = (Struct) record.value();

    // Assumption: the relevant SQL has placeholders for keyFieldNames first followed by nonKeyFieldNames, in iteration order for all INSERT/ UPSERT queries
    //             the relevant SQL has placeholders for keyFieldNames, in iteration order for all DELETE queries
    //             the relevant SQL has placeholders for nonKeyFieldNames first followed by keyFieldNames, in iteration order for all UPDATE queries

    int index = 1;
    if (valueStruct == null && config.deleteEnabled) {
      bindKeyFields(config.pkMode, deleteStatement, fieldsMetadata, record, index);
      deleteStatement.addBatch();
    } else {
      switch (config.insertMode) {
        case INSERT:
        case UPSERT:
          index = bindKeyFields(config.pkMode, statement, fieldsMetadata, record, index);
          bindNonKeyFields(statement, fieldsMetadata, record, valueStruct, index);
          break;

        case UPDATE:
          index = bindNonKeyFields(statement, fieldsMetadata, record, valueStruct, index);
          bindKeyFields(config.pkMode, statement, fieldsMetadata, record, index);
          break;
        default:
          throw new AssertionError();

      }
      statement.addBatch();
    }
  }

  private int bindKeyFields(
      JdbcSinkConfig.PrimaryKeyMode pkMode,
      PreparedStatement statement,
      FieldsMetadata fieldsMetadata,
      SinkRecord record,
      int index) throws SQLException {
    switch (pkMode) {
      case NONE:
        if (!fieldsMetadata.keyFieldNames.isEmpty()) {
          throw new AssertionError();
        }
        break;

      case KAFKA: {
        assert fieldsMetadata.keyFieldNames.size() == 3;
        bindField(statement, index++, Schema.STRING_SCHEMA, record.topic());
        bindField(statement, index++, Schema.INT32_SCHEMA, record.kafkaPartition());
        bindField(statement, index++, Schema.INT64_SCHEMA, record.kafkaOffset());
      }
      break;

      case RECORD_KEY: {
        if (record.keySchema().type().isPrimitive()) {
          assert fieldsMetadata.keyFieldNames.size() == 1;
          bindField(statement, index++, record.keySchema(), record.key());
        } else {
          for (String fieldName : fieldsMetadata.keyFieldNames) {
            final Field field = record.keySchema().field(fieldName);
            bindField(statement, index++, field.schema(), ((Struct) record.key()).get(field));
          }
        }
      }
      break;

      case RECORD_VALUE: {
        for (String fieldName : fieldsMetadata.keyFieldNames) {
          final Field field = record.valueSchema().field(fieldName);
          bindField(statement, index++, field.schema(), ((Struct) record.value()).get(field));
        }
      }
      break;
    }
    return index;
  }

  private int bindNonKeyFields(
      PreparedStatement statement,
      FieldsMetadata fieldsMetadata,
      SinkRecord record,
      Struct valueStruct,
      int index) throws SQLException {
    for (final String fieldName : fieldsMetadata.nonKeyFieldNames) {
      final Field field = record.valueSchema().field(fieldName);
      bindField(statement, index++, field.schema(), valueStruct.get(field));
    }
    return index;
  }

  static void bindField(PreparedStatement statement, int index, Schema schema, Object value) throws SQLException {
    if (value == null) {
      statement.setObject(index, null);
    } else {
      final boolean bound = maybeBindLogical(statement, index, schema, value);
      if (!bound) {
        switch (schema.type()) {
          case INT8:
            statement.setByte(index, (Byte) value);
            break;
          case INT16:
            statement.setShort(index, (Short) value);
            break;
          case INT32:
            statement.setInt(index, (Integer) value);
            break;
          case INT64:
            statement.setLong(index, (Long) value);
            break;
          case FLOAT32:
            statement.setFloat(index, (Float) value);
            break;
          case FLOAT64:
            statement.setDouble(index, (Double) value);
            break;
          case BOOLEAN:
            statement.setBoolean(index, (Boolean) value);
            break;
          case STRING:
            statement.setString(index, (String) value);
            break;
          case BYTES:
            final byte[] bytes;
            if (value instanceof ByteBuffer) {
              final ByteBuffer buffer = ((ByteBuffer) value).slice();
              bytes = new byte[buffer.remaining()];
              buffer.get(bytes);
            } else {
              bytes = (byte[]) value;
            }
            statement.setBytes(index, bytes);
            break;
          default:
            throw new ConnectException("Unsupported source data type: " + schema.type());
        }
      }
    }
  }

  static boolean maybeBindLogical(PreparedStatement statement, int index, Schema schema, Object value) throws SQLException {
    if (schema.name() != null) {
      switch (schema.name()) {
        case Date.LOGICAL_NAME:
          statement.setDate(index, new java.sql.Date(((java.util.Date) value).getTime()), DateTimeUtils.UTC_CALENDAR.get());
          return true;
        case Decimal.LOGICAL_NAME:
          statement.setBigDecimal(index, (BigDecimal) value);
          return true;
        case Time.LOGICAL_NAME:
          statement.setTime(index, new java.sql.Time(((java.util.Date) value).getTime()), DateTimeUtils.UTC_CALENDAR.get());
          return true;
        case Timestamp.LOGICAL_NAME:
          statement.setTimestamp(index, new java.sql.Timestamp(((java.util.Date) value).getTime()), DateTimeUtils.UTC_CALENDAR.get());
          return true;
        default:
          return false;
      }
    }
    return false;
  }

}
