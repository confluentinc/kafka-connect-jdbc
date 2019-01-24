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

import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.SinkRecord;

import java.sql.PreparedStatement;
import java.sql.SQLException;

import io.confluent.connect.jdbc.dialect.DatabaseDialect;
import io.confluent.connect.jdbc.dialect.DatabaseDialect.StatementBinder;
import io.confluent.connect.jdbc.sink.metadata.FieldsMetadata;
import io.confluent.connect.jdbc.sink.metadata.SchemaPair;

public class PreparedStatementBinder implements StatementBinder {

  private final JdbcSinkConfig.PrimaryKeyMode pkMode;
  private final PreparedStatement statement;
  private final PreparedStatement deleteStatement;
  private final SchemaPair schemaPair;
  private final FieldsMetadata fieldsMetadata;
  private final JdbcSinkConfig.InsertMode insertMode;
  private final JdbcSinkConfig config;
  private final DatabaseDialect dialect;

  public PreparedStatementBinder(
      DatabaseDialect dialect,
      PreparedStatement statement,
      PreparedStatement deleteStatement,
      JdbcSinkConfig.PrimaryKeyMode pkMode,
      SchemaPair schemaPair,
      FieldsMetadata fieldsMetadata,
      JdbcSinkConfig.InsertMode insertMode,
      JdbcSinkConfig config
  ) {
    this.dialect = dialect;
    this.pkMode = pkMode;
    this.statement = statement;
    this.deleteStatement = deleteStatement;
    this.schemaPair = schemaPair;
    this.fieldsMetadata = fieldsMetadata;
    this.insertMode = insertMode;
    this.config = config;
  }

  @Override
  public void bindRecord(SinkRecord record) throws SQLException {
    final Struct valueStruct = (Struct) record.value();

    // Assumption: the relevant SQL has placeholders for keyFieldNames first followed by
    //             nonKeyFieldNames, in iteration order for all INSERT/ UPSERT queries
    //             the relevant SQL has placeholders for keyFieldNames, in iteration order
    //             for all DELETE queries
    //             the relevant SQL has placeholders for nonKeyFieldNames first followed by
    //             keyFieldNames, in iteration order for all UPDATE queries

    int index = 1;
    if (valueStruct == null && config.deleteEnabled) {
      bindKeyFields(record, index, deleteStatement);
      deleteStatement.addBatch();
    } else {
      switch (insertMode) {
        case INSERT:
        case UPSERT:
          index = bindKeyFields(record, index, statement);
          bindNonKeyFields(record, valueStruct, index, statement);
          break;

        case UPDATE:
          index = bindNonKeyFields(record, valueStruct, index, statement);
          bindKeyFields(record, index, statement);
          break;
        default:
          throw new AssertionError();

      }
      statement.addBatch();
    }
  }

  protected int bindKeyFields(
      SinkRecord record,
      int index,
      PreparedStatement statement
  ) throws SQLException {
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
        if (schemaPair.keySchema.type().isPrimitive()) {
          assert fieldsMetadata.keyFieldNames.size() == 1;
          bindField(statement, index++, schemaPair.keySchema, record.key());
        } else {
          for (String fieldName : fieldsMetadata.keyFieldNames) {
            final Field field = schemaPair.keySchema.field(fieldName);
            bindField(statement, index++, field.schema(), ((Struct) record.key()).get(field));
          }
        }
      }
      break;

      case RECORD_VALUE: {
        for (String fieldName : fieldsMetadata.keyFieldNames) {
          final Field field = schemaPair.valueSchema.field(fieldName);
          bindField(statement, index++, field.schema(), ((Struct) record.value()).get(field));
        }
      }
      break;

      default:
        throw new ConnectException("Unknown primary key mode: " + pkMode);
    }
    return index;
  }

  protected int bindNonKeyFields(
      SinkRecord record,
      Struct valueStruct,
      int index,
      PreparedStatement statement
  ) throws SQLException {
    for (final String fieldName : fieldsMetadata.nonKeyFieldNames) {
      final Field field = record.valueSchema().field(fieldName);
      bindField(statement, index++, field.schema(), valueStruct.get(field));
    }
    return index;
  }

  protected void bindField(
      PreparedStatement statement,
      int index,
      Schema schema,
      Object value
  ) throws SQLException {
    dialect.bindField(statement, index, schema, value);
  }
}
