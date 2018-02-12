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

package io.confluent.connect.jdbc.sink.metadata;

import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.errors.ConnectException;

import java.util.*;

import io.confluent.connect.jdbc.sink.JdbcSinkConfig;

public class FieldsMetadata {

  public final Set<String> keyFieldNames;
  public final Set<String> nonKeyFieldNames;
  public final Map<String, SinkRecordField> allFields;

  private FieldsMetadata(Set<String> keyFieldNames, Set<String> nonKeyFieldNames, Map<String, SinkRecordField> allFields) {
    if ((keyFieldNames.size() + nonKeyFieldNames.size() != allFields.size())
        || !(allFields.keySet().containsAll(keyFieldNames) && allFields.keySet().containsAll(nonKeyFieldNames))) {
      throw new IllegalArgumentException(String.format(
          "Validation fail -- keyFieldNames:%s nonKeyFieldNames:%s allFields:%s",
          keyFieldNames, nonKeyFieldNames, allFields
      ));
    }
    this.keyFieldNames = keyFieldNames;
    this.nonKeyFieldNames = nonKeyFieldNames;
    this.allFields = allFields;
  }

  public static FieldsMetadata extract(
      final String tableName,
      final JdbcSinkConfig.PrimaryKeyMode pkMode,
      final List<String> configuredPkFields,
      final Set<String> fieldsWhitelist,
      final SchemaPair schemaPair
  ) {
    return extract(tableName, pkMode, configuredPkFields, fieldsWhitelist, schemaPair.keySchema, schemaPair.valueSchema);
  }



  public static FieldsMetadata extract(String tableName,
                                       Set<String> keyFieldNames,
                                       Map<String,SinkRecordField> keyFields,
                                       final Set<String> fieldsWhitelist,
                                       final Schema valueSchema){
    final Set<String> nonKeyFieldNames = new LinkedHashSet<>();
    final Map<String, SinkRecordField> allFields = new HashMap<>();
    allFields.putAll(keyFields);
    if (valueSchema != null) {
      for (Field field : valueSchema.fields()) {
        if (keyFieldNames.contains(field.name())) {
          continue;
        }
        if (!fieldsWhitelist.isEmpty() && !fieldsWhitelist.contains(field.name())) {
          continue;
        }

        nonKeyFieldNames.add(field.name());

        final Schema fieldSchema = field.schema();
        allFields.put(field.name(), new SinkRecordField(fieldSchema, field.name(), false));
      }
    }

    if (allFields.isEmpty()) {
      throw new ConnectException("No keyFields found using key and value schemas for table: " + tableName);
    }

    return new FieldsMetadata(keyFieldNames, nonKeyFieldNames, allFields);
  }



  public static FieldsMetadata extract(
      final String tableName,
      final JdbcSinkConfig.PrimaryKeyMode pkMode,
      final List<String> configuredPkFields,
      final Set<String> fieldsWhitelist,
      final Schema keySchema,
      final Schema valueSchema
  ) {
    if (valueSchema != null && valueSchema.type() != Schema.Type.STRUCT) {
      throw new ConnectException("Value schema must be of type Struct");
    }

    PrimaryKeyStrategy primaryKeyStrategy = null;
    switch (pkMode) {
      case KAFKA: {
        primaryKeyStrategy = KafkaPrimaryKeyStrategy.Instance;
      }
      break;
      case RECORD_KEY: {
        primaryKeyStrategy = new RecordKeyPrimaryKeyStrategy(keySchema);
      }
      break;
      case RECORD_VALUE: {
        primaryKeyStrategy = new RecordValuePrimaryKeyStrategy(valueSchema);
      }
      break;
    }
    Set<String> keyFieldNames = new LinkedHashSet<>();
    Map<String,SinkRecordField> keyFields = new HashMap<>();
    if(primaryKeyStrategy!=null){
      PrimaryKeyStrategy.PrimaryKeyFields fields = primaryKeyStrategy.get(configuredPkFields,tableName);
      keyFieldNames.addAll(fields.keyFieldNames);
      keyFields.putAll(fields.keyFields);
    }
    return extract(tableName, keyFieldNames,keyFields,fieldsWhitelist,valueSchema);
  }

  @Override
  public String toString() {
    return "FieldsMetadata{" +
           "keyFieldNames=" + keyFieldNames +
           ", nonKeyFieldNames=" + nonKeyFieldNames +
           ", allFields=" + allFields +
           '}';
  }
}
