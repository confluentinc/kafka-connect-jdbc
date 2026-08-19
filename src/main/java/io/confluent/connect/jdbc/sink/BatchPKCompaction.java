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

package io.confluent.connect.jdbc.sink;

import io.confluent.connect.jdbc.sink.metadata.FieldsMetadata;
import io.confluent.connect.jdbc.sink.metadata.SchemaPair;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;


public class BatchPKCompaction {

  private final JdbcSinkConfig.PrimaryKeyMode pkMode;
  private final FieldsMetadata fieldsMetadata;
  private final SchemaPair schemaPair;

  public BatchPKCompaction(
      JdbcSinkConfig.PrimaryKeyMode pkMode,
      FieldsMetadata fieldsMetadata,
      SchemaPair schemaPair) {
    this.pkMode = pkMode;
    this.fieldsMetadata = fieldsMetadata;
    this.schemaPair = schemaPair;
  }

  List<SinkRecord> applyCompaction(List<SinkRecord> records) {
    LinkedHashMap<PrimaryKeyValue, SinkRecord> lastValues = new LinkedHashMap<>();
    for (int i = records.size() - 1; i >= 0; i--) {
      lastValues.putIfAbsent(getPK(records.get(i)), records.get(i));
    }
    List<SinkRecord> result = new ArrayList<>(lastValues.values());
    Collections.reverse(result);
    return result;
  }

  private PrimaryKeyValue getPK(SinkRecord record) {
    switch (pkMode) {
      case KAFKA: {
        return new PrimaryKeyValue(record.topic(), record.kafkaPartition(), record.kafkaOffset());
      }
      case RECORD_KEY: {
        return getPK(schemaPair.keySchema, record.key());
      }
      case RECORD_VALUE: {
        return getPK(schemaPair.valueSchema, record.value());
      }
      case NONE: {
        throw new UnsupportedOperationException("Compaction isn't supported for pkMode NONE");
      }
      default:
        throw new UnsupportedOperationException("Compaction isn't supported for pkMode: " + pkMode);
    }
  }

  private PrimaryKeyValue getPK(Schema schema, Object value) {
    if (schema.type().isPrimitive()) {
      assert fieldsMetadata.keyFieldNames.size() == 1;
      return new PrimaryKeyValue(value);
    } else {
      return new PrimaryKeyValue(
          fieldsMetadata.keyFieldNames.stream()
              .map(fieldName -> ((Struct) value).get(schema.field(fieldName)))
              .toArray()
      );
    }
  }

  private static class PrimaryKeyValue {

    private final Object[] values;

    private PrimaryKeyValue(Object... values) {
      this.values = values;
    }

    @Override
    public boolean equals(Object obj) {
      if (this == obj) {
        return true;
      }
      if (obj == null || getClass() != obj.getClass()) {
        return false;
      }
      PrimaryKeyValue that = (PrimaryKeyValue) obj;
      return Arrays.equals(this.values, that.values);
    }

    @Override
    public int hashCode() {
      return Arrays.hashCode(values);
    }
  }
}
