/*
 * Copyright 2020 Confluent Inc.
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

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.Before;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

public class RecordValidatorTest {

  private Map<Object, Object> props;

  @Before
  public void setUp() {
    props = new HashMap<>();
    props.put("name", "test-connector");
    props.put("connection.url", "jdbc:bogus:something");
    props.put("connection.user", "sa");
    props.put("connection.password", "password");
  }

  @Test
  public void requiresValueAcceptsStructSchema() {
    props.put("pk.mode", "none");
    JdbcSinkConfig config = new JdbcSinkConfig(props);
    RecordValidator validator = RecordValidator.create(config);

    Schema valueSchema = SchemaBuilder.struct()
        .field("name", Schema.STRING_SCHEMA)
        .build();
    Struct value = new Struct(valueSchema).put("name", "test");
    SinkRecord record = new SinkRecord("topic", 0, null, null, valueSchema, value, 0);

    validator.validate(record);
  }

  @Test
  public void requiresValueAcceptsStringSchema() {
    props.put("pk.mode", "none");
    JdbcSinkConfig config = new JdbcSinkConfig(props);
    RecordValidator validator = RecordValidator.create(config);

    SinkRecord record = new SinkRecord(
        "topic", 0, null, null, Schema.STRING_SCHEMA, "hello", 0
    );

    validator.validate(record);
  }

  @Test
  public void requiresValueRejectsNonStringPrimitiveSchema() {
    props.put("pk.mode", "none");
    JdbcSinkConfig config = new JdbcSinkConfig(props);
    RecordValidator validator = RecordValidator.create(config);

    SinkRecord record = new SinkRecord(
        "topic", 0, null, null, Schema.INT32_SCHEMA, 42, 0
    );

    ConnectException e = assertThrows(ConnectException.class, () -> validator.validate(record));
    assertTrue(e.getMessage().contains("Struct or String"));
  }

  @Test
  public void requiresValueRejectsNullValue() {
    props.put("pk.mode", "none");
    JdbcSinkConfig config = new JdbcSinkConfig(props);
    RecordValidator validator = RecordValidator.create(config);

    SinkRecord record = new SinkRecord("topic", 0, null, null, null, null, 0);

    ConnectException e = assertThrows(ConnectException.class, () -> validator.validate(record));
    assertTrue(e.getMessage().contains("with a null value"));
  }

  @Test
  public void stringValueWithRecordKeyPkMode() {
    props.put("pk.mode", "record_key");
    props.put("pk.fields", "id");
    JdbcSinkConfig config = new JdbcSinkConfig(props);
    RecordValidator validator = RecordValidator.create(config);

    SinkRecord record = new SinkRecord(
        "topic", 0, Schema.INT64_SCHEMA, 42L, Schema.STRING_SCHEMA, "hello", 0
    );

    validator.validate(record);
  }

  @Test
  public void stringValueWithDeleteEnabled() {
    props.put("delete.enabled", true);
    props.put("pk.mode", "record_key");
    props.put("pk.fields", "id");
    JdbcSinkConfig config = new JdbcSinkConfig(props);
    RecordValidator validator = RecordValidator.create(config);

    SinkRecord insertRecord = new SinkRecord(
        "topic", 0, Schema.INT64_SCHEMA, 42L, Schema.STRING_SCHEMA, "hello", 0
    );
    validator.validate(insertRecord);

    SinkRecord deleteRecord = new SinkRecord(
        "topic", 0, Schema.INT64_SCHEMA, 42L, null, null, 1
    );
    validator.validate(deleteRecord);
  }

  @Test
  public void stringValueWithFieldsWhitelistIsRejected() {
    props.put("pk.mode", "none");
    props.put("fields.whitelist", "field_a,field_b");
    JdbcSinkConfig config = new JdbcSinkConfig(props);
    RecordValidator validator = RecordValidator.create(config);

    SinkRecord record = new SinkRecord(
        "topic", 0, null, null, Schema.STRING_SCHEMA, "hello", 0
    );

    ConnectException e = assertThrows(ConnectException.class, () -> validator.validate(record));
    assertTrue(e.getMessage().contains("fields.whitelist"));
    assertTrue(e.getMessage().contains("not applicable to String values"));
  }

  @Test
  public void stringValueWithTimestampFieldsListIsRejected() {
    props.put("pk.mode", "none");
    props.put("timestamp.fields.list", "created_at");
    JdbcSinkConfig config = new JdbcSinkConfig(props);
    RecordValidator validator = RecordValidator.create(config);

    SinkRecord record = new SinkRecord(
        "topic", 0, null, null, Schema.STRING_SCHEMA, "hello", 0
    );

    ConnectException e = assertThrows(ConnectException.class, () -> validator.validate(record));
    assertTrue(e.getMessage().contains("timestamp.fields.list"));
    assertTrue(e.getMessage().contains("not applicable to String values"));
  }

  @Test
  public void stringValueWithExplicitTimestampPrecisionModeIsRejected() {
    props.put("pk.mode", "none");
    props.put("timestamp.precision.mode", "nanoseconds");
    JdbcSinkConfig config = new JdbcSinkConfig(props);
    RecordValidator validator = RecordValidator.create(config);

    SinkRecord record = new SinkRecord(
        "topic", 0, null, null, Schema.STRING_SCHEMA, "hello", 0
    );

    ConnectException e = assertThrows(ConnectException.class, () -> validator.validate(record));
    assertTrue(e.getMessage().contains("timestamp.precision.mode"));
    assertTrue(e.getMessage().contains("not applicable to String values"));
  }

  @Test
  public void stringValueWithAllIncompatibleConfigsIsRejectedWithAllConfigsNamed() {
    props.put("pk.mode", "none");
    props.put("fields.whitelist", "field_a");
    props.put("timestamp.fields.list", "created_at");
    props.put("timestamp.precision.mode", "nanoseconds");
    JdbcSinkConfig config = new JdbcSinkConfig(props);
    RecordValidator validator = RecordValidator.create(config);

    SinkRecord record = new SinkRecord(
        "topic", 0, null, null, Schema.STRING_SCHEMA, "hello", 0
    );

    ConnectException e = assertThrows(ConnectException.class, () -> validator.validate(record));
    assertTrue(e.getMessage().contains("fields.whitelist"));
    assertTrue(e.getMessage().contains("timestamp.fields.list"));
    assertTrue(e.getMessage().contains("timestamp.precision.mode"));
  }

  @Test
  public void structValueWithIncompatibleConfigsIsAccepted() {
    // The same configs that fail for STRING values are perfectly valid for STRUCT values —
    // the rejection must only fire on the STRING path.
    props.put("pk.mode", "none");
    props.put("fields.whitelist", "name");
    props.put("timestamp.fields.list", "created_at");
    props.put("timestamp.precision.mode", "nanoseconds");
    JdbcSinkConfig config = new JdbcSinkConfig(props);
    RecordValidator validator = RecordValidator.create(config);

    Schema valueSchema = SchemaBuilder.struct()
        .field("name", Schema.STRING_SCHEMA)
        .field("created_at", Schema.INT64_SCHEMA)
        .build();
    Struct value = new Struct(valueSchema).put("name", "test").put("created_at", 1L);
    SinkRecord record = new SinkRecord("topic", 0, null, null, valueSchema, value, 0);

    validator.validate(record);
  }
}
