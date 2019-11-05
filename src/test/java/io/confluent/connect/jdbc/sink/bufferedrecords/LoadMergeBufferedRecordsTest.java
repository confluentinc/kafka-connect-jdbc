/*
 * Copyright 2018 Confluent Inc.
 * Copyright 2019 Nike, Inc.
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

package io.confluent.connect.jdbc.sink.bufferedrecords;

import io.confluent.connect.jdbc.dialect.DatabaseDialect;
import io.confluent.connect.jdbc.dialect.DatabaseDialects;
import io.confluent.connect.jdbc.dialect.SqliteDatabaseDialect;
import io.confluent.connect.jdbc.sink.DbStructure;
import io.confluent.connect.jdbc.sink.JdbcSinkConfig;
import io.confluent.connect.jdbc.sink.SqliteHelper;
import io.confluent.connect.jdbc.sink.metadata.FieldsMetadata;
import io.confluent.connect.jdbc.sink.metadata.SchemaPair;
import io.confluent.connect.jdbc.util.TableId;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.connect.data.Date;
import org.apache.kafka.connect.data.Decimal;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.data.Time;
import org.apache.kafka.connect.data.Timestamp;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.math.BigDecimal;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.function.Predicate;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.spy;

public class LoadMergeBufferedRecordsTest {

    private final SqliteHelper sqliteHelper = new SqliteHelper(getClass().getSimpleName());

    @Before
    public void setUp() throws IOException, SQLException {
        sqliteHelper.setUp();
    }

    @After
    public void tearDown() throws IOException, SQLException {
        sqliteHelper.tearDown();
    }

    @Test
    public void correctBatching() throws SQLException {
        final HashMap<Object, Object> props = new HashMap<>();
        props.put("connection.url", sqliteHelper.sqliteUri());
        props.put("auto.create", true);
        props.put("auto.evolve", true);
        props.put("batch.size", 1000); // sufficiently high to not cause flushes due to buffer being full
        final JdbcSinkConfig config = new JdbcSinkConfig(props);

        final String url = sqliteHelper.sqliteUri();
        final DatabaseDialect dbDialect = DatabaseDialects.findBestFor(url, config);
        final DbStructure dbStructure = new DbStructure(dbDialect);

        final TableId tableId = new TableId(null, null, "dummy");
        final BatchedBufferedRecords buffer = new BatchedBufferedRecords(config, tableId, dbDialect, dbStructure, sqliteHelper.connection);

        final Schema schemaA = SchemaBuilder.struct()
                .field("name", Schema.STRING_SCHEMA)
                .build();
        final Struct valueA = new Struct(schemaA)
                .put("name", "cuba");
        final SinkRecord recordA = new SinkRecord("dummy", 0, null, null, schemaA, valueA, 0);

        final Schema schemaB = SchemaBuilder.struct()
                .field("name", Schema.STRING_SCHEMA)
                .field("age", Schema.OPTIONAL_INT32_SCHEMA)
                .build();
        final Struct valueB = new Struct(schemaB)
                .put("name", "cuba")
                .put("age", 4);
        final SinkRecord recordB = new SinkRecord("dummy", 1, null, null, schemaB, valueB, 1);

        // test records are batched correctly based on schema equality as records are added
        //   (schemaA,schemaA,schemaA,schemaB,schemaA) -> ([schemaA,schemaA,schemaA],[schemaB],[schemaA])

        assertEquals(Collections.emptyList(), buffer.add(recordA));
        assertEquals(Collections.emptyList(), buffer.add(recordA));
        assertEquals(Collections.emptyList(), buffer.add(recordA));

        assertEquals(Arrays.asList(recordA, recordA, recordA), buffer.add(recordB));

        assertEquals(Collections.singletonList(recordB), buffer.add(recordA));

        assertEquals(Collections.singletonList(recordA), buffer.flush());
    }



    @Test(expected = ConfigException.class)
    public void configParsingFailsIfDeleteWithWrongPKMode() {
        final HashMap<Object, Object> props = new HashMap<>();
        props.put("connection.url", sqliteHelper.sqliteUri());
        props.put("auto.create", true);
        props.put("auto.evolve", true);
        props.put("delete.enabled", true);
        props.put("insert.mode", "upsert");
        props.put("pk.mode", "kafka"); // wrong pk mode for deletes
        new JdbcSinkConfig(props);
    }

    @Test
    public void insertThenDeleteInBatchNoFlush() throws SQLException {
        final HashMap<Object, Object> props = new HashMap<>();
        props.put("connection.url", sqliteHelper.sqliteUri());
        props.put("auto.create", true);
        props.put("auto.evolve", true);
        props.put("delete.enabled", true);
        props.put("insert.mode", "upsert");
        props.put("pk.mode", "record_key");
        props.put("batch.size", 1000); // sufficiently high to not cause flushes due to buffer being full
        final JdbcSinkConfig config = new JdbcSinkConfig(props);

        final String url = sqliteHelper.sqliteUri();
        final DatabaseDialect dbDialect = DatabaseDialects.findBestFor(url, config);
        final DbStructure dbStructure = new DbStructure(dbDialect);

        final TableId tableId = new TableId(null, null, "dummy");
        final BatchedBufferedRecords buffer = new BatchedBufferedRecords(config, tableId, dbDialect, dbStructure, sqliteHelper.connection);

        final Schema keySchemaA = SchemaBuilder.struct()
            .field("id", Schema.INT64_SCHEMA)
            .build();
        final Schema valueSchemaA = SchemaBuilder.struct()
            .field("name", Schema.STRING_SCHEMA)
            .build();
        final Struct keyA = new Struct(keySchemaA)
            .put("id", 1234L);
        final Struct valueA = new Struct(valueSchemaA)
            .put("name", "cuba");
        final SinkRecord recordA = new SinkRecord("dummy", 0, keySchemaA, keyA, valueSchemaA, valueA, 0);
        final SinkRecord recordADelete = new SinkRecord("dummy", 0, keySchemaA, keyA, null, null, 0);

        final Schema schemaB = SchemaBuilder.struct()
            .field("name", Schema.STRING_SCHEMA)
            .field("age", Schema.OPTIONAL_INT32_SCHEMA)
            .build();
        final Struct valueB = new Struct(schemaB)
            .put("name", "cuba")
            .put("age", 4);
        final SinkRecord recordB = new SinkRecord("dummy", 1, keySchemaA, keyA, schemaB, valueB, 1);

        // test records are batched correctly based on schema equality as records are added
        //   (schemaA,schemaA,schemaA,schemaB,schemaA) -> ([schemaA,schemaA,schemaA],[schemaB],[schemaA])

        assertEquals(Collections.emptyList(), buffer.add(recordA));
        assertEquals(Collections.emptyList(), buffer.add(recordA));

        // delete should not cause a flush (i.e. not treated as a schema change)
        assertEquals(Collections.emptyList(), buffer.add(recordADelete));

        // schema change should trigger flush
        assertEquals(Arrays.asList(recordA, recordA, recordADelete), buffer.add(recordB));

        // second schema change should trigger flush
        assertEquals(Collections.singletonList(recordB), buffer.add(recordA));

        assertEquals(Collections.singletonList(recordA), buffer.flush());
    }

    @Test
    public void insertThenDeleteThenInsertInBatchFlush() throws SQLException {
        final HashMap<Object, Object> props = new HashMap<>();
        props.put("connection.url", sqliteHelper.sqliteUri());
        props.put("auto.create", true);
        props.put("auto.evolve", true);
        props.put("delete.enabled", true);
        props.put("insert.mode", "upsert");
        props.put("pk.mode", "record_key");
        props.put("batch.size", 1000); // sufficiently high to not cause flushes due to buffer being full
        final JdbcSinkConfig config = new JdbcSinkConfig(props);

        final String url = sqliteHelper.sqliteUri();
        final DatabaseDialect dbDialect = DatabaseDialects.findBestFor(url, config);
        final DbStructure dbStructure = new DbStructure(dbDialect);

        final TableId tableId = new TableId(null, null, "dummy");
        final BatchedBufferedRecords buffer = new BatchedBufferedRecords(config, tableId, dbDialect, dbStructure, sqliteHelper.connection);

        final Schema keySchemaA = SchemaBuilder.struct()
            .field("id", Schema.INT64_SCHEMA)
            .build();
        final Schema valueSchemaA = SchemaBuilder.struct()
            .field("name", Schema.STRING_SCHEMA)
            .build();
        final Struct keyA = new Struct(keySchemaA)
            .put("id", 1234L);
        final Struct valueA = new Struct(valueSchemaA)
            .put("name", "cuba");
        final SinkRecord recordA = new SinkRecord("dummy", 0, keySchemaA, keyA, valueSchemaA, valueA, 0);
        final SinkRecord recordADelete = new SinkRecord("dummy", 0, keySchemaA, keyA, null, null, 0);

        final Schema schemaB = SchemaBuilder.struct()
            .field("name", Schema.STRING_SCHEMA)
            .field("age", Schema.OPTIONAL_INT32_SCHEMA)
            .build();
        final Struct valueB = new Struct(schemaB)
            .put("name", "cuba")
            .put("age", 4);
        final SinkRecord recordB = new SinkRecord("dummy", 1, keySchemaA, keyA, schemaB, valueB, 1);

        assertEquals(Collections.emptyList(), buffer.add(recordA));
        assertEquals(Collections.emptyList(), buffer.add(recordA));

        // delete should not cause a flush (i.e. not treated as a schema change)
        assertEquals(Collections.emptyList(), buffer.add(recordADelete));

        // insert after delete should flush to insure insert isn't lost in batching
        assertEquals(Arrays.asList(recordA, recordA, recordADelete), buffer.add(recordA));

        // schema change should trigger flush
        assertEquals(Collections.singletonList(recordA), buffer.add(recordB));

        // second schema change should trigger flush
        assertEquals(Collections.singletonList(recordB), buffer.add(recordA));

        assertEquals(Collections.singletonList(recordA), buffer.flush());
    }

    @Test
    public void testMultipleDeletesBatchedTogether() throws SQLException {
        final HashMap<Object, Object> props = new HashMap<>();
        props.put("connection.url", sqliteHelper.sqliteUri());
        props.put("auto.create", true);
        props.put("auto.evolve", true);
        props.put("delete.enabled", true);
        props.put("insert.mode", "upsert");
        props.put("pk.mode", "record_key");
        props.put("batch.size", 1000); // sufficiently high to not cause flushes due to buffer being full
        final JdbcSinkConfig config = new JdbcSinkConfig(props);

        final String url = sqliteHelper.sqliteUri();
        final DatabaseDialect dbDialect = DatabaseDialects.findBestFor(url, config);
        final DbStructure dbStructure = new DbStructure(dbDialect);

        final TableId tableId = new TableId(null, null, "dummy");
        final BufferedRecords buffer = new LoadMergeBufferedRecords(config, tableId, dbDialect, dbStructure, sqliteHelper.connection);

        final Schema keySchemaA = SchemaBuilder.struct()
            .field("id", Schema.INT64_SCHEMA)
            .build();
        final Schema valueSchemaA = SchemaBuilder.struct()
            .field("name", Schema.STRING_SCHEMA)
            .build();
        final Struct keyA = new Struct(keySchemaA)
            .put("id", 1234L);
        final Struct valueA = new Struct(valueSchemaA)
            .put("name", "cuba");
        final SinkRecord recordA = new SinkRecord("dummy", 0, keySchemaA, keyA, valueSchemaA, valueA, 0);
        final SinkRecord recordADelete = new SinkRecord("dummy", 0, keySchemaA, keyA, null, null, 0);

        final Schema schemaB = SchemaBuilder.struct()
            .field("name", Schema.STRING_SCHEMA)
            .field("age", Schema.OPTIONAL_INT32_SCHEMA)
            .build();
        final Struct valueB = new Struct(schemaB)
            .put("name", "cuba")
            .put("age", 4);
        final SinkRecord recordB = new SinkRecord("dummy", 1, keySchemaA, keyA, schemaB, valueB, 1);
        final SinkRecord recordBDelete = new SinkRecord("dummy", 1, keySchemaA, keyA, null, null, 1);

        assertEquals(Collections.emptyList(), buffer.add(recordA));

        // schema change should trigger flush
        assertEquals(Collections.singletonList(recordA), buffer.add(recordB));

        // deletes should not cause a flush (i.e. not treated as a schema change)
        assertEquals(Collections.emptyList(), buffer.add(recordADelete));
        assertEquals(Collections.emptyList(), buffer.add(recordBDelete));

        // insert after delete should flush to insure insert isn't lost in batching
        assertEquals(Arrays.asList(recordB, recordADelete, recordBDelete), buffer.add(recordB));

        assertEquals(Collections.singletonList(recordB), buffer.flush());
    }

    @SuppressWarnings("unchecked")
    @Test
    public void testInsertRecordsWithLoadMergeFlow() throws SQLException {
        final HashMap<Object, Object> props = new HashMap<>();
        props.put("connection.url", sqliteHelper.sqliteUri());
        props.put("auto.create", true);
        props.put("auto.evolve", true);
        props.put("batch.size", 1000);
        props.put("pk.mode", "record_key");
        props.put("insert.mode", "upsert");
        final JdbcSinkConfig config = new JdbcSinkConfig(props);

        final DatabaseDialect dbDialect = spy(new SqliteDatabaseDialect(config, true));
        final DbStructure dbStructure = new DbStructure(dbDialect);

        final TableId tableId = new TableId(null, null, "dummy");
        final BufferedRecords buffer = new LoadMergeBufferedRecords(config, tableId, dbDialect, dbStructure, sqliteHelper.connection);
        final Schema keySchema = SchemaBuilder.struct()
                .field("id", Schema.INT64_SCHEMA)
                .build();
        final Schema valueSchema = SchemaBuilder.struct()
                .field("name", Schema.STRING_SCHEMA)
                .build();

        final Struct keyA = new Struct(keySchema).put("id", 1234L);
        final Struct valueA = new Struct(valueSchema).put("name", "cuba");
        final SinkRecord recordA = new SinkRecord("dummy", 0, keySchema, keyA, valueSchema, valueA, 0);

        final Struct updatedValueA = new Struct(valueSchema).put("name", "updated");
        final SinkRecord updatedRecordA = new SinkRecord("dummy", 0, keySchema, keyA, valueSchema, updatedValueA, 0);

        final Struct keyB = new Struct(keySchema).put("id", 5678L);
        final Struct valueB = new Struct(valueSchema).put("name", "gooding");
        final SinkRecord recordB = new SinkRecord("dummy", 0, keySchema, keyB, valueSchema, valueB, 0);

        final Struct keyC = new Struct(keySchema).put("id", 9012L);
        final Struct valueC = new Struct(valueSchema).put("name", "jr");
        final SinkRecord recordC = new SinkRecord("dummy", 0, keySchema, keyC, valueSchema, valueC, 0);

        final Struct duplicateValueC = new Struct(valueSchema).put("name", "senior");
        final SinkRecord duplicateRecordC = new SinkRecord("dummy", 0, keySchema, keyC, valueSchema, duplicateValueC, 0);

        assertEquals(Collections.emptyList(), buffer.add(recordA));
        buffer.flush();

        final Schema expectedSchema = SchemaBuilder.struct()
                .field("id", Schema.INT64_SCHEMA)
                .field("name", Schema.STRING_SCHEMA)
                .build();

        Collection<Struct> expected = new ArrayList();
        SqliteHelper.ResultSetReadCallback validateExpectedCallback = rs -> {
            long id = rs.getLong("id");
            String name = rs.getString("name");

            Struct matchedValue = expected.stream()
                    .filter(val ->
                            val.get("id").equals(id) && val.get("name").equals(name))
                    .findFirst()
                    .get();

            assertNotNull(matchedValue);
            expected.remove(matchedValue);
        };

        expected.add(new Struct(expectedSchema).put("id", 1234L).put("name", "cuba"));
        assertEquals(1, sqliteHelper.select("select * from dummy", validateExpectedCallback));
        assertEquals(0, expected.size());

        assertEquals(Collections.emptyList(), buffer.add(recordB));
        assertEquals(Collections.emptyList(), buffer.add(recordC));
        assertEquals(Collections.emptyList(), buffer.add(duplicateRecordC));
        assertEquals(Collections.emptyList(), buffer.add(updatedRecordA));
        buffer.flush();

        expected.add(new Struct(expectedSchema).put("id", 1234L).put("name", "updated"));
        expected.add(new Struct(expectedSchema).put("id", 5678L).put("name", "gooding"));
        expected.add(new Struct(expectedSchema).put("id", 9012L).put("name", "senior"));
        assertEquals(3, sqliteHelper.select("select * from dummy", validateExpectedCallback));
        assertEquals(0, expected.size());
    }

    @SuppressWarnings("unchecked")
    @Test
    public void testRemoveDuplicatesWithRecordKeyPkModeSinglePk() {
        final HashMap<Object, Object> props = new HashMap<>();
        props.put("connection.url", sqliteHelper.sqliteUri());
        props.put("auto.create", true);
        props.put("auto.evolve", true);
        props.put("batch.size", 1000);
        props.put("pk.mode", "record_key");
        props.put("insert.mode", "upsert");
        final JdbcSinkConfig config = new JdbcSinkConfig(props);

        final LoadMergeBufferedRecords buffer = new LoadMergeBufferedRecords(config, null, null, null, null);
        final Schema keySchema = SchemaBuilder.struct()
                .field("id", Schema.INT64_SCHEMA)
                .build();
        final Schema valueSchema = SchemaBuilder.struct()
                .field("name", Schema.STRING_SCHEMA)
                .build();

        FieldsMetadata fieldsMetadata = FieldsMetadata.extract(
                "",
                config.pkMode,
                config.pkFields,
                config.fieldsWhitelist,
                new SchemaPair(keySchema, valueSchema)
        );

        final Struct keyA = new Struct(keySchema).put("id", 1234L);
        final Struct valueA = new Struct(valueSchema).put("name", "cuba");
        final SinkRecord recordA = new SinkRecord("dummy", 0, keySchema, keyA, valueSchema, valueA, 0);

        final Struct keyB = new Struct(keySchema).put("id", 5678L);
        final Struct valueB = new Struct(valueSchema).put("name", "gooding");
        final SinkRecord recordB = new SinkRecord("dummy", 0, keySchema, keyB, valueSchema, valueB, 0);

        final Struct duplicateValueA = new Struct(valueSchema).put("name", "jr");
        final SinkRecord duplicateRecordA = new SinkRecord("dummy", 0, keySchema, keyA, valueSchema, duplicateValueA, 0);

        Collection<SinkRecord> expected = new ArrayList();
        Predicate<SinkRecord> validate = (record) -> {
            assertTrue(expected.contains(record));
            expected.remove(record);

            return true;
        };

        expected.add(recordB);
        expected.add(duplicateRecordA);

        Collection<SinkRecord> records = Arrays.asList(recordA, recordB, duplicateRecordA);
        Collection<SinkRecord>  actual = buffer.removeDuplicates(records, fieldsMetadata);

        for (SinkRecord s : actual) {
            assertTrue(validate.test(s));
        }

        assertEquals(0, expected.size());
    }

    @SuppressWarnings("unchecked")
    @Test
    public void testRemoveDuplicatesWithRecordKeyPkModePrimitivePk() {
        final HashMap<Object, Object> props = new HashMap<>();
        props.put("connection.url", sqliteHelper.sqliteUri());
        props.put("auto.create", true);
        props.put("auto.evolve", true);
        props.put("batch.size", 1000);
        props.put("pk.mode", "record_key");
        props.put("pk.fields", "pk1");
        props.put("insert.mode", "upsert");
        final JdbcSinkConfig config = new JdbcSinkConfig(props);

        final LoadMergeBufferedRecords buffer = new LoadMergeBufferedRecords(config, null, null, null, null);
        final Schema keySchema = Schema.INT64_SCHEMA;

        final Schema valueSchema = SchemaBuilder.struct()
                .field("name", Schema.STRING_SCHEMA)
                .build();

        FieldsMetadata fieldsMetadata = FieldsMetadata.extract(
                "",
                config.pkMode,
                config.pkFields,
                config.fieldsWhitelist,
                new SchemaPair(keySchema, valueSchema)
        );

        final long keyA = 1234L;
        final Struct valueA = new Struct(valueSchema).put("name", "cuba");
        final SinkRecord recordA = new SinkRecord("dummy", 0, keySchema, keyA, valueSchema, valueA, 0);

        final long keyB = 5678L;
        final Struct valueB = new Struct(valueSchema).put("name", "gooding");
        final SinkRecord recordB = new SinkRecord("dummy", 0, keySchema, keyB, valueSchema, valueB, 0);

        final Struct duplicateValueA = new Struct(valueSchema).put("name", "jr");
        final SinkRecord duplicateRecordA = new SinkRecord("dummy", 0, keySchema, keyA, valueSchema, duplicateValueA, 0);

        Collection<SinkRecord> expected = new ArrayList();
        Predicate <SinkRecord> validate = (record) -> {
            assertTrue(expected.contains(record));
            expected.remove(record);

            return true;
        };

        expected.add(recordB);
        expected.add(duplicateRecordA);

        Collection<SinkRecord> records = Arrays.asList(recordA, recordB, duplicateRecordA);
        Collection<SinkRecord> actual = buffer.removeDuplicates(records, fieldsMetadata);

        for (SinkRecord s : actual) {
            assertTrue(validate.test(s));
        }

        assertEquals(0, expected.size());
    }

    @SuppressWarnings("unchecked")
    @Test
    public void testRemoveDuplicatesWithRecordKeyPkModeMultiplePk() {
        final HashMap<Object, Object> props = new HashMap<>();
        props.put("connection.url", sqliteHelper.sqliteUri());
        props.put("auto.create", true);
        props.put("auto.evolve", true);
        props.put("batch.size", 1000);
        props.put("pk.mode", "record_key");
        props.put("insert.mode", "upsert");
        final JdbcSinkConfig config = new JdbcSinkConfig(props);

        final LoadMergeBufferedRecords buffer = new LoadMergeBufferedRecords(config, null, null, null, null);
        final Schema keySchema = SchemaBuilder.struct()
                .field("pk1", Schema.INT64_SCHEMA)
                .field("pk2", Schema.INT64_SCHEMA)
                .build();
        final Schema valueSchema = SchemaBuilder.struct()
                .field("name", Schema.STRING_SCHEMA)
                .build();

        FieldsMetadata fieldsMetadata = FieldsMetadata.extract(
                "",
                config.pkMode,
                config.pkFields,
                config.fieldsWhitelist,
                new SchemaPair(keySchema, valueSchema)
        );

        final Struct keyA = new Struct(keySchema).put("pk1", 1234L).put("pk2", 1234L);
        final Struct valueA = new Struct(valueSchema).put("name", "cuba");
        final SinkRecord recordA = new SinkRecord("dummy", 0, keySchema, keyA, valueSchema, valueA, 0);

        final Struct keyB = new Struct(keySchema).put("pk1", 1234L).put("pk2", 5678L);
        final Struct valueB = new Struct(valueSchema).put("name", "gooding");
        final SinkRecord recordB = new SinkRecord("dummy", 0, keySchema, keyB, valueSchema, valueB, 0);

        final Struct duplicateValueA = new Struct(valueSchema).put("name", "jr");
        final SinkRecord duplicateRecordA = new SinkRecord("dummy", 0, keySchema, keyA, valueSchema, duplicateValueA, 0);

        Collection<SinkRecord> expected = new ArrayList();
        Predicate <SinkRecord> validate = (record) -> {
            assertTrue(expected.contains(record));
            expected.remove(record);

            return true;
        };

        expected.add(recordB);
        expected.add(duplicateRecordA);

        Collection<SinkRecord> records = Arrays.asList(recordA, recordB, duplicateRecordA);
        Collection<SinkRecord> actual = buffer.removeDuplicates(records, fieldsMetadata);

        for (SinkRecord s : actual) {
            assertTrue(validate.test(s));
        }

        assertEquals(0, expected.size());
    }

    @SuppressWarnings("unchecked")
    @Test
    public void testRemoveDuplicatesWithRecordValuePkModeSinglePK() {
        final HashMap<Object, Object> props = new HashMap<>();
        props.put("connection.url", sqliteHelper.sqliteUri());
        props.put("auto.create", true);
        props.put("auto.evolve", true);
        props.put("batch.size", 1000);
        props.put("pk.mode", "record_value");
        props.put("pk.fields", "id");
        props.put("insert.mode", "upsert");
        final JdbcSinkConfig config = new JdbcSinkConfig(props);

        final LoadMergeBufferedRecords buffer = new LoadMergeBufferedRecords(config, null, null, null, null);

        final Schema valueSchema = SchemaBuilder.struct()
                .field("id", Schema.INT64_SCHEMA)
                .field("name", Schema.STRING_SCHEMA)
                .build();

        FieldsMetadata fieldsMetadata = FieldsMetadata.extract(
                "",
                config.pkMode,
                config.pkFields,
                config.fieldsWhitelist,
                new SchemaPair(null, valueSchema)
        );

        final Struct valueA = new Struct(valueSchema)
                .put("id", 1234L)
                .put("name", "cuba");
        final SinkRecord recordA = new SinkRecord("dummy", 0, null, null, valueSchema, valueA, 0);

        final Struct valueB = new Struct(valueSchema)
                .put("id", 5678L)
                .put("name", "gooding");
        final SinkRecord recordB = new SinkRecord("dummy", 0, null, null, valueSchema, valueB, 0);

        final Struct duplicateValueA = new Struct(valueSchema)
                .put("id", 1234L)
                .put("name", "jr");
        final SinkRecord duplicateRecordA = new SinkRecord("dummy", 0, null, null, valueSchema, duplicateValueA, 0);

        Collection<SinkRecord> expected = new ArrayList();
        Predicate <SinkRecord> validate = (record) -> {
            assertTrue(expected.contains(record));
            expected.remove(record);

            return true;
        };

        expected.add(recordB);
        expected.add(duplicateRecordA);

        Collection<SinkRecord> records = Arrays.asList(recordA, recordB, duplicateRecordA);
        Collection<SinkRecord> actual = buffer.removeDuplicates(records, fieldsMetadata);

        for (SinkRecord s : actual) {
            assertTrue(validate.test(s));
        }

        assertEquals(0, expected.size());
    }

    @SuppressWarnings("unchecked")
    @Test
    public void testRemoveDuplicatesWithRecordValuePkModeMultiplePK() {
        final HashMap<Object, Object> props = new HashMap<>();
        props.put("connection.url", sqliteHelper.sqliteUri());
        props.put("auto.create", true);
        props.put("auto.evolve", true);
        props.put("batch.size", 1000);
        props.put("pk.mode", "record_value");
        props.put("pk.fields", "pk1,pk2");
        props.put("insert.mode", "upsert");
        final JdbcSinkConfig config = new JdbcSinkConfig(props);

        final LoadMergeBufferedRecords buffer = new LoadMergeBufferedRecords(config, null, null, null, null);

        final Schema valueSchema = SchemaBuilder.struct()
                .field("pk1", Schema.INT64_SCHEMA)
                .field("pk2", Schema.INT64_SCHEMA)
                .field("name", Schema.STRING_SCHEMA)
                .build();

        FieldsMetadata fieldsMetadata = FieldsMetadata.extract(
                "",
                config.pkMode,
                config.pkFields,
                config.fieldsWhitelist,
                new SchemaPair(null, valueSchema)
        );

        final Struct valueA = new Struct(valueSchema)
                .put("pk1", 1234L)
                .put("pk2", 1234L)
                .put("name", "cuba");
        final SinkRecord recordA = new SinkRecord("dummy", 0, null, null, valueSchema, valueA, 0);

        final Struct valueB = new Struct(valueSchema)
                .put("pk1", 1234L)
                .put("pk2", 5678L)
                .put("name", "gooding");
        final SinkRecord recordB = new SinkRecord("dummy", 0, null, null, valueSchema, valueB, 0);

        final Struct duplicateValueA = new Struct(valueSchema)
                .put("pk1", 1234L)
                .put("pk2", 1234L)
                .put("name", "jr");
        final SinkRecord duplicateRecordA = new SinkRecord("dummy", 0, null, null, valueSchema, duplicateValueA, 0);

        Collection<SinkRecord> expected = new ArrayList();
        Predicate<SinkRecord> validate = (record) -> {
            assertTrue(expected.contains(record));
            expected.remove(record);

            return true;
        };

        expected.add(recordB);
        expected.add(duplicateRecordA);

        Collection<SinkRecord> records = Arrays.asList(recordA, recordB, duplicateRecordA);
        Collection<SinkRecord>  actual = buffer.removeDuplicates(records, fieldsMetadata);

        for (SinkRecord s : actual) {
            assertTrue(validate.test(s));
        }

        assertEquals(0, expected.size());
    }

    @Test
    public void testPrimitiveValueToString() {
        final LoadMergeBufferedRecords buffer = new LoadMergeBufferedRecords(null, null, null, null, null);

        assertEquals("-1", buffer.primitiveValueToString(Schema.INT8_SCHEMA, (byte) 0xff));
        assertEquals("123", buffer.primitiveValueToString(Schema.INT32_SCHEMA,123));
        assertEquals("1234", buffer.primitiveValueToString(Schema.INT64_SCHEMA,1234L));
        assertEquals("1.234", buffer.primitiveValueToString(Schema.FLOAT32_SCHEMA,1.234f));
        assertEquals("1.2345678", buffer.primitiveValueToString(Schema.FLOAT64_SCHEMA,1.2345678d));
        assertEquals("true", buffer.primitiveValueToString(Schema.BOOLEAN_SCHEMA,true));
        assertEquals("foo", buffer.primitiveValueToString(Schema.STRING_SCHEMA,"foo"));
        assertEquals("[0, 0, 0, 0, 0, 0, 0, 0, 0, 0]", buffer.primitiveValueToString(Schema.BYTES_SCHEMA, new byte[10]));
    }

    @Test
    public void testKeyFieldToString() {
        final LoadMergeBufferedRecords buffer = new LoadMergeBufferedRecords(null, null, null, null, null);

        assertEquals("<null>", buffer.keyFieldToString(null, null));
        assertEquals("-1", buffer.keyFieldToString(Schema.INT8_SCHEMA, (byte) 0xff));
        assertEquals("123", buffer.keyFieldToString(Schema.INT32_SCHEMA,123));
        assertEquals("1234", buffer.keyFieldToString(Schema.INT64_SCHEMA,1234L));
        assertEquals("1.234", buffer.keyFieldToString(Schema.FLOAT32_SCHEMA,1.234f));
        assertEquals("1.2345678", buffer.keyFieldToString(Schema.FLOAT64_SCHEMA,1.2345678d));
        assertEquals("true", buffer.keyFieldToString(Schema.BOOLEAN_SCHEMA,true));
        assertEquals("foo", buffer.keyFieldToString(Schema.STRING_SCHEMA,"foo"));
        assertEquals("[0, 0, 0, 0, 0, 0, 0, 0, 0, 0]", buffer.keyFieldToString(Schema.BYTES_SCHEMA, new byte[10]));

        String expectedDate = "1970-01-01T00:00:11.111Z";
        java.util.Date date = new java.util.Date(11111L);
        assertEquals(expectedDate, buffer.keyFieldToString(SchemaBuilder.struct().name(Date.LOGICAL_NAME), date));
        assertEquals(expectedDate, buffer.keyFieldToString(SchemaBuilder.struct().name(Time.LOGICAL_NAME), date));
        assertEquals(expectedDate, buffer.keyFieldToString(SchemaBuilder.struct().name(Timestamp.LOGICAL_NAME), date));

        assertEquals("1.23",buffer.keyFieldToString(SchemaBuilder.struct().name(Decimal.LOGICAL_NAME), new BigDecimal("1.23")));
    }
}
