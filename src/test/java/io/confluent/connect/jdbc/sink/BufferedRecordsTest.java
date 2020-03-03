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

import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Matchers;
import org.mockito.Mockito;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Random;

import io.confluent.connect.jdbc.dialect.DatabaseDialect;
import io.confluent.connect.jdbc.dialect.DatabaseDialects;
import io.confluent.connect.jdbc.dialect.SqliteDatabaseDialect;
import io.confluent.connect.jdbc.sink.metadata.FieldsMetadata;
import io.confluent.connect.jdbc.util.TableId;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class BufferedRecordsTest {

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
    final BufferedRecords buffer = new BufferedRecords(config, tableId, dbDialect, dbStructure, sqliteHelper.connection);

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
    final BufferedRecords buffer = new BufferedRecords(config, tableId, dbDialect, dbStructure, sqliteHelper.connection);

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
  public void insertThenTwoDeletesWithSchemaInBatchNoFlush() throws SQLException {
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
	    final BufferedRecords buffer = new BufferedRecords(config, tableId, dbDialect, dbStructure, sqliteHelper.connection);

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
	    final SinkRecord recordADeleteWithSchema = new SinkRecord("dummy", 0, keySchemaA, keyA, valueSchemaA, null, 0);
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
	    assertEquals(Collections.emptyList(), buffer.add(recordADeleteWithSchema));

	    // delete should not cause a flush (i.e. not treated as a schema change)
	    assertEquals(Collections.emptyList(), buffer.add(recordADelete));
	    
	    // schema change and/or previous deletes should trigger flush
	    assertEquals(Arrays.asList(recordA, recordA, recordADeleteWithSchema, recordADelete), buffer.add(recordB));

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
    final BufferedRecords buffer = new BufferedRecords(config, tableId, dbDialect, dbStructure, sqliteHelper.connection);

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
  public void insertThenDeleteWithSchemaThenInsertInBatchFlush() throws SQLException {
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
	    final BufferedRecords buffer = new BufferedRecords(config, tableId, dbDialect, dbStructure, sqliteHelper.connection);

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
	    final SinkRecord recordADeleteWithSchema = new SinkRecord("dummy", 0, keySchemaA, keyA, valueSchemaA, null, 0);

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
	    assertEquals(Collections.emptyList(), buffer.add(recordADeleteWithSchema));

	    // insert after delete should flush to insure insert isn't lost in batching
	    assertEquals(Arrays.asList(recordA, recordA, recordADeleteWithSchema), buffer.add(recordA));

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
    final BufferedRecords buffer = new BufferedRecords(config, tableId, dbDialect, dbStructure, sqliteHelper.connection);

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

  @Test
  public void testMultipleDeletesWithSchemaBatchedTogether() throws SQLException {
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
	    final BufferedRecords buffer = new BufferedRecords(config, tableId, dbDialect, dbStructure, sqliteHelper.connection);

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
	    final SinkRecord recordADeleteWithSchema = new SinkRecord("dummy", 0, keySchemaA, keyA, valueSchemaA, null, 0);

	    final Schema schemaB = SchemaBuilder.struct()
	        .field("name", Schema.STRING_SCHEMA)
	        .field("age", Schema.OPTIONAL_INT32_SCHEMA)
	        .build();
	    final Struct valueB = new Struct(schemaB)
	        .put("name", "cuba")
	        .put("age", 4);
	    final SinkRecord recordB = new SinkRecord("dummy", 1, keySchemaA, keyA, schemaB, valueB, 1);
	    final SinkRecord recordBDeleteWithSchema = new SinkRecord("dummy", 1, keySchemaA, keyA, schemaB, null, 1);

	    assertEquals(Collections.emptyList(), buffer.add(recordA));

	    // schema change should trigger flush
	    assertEquals(Collections.singletonList(recordA), buffer.add(recordB));

	    // schema change should trigger flush
	    assertEquals(Collections.singletonList(recordB), buffer.add(recordADeleteWithSchema));
	    
	    // schema change should trigger flush
	    assertEquals(Collections.singletonList(recordADeleteWithSchema), buffer.add(recordBDeleteWithSchema));

	    // insert after delete should flush to insure insert isn't lost in batching
	    assertEquals(Collections.singletonList(recordBDeleteWithSchema), buffer.add(recordB));

	    assertEquals(Collections.singletonList(recordB), buffer.flush());
  }
  
  @Test
  public void testFlushSuccessNoInfo() throws SQLException {
    final String url = sqliteHelper.sqliteUri();
    final HashMap<Object, Object> props = new HashMap<>();
    props.put("connection.url", url);
    props.put("auto.create", true);
    props.put("auto.evolve", true);
    props.put("batch.size", 1000);
    final JdbcSinkConfig config = new JdbcSinkConfig(props);

    final DatabaseDialect dbDialect = DatabaseDialects.findBestFor(url, config);

    int[] batchResponse = new int[2];
    batchResponse[0] = Statement.SUCCESS_NO_INFO;
    batchResponse[1] = Statement.SUCCESS_NO_INFO;

    final DbStructure dbStructureMock = mock(DbStructure.class);
    when(dbStructureMock.createOrAmendIfNecessary(Matchers.any(JdbcSinkConfig.class),
                                                  Matchers.any(Connection.class),
                                                  Matchers.any(TableId.class),
                                                  Matchers.any(FieldsMetadata.class)))
        .thenReturn(true);

    PreparedStatement preparedStatementMock = mock(PreparedStatement.class);
    when(preparedStatementMock.executeBatch()).thenReturn(batchResponse);

    Connection connectionMock = mock(Connection.class);
    when(connectionMock.prepareStatement(Matchers.anyString())).thenReturn(preparedStatementMock);

    final TableId tableId = new TableId(null, null, "dummy");
    final BufferedRecords buffer = new BufferedRecords(config, tableId, dbDialect,
                                                       dbStructureMock, connectionMock);

    final Schema schemaA = SchemaBuilder.struct().field("name", Schema.STRING_SCHEMA).build();
    final Struct valueA = new Struct(schemaA).put("name", "cuba");
    final SinkRecord recordA = new SinkRecord("dummy", 0, null, null, schemaA, valueA, 0);
    buffer.add(recordA);

    final Schema schemaB = SchemaBuilder.struct().field("name", Schema.STRING_SCHEMA).build();
    final Struct valueB = new Struct(schemaA).put("name", "cubb");
    final SinkRecord recordB = new SinkRecord("dummy", 0, null, null, schemaB, valueB, 0);
    buffer.add(recordB);
    buffer.flush();

  }


  @Test
  public void testInsertModeUpdate() throws SQLException {
    final String url = sqliteHelper.sqliteUri();
    final HashMap<Object, Object> props = new HashMap<>();
    props.put("connection.url", url);
    props.put("auto.create", true);
    props.put("auto.evolve", true);
    props.put("batch.size", 1000);
    props.put("insert.mode", "update");
    final JdbcSinkConfig config = new JdbcSinkConfig(props);

    final DatabaseDialect dbDialect = DatabaseDialects.findBestFor(url, config);
    assertTrue(dbDialect instanceof SqliteDatabaseDialect);
    final DbStructure dbStructureMock = mock(DbStructure.class);
    when(dbStructureMock.createOrAmendIfNecessary(Matchers.any(JdbcSinkConfig.class),
                                                  Matchers.any(Connection.class),
                                                  Matchers.any(TableId.class),
                                                  Matchers.any(FieldsMetadata.class)))
        .thenReturn(true);

    final Connection connectionMock = mock(Connection.class);
    final TableId tableId = new TableId(null, null, "dummy");
    final BufferedRecords buffer = new BufferedRecords(config, tableId, dbDialect, dbStructureMock,
            connectionMock);

    final Schema schemaA = SchemaBuilder.struct().field("name", Schema.STRING_SCHEMA).build();
    final Struct valueA = new Struct(schemaA).put("name", "cuba");
    final SinkRecord recordA = new SinkRecord("dummy", 0, null, null, schemaA, valueA, 0);
    buffer.add(recordA);

    // Even though we're using the SQLite dialect, which uses backtick as the default quote
    // character, the SQLite JDBC driver does return double quote as the quote characters.
    Mockito.verify(
        connectionMock,
        Mockito.times(1)
    ).prepareStatement(Matchers.eq("UPDATE \"dummy\" SET \"name\" = ?"));

  }
}
