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

import jdk.nashorn.internal.runtime.options.Option;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.SinkRecord;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.*;
import java.util.stream.Collectors;

import io.confluent.connect.jdbc.sink.dialect.DbDialect;
import io.confluent.connect.jdbc.util.CachedConnectionProvider;

public class JdbcDbWriter {

    private final JdbcSinkConfig config;
    private final DbDialect dbDialect;
    private final DbStructure dbStructure;
    final CachedConnectionProvider cachedConnectionProvider;
    final Optional<JdbcRecordConverter> converter;

    JdbcDbWriter(final JdbcSinkConfig config, DbDialect dbDialect, DbStructure dbStructure) {
        this.config = config;
        this.dbDialect = dbDialect;
        this.dbStructure = dbStructure;
        this.cachedConnectionProvider = new CachedConnectionProvider(config.connectionUrl, config.connectionUser, config.connectionPassword) {
            @Override
            protected void onConnect(Connection connection) throws SQLException {
                connection.setAutoCommit(false);
            }
        };
        this.converter = createSinkConverter(config);
    }

    private static Optional<JdbcRecordConverter> createSinkConverter(JdbcSinkConfig config) {
        if (config.sinkRecordConverter.isPresent()) {
            String converterClassName = config.sinkRecordConverter.get();
            try {

                JdbcRecordConverter converter = (JdbcRecordConverter) Class.forName(converterClassName).newInstance();
                return Optional.of(converter);
            } catch (Exception e) {
                throw new IllegalArgumentException("Cannot create an instance of sink record converter with class " + converterClassName,e);
            }
        } else {
            return Optional.empty();
        }
    }


     void write(final Collection<SinkRecord> records) throws SQLException {
        List<? extends WriteableRecord> writeableRecords =  records.stream().flatMap( s -> flatten(s).stream()).collect(Collectors.toList());
        writeTransactionally(writeableRecords);
     }

     Collection<? extends WriteableRecord> flatten(SinkRecord record) {
        Optional<Collection<? extends WriteableRecord>> collection  = converter.map( s-> s.convert(record));
        return collection.orElseGet(() -> Collections.singletonList(new DynamicTableRecord(record)));
     }



    private void writeTransactionally(final Collection<? extends WriteableRecord> records) throws SQLException {
        final Connection connection = cachedConnectionProvider.getValidConnection();
        final Map<String, BufferedRecords> bufferByTable = new HashMap<>();
        for (WriteableRecord record : records) {
            final String table = record.getTableName(config);
            BufferedRecords buffer = bufferByTable.get(table);
            if (buffer == null) {
                buffer = new BufferedRecords(config, table, dbDialect, dbStructure, connection);
                bufferByTable.put(table, buffer);
            }
            buffer.add(record.getRecord(), record.getMetadataExtractor());
        }
        for (BufferedRecords buffer : bufferByTable.values()) {
            buffer.flush();
            buffer.close();
        }
        connection.commit();
    }

    void closeQuietly() {
        cachedConnectionProvider.closeQuietly();
    }


}
