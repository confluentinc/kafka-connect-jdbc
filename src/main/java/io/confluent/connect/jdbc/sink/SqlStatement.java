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

import io.confluent.connect.jdbc.dialect.DatabaseDialect;
import io.confluent.connect.jdbc.sink.metadata.FieldsMetadata;
import io.confluent.connect.jdbc.util.ColumnId;
import io.confluent.connect.jdbc.util.TableId;
import org.apache.kafka.connect.errors.ConnectException;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Collection;
import java.util.stream.Collectors;

public class SqlStatement {
    private final JdbcSinkConfig config;
    private final DatabaseDialect dbDialect;
    private final TableId tableId;
    private FieldsMetadata fieldsMetadata;
    private final DbStructure dbStructure;
    private final Connection connection;

    public SqlStatement(
            JdbcSinkConfig config,
            DatabaseDialect dbDialect,
            TableId tableId,
            DbStructure dbStructure,
            Connection connection
    ) {
        this.config = config;
        this.tableId = tableId;
        this.dbDialect = dbDialect;
        this.dbStructure = dbStructure;
        this.connection = connection;
    }

    public String getInsertSql() throws SQLException {
        switch (config.insertMode) {
            case INSERT:
                return dbDialect.buildInsertStatement(
                        tableId,
                        asColumns(fieldsMetadata.keyFieldNames),
                        asColumns(fieldsMetadata.nonKeyFieldNames),
                        dbStructure.tableDefinition(connection, tableId)
                );
            case UPSERT:
                if (fieldsMetadata.keyFieldNames.isEmpty()) {
                    throw new ConnectException(String.format(
                            "Write to table '%s' in UPSERT mode requires key field names to be known, check the"
                                    + " primary key configuration",
                            tableId
                    ));
                }
                try {
                    return dbDialect.buildUpsertQueryStatement(
                            tableId,
                            asColumns(fieldsMetadata.keyFieldNames),
                            asColumns(fieldsMetadata.nonKeyFieldNames),
                            dbStructure.tableDefinition(connection, tableId)
                    );
                } catch (UnsupportedOperationException e) {
                    throw new ConnectException(String.format(
                            "Write to table '%s' in UPSERT mode is not supported with the %s dialect.",
                            tableId,
                            dbDialect.name()
                    ));
                }
            case UPDATE:
                return dbDialect.buildUpdateStatement(
                        tableId,
                        asColumns(fieldsMetadata.keyFieldNames),
                        asColumns(fieldsMetadata.nonKeyFieldNames),
                        dbStructure.tableDefinition(connection, tableId)
                );
            default:
                throw new ConnectException("Invalid insert mode");
        }
    }

    public String getDeleteSql() {
        String sql = null;
        if (config.deleteEnabled) {
            switch (config.pkMode) {
                case RECORD_KEY:
                    if (fieldsMetadata.keyFieldNames.isEmpty()) {
                        throw new ConnectException("Require primary keys to support delete");
                    }
                    try {
                        sql = dbDialect.buildDeleteStatement(
                                tableId,
                                asColumns(fieldsMetadata.keyFieldNames)
                        );
                    } catch (UnsupportedOperationException e) {
                        throw new ConnectException(String.format(
                                "Deletes to table '%s' are not supported with the %s dialect.",
                                tableId,
                                dbDialect.name()
                        ));
                    }
                    break;

                default:
                    throw new ConnectException("Deletes are only supported for pk.mode record_key");
            }
        }
        return sql;
    }

    private Collection<ColumnId> asColumns(Collection<String> names) {
        return names.stream()
                .map(name -> new ColumnId(tableId, name))
                .collect(Collectors.toList());
    }
}
