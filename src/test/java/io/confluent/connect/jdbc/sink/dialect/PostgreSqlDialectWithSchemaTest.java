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

package io.confluent.connect.jdbc.sink.dialect;

import org.apache.kafka.connect.data.Date;
import org.apache.kafka.connect.data.Decimal;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Time;
import org.apache.kafka.connect.data.Timestamp;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.Optional;

import static org.junit.Assert.assertEquals;

public class PostgreSqlDialectWithSchemaTest extends BaseDialectTest {

    public final static Optional<String> schema = Optional.of("schema");

    public PostgreSqlDialectWithSchemaTest() {
        super(new PostgreSqlDialect(schema));
    }


    @Test
    public void upsertOnDifferentSchema() {
        assertEquals(
                "INSERT INTO \"schema\".\"Customer\" (\"id\",\"name\",\"salary\",\"address\") "
                        + "VALUES (?,?,?,?) ON CONFLICT (\"id\") DO UPDATE SET \"name\"=EXCLUDED.\"name\",\"salary\"=EXCLUDED.\"salary\",\"address\"=EXCLUDED.\"address\"",
                dialect.getUpsertQuery("Customer", Collections.singletonList("id"), Arrays.asList("name", "salary", "address"))
        );
    }

    @Test
    public void insertOnDifferentSchema(){
        assertEquals(
                "INSERT INTO \"schema\".\"Customer\" (\"id\",\"name\",\"salary\",\"address\") "
                        + "VALUES (?,?,?,?)",
                dialect.getInsert("Customer", Collections.singletonList("id"), Arrays.asList("name", "salary", "address"))
        );
    }

}
