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

import org.apache.kafka.connect.sink.SinkRecord;

import java.sql.SQLException;
import java.util.List;

/**
 * A component used to manage how SinkRecords are processed and sent to JDBC.
 *
 * <p>Component responsibilities include
 * <ul>
   * <li>Handling sink record batching.
   * <li>Detecting then invoking record inserts, update, and delete.
   * <li>detecting record schema changes then altering target tables when appropriate.
 * </ul>
 */
public interface BufferedRecords {
  /**
   * Add record to writer.
   * @param record added record.
   * @return records flushed if record flush was triggered, otherwise an empty collection.
   * @throws SQLException if an issue with the jdbc connection had occurred.
   */
  List<SinkRecord> add(SinkRecord record) throws SQLException;

  /**
   * flush pending sink records.
   * @return collection of records flushed.
   * @throws SQLException if an issue with the JDBC connection had occurred.
   */
  List<SinkRecord> flush() throws SQLException;

  /**
   * Close the JDBC connection and prepared statements.
   * @throws SQLException if an issue with the JDBC connection had occurred.
   */
  void close() throws SQLException;
}
