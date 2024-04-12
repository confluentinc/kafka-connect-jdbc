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

package io.confluent.connect.jdbc.source;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Importance;
import org.apache.kafka.common.config.ConfigDef.Type;

import java.util.Map;

/**
 * Configuration options for a single JdbcSourceTask. These are processed after all
 * Connector-level configs have been parsed.
 */
public class JdbcSourceTaskConfig extends JdbcSourceConnectorConfig {

  public static final String TABLES_CONFIG = "tables";
  private static final String TABLES_DOC = "List of tables for this task to watch for changes.";
  public static final String TABLES_FETCHED = "tables.fetched";

  public static final String STORED_PROCEDURE_CONFIG = "stored.procedure.name";
  public static final String STORED_PROCEDURE_DOC = "Stored Procedure name.";
  private static final String STORED_PROCEDURE_DEFAULT = "";

  static ConfigDef config = baseConfigDef()
      .define(TABLES_CONFIG, Type.LIST, Importance.HIGH, TABLES_DOC)
      .define(STORED_PROCEDURE_CONFIG, Type.STRING, STORED_PROCEDURE_DEFAULT,
              Importance.HIGH, STORED_PROCEDURE_DOC)
      .defineInternal(TABLES_FETCHED, Type.BOOLEAN, false, Importance.HIGH);

  public JdbcSourceTaskConfig(Map<String, String> props) {
    super(config, props);
  }
}
