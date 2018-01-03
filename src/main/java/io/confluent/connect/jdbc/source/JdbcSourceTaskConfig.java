/**
 * Copyright 2015 Confluent Inc.
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
 **/

package io.confluent.connect.jdbc.source;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Importance;
import org.apache.kafka.common.config.ConfigDef.Type;

import io.confluent.connect.jdbc.util.DateTimeUtils;
import java.util.Map;

/**
 * Configuration options for a single JdbcSourceTask. These are processed after all
 * Connector-level configs have been parsed.
 */
public class JdbcSourceTaskConfig extends JdbcSourceConnectorConfig {

  public static final String TABLES_CONFIG = "tables";
  private static final String TABLES_DOC = "List of tables for this task to watch for changes.";

  public static final String DB_TIMEZONE_CONFIG = "db.timezone";
  public static final String DB_TIMEZONE_DEFAULT = "UTC";
  private static final String DB_TIMEZONE_CONFIG_DOC =
      "Alternative TimeZone of the database, to be used by JDBC driver instead of UTC (default)"
      + "when instantiating PreparedStatements. If set to special value '"
      + DateTimeUtils.JVM_TIMEZONE + "', the driver will use the timezone of"
      + "the virtual machine running the task.";

  public static final String VIEWS_DEFINITIONS = "views.definitions";
  private static final String VIEWS_DEFINITIONS_DOC =
      "List of views SQL definitions for this task, in the same order than views tags appearing"
      + "in TABLES_CONFIG. Each SQL definition must be enclosed in parenthesis.";
  public static final String VIEWS_DEFINITIONS_DEFAULT = "";
  
  static ConfigDef config = baseConfigDef()
      .define(TABLES_CONFIG, Type.LIST, Importance.HIGH, TABLES_DOC)
      .define(DB_TIMEZONE_CONFIG, Type.STRING, DB_TIMEZONE_DEFAULT,
          Importance.MEDIUM, DB_TIMEZONE_CONFIG_DOC)
      .define(VIEWS_DEFINITIONS, Type.STRING, VIEWS_DEFINITIONS_DEFAULT,
          Importance.MEDIUM, VIEWS_DEFINITIONS_DOC);

  public JdbcSourceTaskConfig(Map<String, String> props) {
    super(config, props);
  }
}
