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

public class JdbcSourceConnectorConstants {
  public static final String TABLE_NAME_KEY = "table";
  public static final String QUERY_NAME_KEY = "query";
  public static final String QUERY_NAME_VALUE = "query";
  public static final String OFFSET_PROTOCOl_VERSION_KEY = "version";
  public static final String PROTOCOL_VERSION_ONE = "1";
  public static final String TOPIC_NAME_KEY = "topic";
}
