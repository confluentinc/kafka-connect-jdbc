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

package io.confluent.connect.jdbc.dialect;

import io.confluent.connect.jdbc.sink.metadata.SinkRecordField;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.connect.data.Schema;

import java.util.HashMap;
import java.util.Map;

/**
 * A {@link DatabaseDialect} for Redshift.
 */
public class RedshiftDatabaseDialect extends PostgreSqlDatabaseDialect {

  private static final Map<Schema.Type, String> TYPE_MAPPING = new HashMap<Schema.Type, String>() {
    {
      put(Schema.Type.STRING, "VARCHAR(MAX)");
    }
  };

  /**
   * The provider for {@link RedshiftDatabaseDialect}.
   */
  public static class Provider extends DatabaseDialectProvider.SubprotocolBasedProvider {

    /**
     * Create a new provider.
     */
    public Provider() {
      super(RedshiftDatabaseDialect.class.getSimpleName(), "redshift");
    }

    public DatabaseDialect create(final AbstractConfig config) {
      return new RedshiftDatabaseDialect(config);
    }
  }

  /**
   * Create a new dialect instance with the given connector configuration.
   *
   * @param config the connector configuration; may not be null
   */
  public RedshiftDatabaseDialect(final AbstractConfig config) {
    super(config);
  }

  @Override
  protected String getSqlType(final SinkRecordField field) {
    if (TYPE_MAPPING.containsKey(field.schemaType())) {
      return TYPE_MAPPING.get(field.schemaType());
    }

    return super.getSqlType(field);
  }
}