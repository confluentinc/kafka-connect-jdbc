/*
 * Copyright 2016 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.confluent.connect.jdbc.gp.gpfdist.framweork.support;

import io.confluent.connect.jdbc.dialect.DatabaseDialect;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.util.Assert;

import java.util.UUID;

public class DefaultLoadService implements LoadService {

	private static final Logger log = LoggerFactory.getLogger(DefaultLoadService.class);


	private final DatabaseDialect jdbcTemplate;

	public DefaultLoadService(DatabaseDialect jdbcTemplate) {
		this.jdbcTemplate = jdbcTemplate;
		Assert.notNull(jdbcTemplate, "JdbcTemplate must be set");
	}

	@Override
	public void load(LoadConfiguration loadConfiguration) throws Exception {
		load(loadConfiguration, null);
	}

	@Override
	public void load(LoadConfiguration loadConfiguration, RuntimeContext context) throws Exception {

		if(loadConfiguration.getExternalTable() != null && loadConfiguration.getExternalTable().getName() == null) {
			String prefix = UUID.randomUUID().toString().replaceAll("-", "_");
			log.debug("Using prefix {}", prefix);
			loadConfiguration.setTable(loadConfiguration.getTable()+"_ext_"+loadConfiguration.getExternalTable().getName());
		}

		// setup jdbc operations
		JdbcCommands operations = new JdbcCommands(jdbcTemplate);

		String sqlCreateTable = SqlUtils.createExternalReadableTable(loadConfiguration,
				context != null ? context.getLocations() : null);
		String sqlDropTable = SqlUtils.dropExternalReadableTable(loadConfiguration);
		String sqlInsert = SqlUtils.load(loadConfiguration);
		log.debug("sqlCreateTable={}", sqlCreateTable);
		log.debug("sqlDropTable={}", sqlDropTable);
		log.debug("sqlInsert={}", sqlInsert);

		operations.setPrepareSql(sqlCreateTable);
		operations.setCleanSql(sqlDropTable);
		operations.setRunSql(sqlInsert);

		operations.setBeforeSqls(loadConfiguration.getSqlBefore());
		operations.setAfterSqls(loadConfiguration.getSqlAfter());

		if (!operations.execute() && operations.getLastException() != null) {
			log.error("Error in load", operations.getLastException());
			throw operations.getLastException();
		}
	}

}
