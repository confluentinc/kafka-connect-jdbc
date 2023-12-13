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
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.util.StringUtils;

import org.springframework.jdbc.support.SQLErrorCodesFactory;

import java.util.List;

/**
 * Utility class helping to execute jdbc operations within a load session.
 * Provides a way to prepare, run and clean a main load command. Additionally
 * it can use a list of before and after commands which are execute before and after
 * of a main command. Clean command is executed last even if some of the other
 * commands fail.
 *
 * @author Janne Valkealahti
 *
 */
public class JdbcCommands {

	private static final Log log = LogFactory.getLog(JdbcCommands.class);

	private DatabaseDialect jdbcTemplate;

	private List<String> beforeSqls;

	private List<String> afterSqls;

	private String prepareSql;

	private String runSql;

	private String cleanSql;

	private Exception lastException;

	public JdbcCommands(DatabaseDialect jdbcTemplate) {
		this.jdbcTemplate = jdbcTemplate;
	}

	public void setJdbcTemplate(DatabaseDialect jdbcTemplate) {
		this.jdbcTemplate = jdbcTemplate;
	}

	public void setPrepareSql(String sql) {
		this.prepareSql = sql;
	}

	public void setRunSql(String sql) {
		this.runSql = sql;
	}

	public void setCleanSql(String sql) {
		this.cleanSql = sql;
	}

	public void setBeforeSqls(List<String> beforeSqls) {
		this.beforeSqls = beforeSqls;
	}

	public void setAfterSqls(List<String> afterSqls) {
		this.afterSqls = afterSqls;
	}

	public boolean execute() {
		boolean succeed = true;

		try {
			succeed = prepare();
			if (succeed) {
				succeed = before();
			}
			if (succeed) {
				succeed = run();
			}
			if (succeed) {
				succeed = after();
			}
		}
		catch (Exception e) {
		}
		finally {
			try {
				clean();
			}
			catch (Exception e2) {
			}
		}
		return succeed;
	}

	public Exception getLastException() {
		return lastException;
	}

	private boolean prepare() {
		try {
			if (log.isDebugEnabled()) {
				log.debug("Executing prepare: " + prepareSql);
			}
			jdbcTemplate.getConnection().createStatement().execute(prepareSql);
		}
		catch (Exception e) {
			log.error("Error during prepare sql", e);
			lastException = e;
			return false;
		}

		return true;
	}

	private boolean run() {
		try {
			if (log.isDebugEnabled()) {
				log.debug("Executing run: " + runSql);
			}
			jdbcTemplate.getConnection().createStatement().execute(runSql);
		}
		catch (Exception e) {
			log.error("Error during run sql", e);
			lastException = e;
			return false;
		}
		return true;
	}

	private boolean clean() {
		try {
			if (log.isDebugEnabled()) {
				log.debug("Executing clean: " + cleanSql);
			}

			jdbcTemplate.getConnection().createStatement().execute(cleanSql);
		}
		catch (Exception e) {
			log.error("Error during clean sql", e);
			lastException = e;
			return false;
		}
		return true;
	}

	private boolean before() {
		if (beforeSqls != null) {
			for (String sql : beforeSqls) {
				if (!StringUtils.hasText(sql)) {
					continue;
				}
				if (log.isDebugEnabled()) {
					log.debug("Executing before: " + sql);
				}
				try {
					jdbcTemplate.getConnection().createStatement().execute(sql);
				}
				catch (Exception e) {
					lastException = e;
					return false;
				}
			}
		}
		return true;
	}

	private boolean after() {
		if (afterSqls != null) {
			for (String sql : afterSqls) {
				if (!StringUtils.hasText(sql)) {
					continue;
				}
				if (log.isDebugEnabled()) {
					log.debug("Executing after: " + sql);
				}
				try {
					jdbcTemplate.getConnection().createStatement().execute(sql);
				}
				catch (Exception e) {
					lastException = e;
					return false;
				}
			}
		}
		return true;
	}

}
