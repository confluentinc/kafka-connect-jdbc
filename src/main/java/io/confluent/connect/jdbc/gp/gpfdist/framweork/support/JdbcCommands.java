
package io.confluent.connect.jdbc.gp.gpfdist.framweork.support;

import io.confluent.connect.jdbc.dialect.DatabaseDialect;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import java.util.List;

/**
 * Utility class helping to execute jdbc operations within a load session.
 * Provides a way to prepare, run and clean a main load command. Additionally
 * it can use a list of before and after commands which are execute before and after
 * of a main command. Clean command is executed last even if some of the other
 * commands fail.
 *

 *
 */
public class JdbcCommands {

	private static final Log log = LogFactory.getLog(JdbcCommands.class);

	private DatabaseDialect jdbcTemplate;

	private List<String> beforeSqls;

	private List<String> afterSqls;

	private List<String> prepareSql;

	private List<String> runSql;

	private List<String> cleanSql;

	private Exception lastException;

	public JdbcCommands(DatabaseDialect jdbcTemplate) {
		this.jdbcTemplate = jdbcTemplate;
	}

	public void setJdbcTemplate(DatabaseDialect jdbcTemplate) {
		this.jdbcTemplate = jdbcTemplate;
	}

	public void setPrepareSql( List<String> sql) {
		this.prepareSql = sql;
	}

	public void setRunSql( List<String> sql) {
		this.runSql = sql;
	}

	public void setCleanSql( List<String> sql) {
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
		if (beforeSqls != null) {
			for (String sql : prepareSql) {
				if (!StringUtils.hasText(sql)) {
					continue;
				}
				if (log.isDebugEnabled()) {
					log.debug("Executing prepare: " + sql);
				}
				try {
					jdbcTemplate.execute(sql);
				}
				catch (Exception e) {
					lastException = e;
					return false;
				}
			}
		}
		return true;
	}



	private boolean run() {
		if (beforeSqls != null) {
			for (String sql : runSql) {
				if (!StringUtils.hasText(sql)) {
					continue;
				}
				if (log.isDebugEnabled()) {
					log.debug("Executing run: " + sql);
				}
				try {
					jdbcTemplate.execute(sql);
				}
				catch (Exception e) {
					lastException = e;
					return false;
				}
			}
		}
		return true;
	}


	private boolean clean() {
		if (beforeSqls != null) {
			for (String sql : cleanSql) {
				if (!StringUtils.hasText(sql)) {
					continue;
				}
				if (log.isDebugEnabled()) {
					log.debug("Executing clean: " + sql);
				}
				try {
					jdbcTemplate.execute(sql);
				}
				catch (Exception e) {
					lastException = e;
					return false;
				}
			}
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
					jdbcTemplate.execute(sql);
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
					jdbcTemplate.execute(sql);
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
