
package io.confluent.connect.jdbc.gp.gpfdist.framweork.support;

import io.confluent.connect.jdbc.dialect.DatabaseDialect;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.UUID;

public class DefaultLoadService implements LoadService {

	private static final Logger log = LoggerFactory.getLogger(DefaultLoadService.class);


	private final DatabaseDialect jdbcTemplate;

	public DefaultLoadService(DatabaseDialect jdbcTemplate) {
		this.jdbcTemplate = jdbcTemplate;
//		Assert.notNull(jdbcTemplate, "JdbcTemplate must be set");
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
