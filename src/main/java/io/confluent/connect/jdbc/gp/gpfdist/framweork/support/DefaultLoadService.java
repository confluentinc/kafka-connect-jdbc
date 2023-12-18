
package io.confluent.connect.jdbc.gp.gpfdist.framweork.support;

import io.confluent.connect.jdbc.dialect.DatabaseDialect;
import io.confluent.connect.jdbc.sink.JdbcSinkConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
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

        if (loadConfiguration.getExternalTable() != null && loadConfiguration.getExternalTable().getName() == null) {
            String prefix = UUID.randomUUID().toString().replaceAll("-", "_");
            log.debug("Using prefix {}", prefix);
            loadConfiguration.getExternalTable().setName("ext_" + loadConfiguration.getTable() + loadConfiguration.getExternalTable().getName());
        }

        // setup jdbc operations
        JdbcCommands operations = new JdbcCommands(jdbcTemplate);


        if (loadConfiguration.getMode() == JdbcSinkConfig.InsertMode.MERGE) {

            loadConfiguration.setStagingTableName(loadConfiguration.getExternalTable().getName().replaceAll("ext_", "stg_"));
            // create staging & external table
            String sqlCreateStgTable = SqlUtils.createStagingTable(loadConfiguration);
            log.info("sqlCreateStgTable={}", sqlCreateStgTable);

            String truncateStgTable = SqlUtils.truncateStagingTable(loadConfiguration);

            List sqlCreateTables =new ArrayList<>();
            sqlCreateTables.add(sqlCreateStgTable);
            sqlCreateTables.add(truncateStgTable);
            if (!isTableExist(loadConfiguration.getExternalTable().getName())) {
                log.info("Table {} does not exist. Creating it.", loadConfiguration.getExternalTable().getName());
                String sqlCreateExtTable = SqlUtils.createExternalReadableTable(loadConfiguration, context != null ? context.getLocations() : null);
                log.info("sqlCreateExtTable={}", sqlCreateExtTable);
                sqlCreateTables.add(sqlCreateExtTable);
            }
            operations.setPrepareSql(sqlCreateTables);

            // step 1 - insert into staging table
            String sqlInsertToStg = SqlUtils.loadToStaging(loadConfiguration);
            log.info("sqlInsert={}", sqlInsertToStg);

            String sqlUpdateFromStg = SqlUtils.updateFromStaging(loadConfiguration);
            log.info("sqlUpdateFromStg={}", sqlUpdateFromStg);

            String sqlInsertFromStg = SqlUtils.insertFromStaging(loadConfiguration);
            log.info("sqlInsertFromStg={}", sqlInsertFromStg);


            operations.setRunSql(Arrays.asList(sqlInsertToStg,sqlUpdateFromStg,sqlInsertFromStg));


            if (!loadConfiguration.shouldReuseTables()) {// add drop stg table
                String sqlDropTable = SqlUtils.dropExternalReadableTable(loadConfiguration);
                log.debug("sqlDropTable={}", sqlDropTable);
                operations.setCleanSql(Arrays.asList(sqlDropTable));
            }else{
                log.info("Reusing tables. Will not drop them.");
            }


        } else {
            String sqlCreateTable = SqlUtils.createExternalReadableTable(loadConfiguration,
                    context != null ? context.getLocations() : null);
            String sqlDropTable = SqlUtils.dropExternalReadableTable(loadConfiguration);
            String sqlInsert = SqlUtils.load(loadConfiguration);
            log.debug("sqlCreateTable={}", sqlCreateTable);
            log.debug("sqlDropTable={}", sqlDropTable);
            log.debug("sqlInsert={}", sqlInsert);

            operations.setPrepareSql(Arrays.asList(sqlCreateTable));
            operations.setCleanSql(Arrays.asList(sqlDropTable));
            operations.setRunSql(Arrays.asList(sqlInsert));

        }

        operations.setBeforeSqls(loadConfiguration.getSqlBefore());
        operations.setAfterSqls(loadConfiguration.getSqlAfter());

        if (!operations.execute() && operations.getLastException() != null) {
            log.error("Error in load", operations.getLastException());
            throw operations.getLastException();
        }
    }

    private boolean isTableExist(String name) {
        String checkTableExistsQuery = SqlUtils.createQueryToCheckTableExists(name);
        ResultSet rs = null;
        try {
            rs = jdbcTemplate.executeQuery(checkTableExistsQuery);
            if (rs.next()) {
                return rs.getBoolean(1);
            }


        } catch (Exception e) {
            log.error("Error while checking if table exist", e);
            return false;
        } finally {   try {
            if (rs != null && !rs.isClosed()) {

                    rs.close();

            } } catch (Exception e) {
            log.error("Error while closing result set", e);
        }
        }
        return false;

    }
}