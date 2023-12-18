package io.confluent.connect.jdbc.gp.gpfdist.framweork;///*
import io.confluent.connect.jdbc.dialect.DatabaseDialect;
import io.confluent.connect.jdbc.gp.gpfdist.framweork.support.*;
import io.confluent.connect.jdbc.sink.JdbcSinkConfig;

import java.util.Arrays;
import java.util.List;

/**
 * Configuration for all beans needed for gpfdist sink.
 *

 */
public class GpfdistSinkConfiguration {

	private JdbcSinkConfig config;
	private String table;


	private String columns;	private String externalTableName;
	private String columnsWithDataType;
	private List<String> matchColumns;
	private List<String> updateColumns;
	private String updateCondition;
	private List<String> sqlBefore;
	private List<String> sqlAfter;


	public GpfdistSinkConfiguration(JdbcSinkConfig config,String externalTableName, String table, String columns, String columnsWithDataType, List<String> matchColumns, List<String> updateColumns, String updateCondition, List<String> sqlBefore, List<String> sqlAfter){
		this.externalTableName = externalTableName;
		this.config = config;
		this.table = table;
		this.columns = columns;
		this.columnsWithDataType = columnsWithDataType;
		this.matchColumns = matchColumns;
		this.updateColumns = updateColumns;
		this.updateCondition = updateCondition;
		this.sqlBefore = sqlBefore;
		this.sqlAfter = sqlAfter;
	}

//
//	public BasicDataSource dataSource() {
//		BasicDataSource ds = new BasicDataSource();
//		ds.setDriverClassName("org.postgresql.Driver");
//		ConnectionURLParser parser = new ConnectionURLParser(config.connectionUrl);
//
//		if (StringUtils.hasText(parser.getUsername())) {
//			ds.setUsername(parser.getUsername());
//		}
//		if (StringUtils.hasText(parser.getPassword())) {
//			ds.setPassword(parser.getPassword());
//		}
//		ds.setUrl(config.connectionUrl);
//		return ds;
//
//	}

	private void setSegmentReject(String reject, ReadableTable table) {
		if (!StringUtils.hasText(reject)) {
			return;
		}
		Integer parsedLimit = null;
		try {
			parsedLimit = Integer.parseInt(reject);
			table.setSegmentRejectType(SegmentRejectType.ROWS);
		} catch (NumberFormatException e) {
		}
		if (parsedLimit == null && reject.contains("%")) {
			try {
				parsedLimit = Integer.parseInt(reject.replace("%", "").trim());
				table.setSegmentRejectType(SegmentRejectType.PERCENT);
			} catch (NumberFormatException e) {
			}
		}
		table.setSegmentRejectLimit(parsedLimit);
	}
	public ReadableTable greenplumReadableTable() {

		ReadableTable readableTable = new ReadableTable();
		readableTable.setLocations(Arrays.asList(NetworkUtils.getGPFDistUri(config.getGpfdistHost(), config.getGpfdistPort())));
		readableTable.setColumns(columns);
		readableTable.setColumnsWithDataType(columnsWithDataType);
		readableTable.setName(externalTableName);

		// TODO- set like only if destination table have same name and number of columns as source table
		//readableTable.setLike(true);
		readableTable.setLogErrors(config.gpLogErrors);

		setSegmentReject(config.segmentRejectLimit, readableTable);
		if (config.segmentRejectLimit != null) {
			try {
				int value = Integer.valueOf(config.segmentRejectLimit);
				if (value > 0) {
					readableTable.setSegmentRejectLimit(value);
				}

			}catch (NumberFormatException e) {
				e.printStackTrace();

			}

		}
		readableTable.setSegmentRejectType(config.segmentRejectType);

		//TODO - add type config
		Format format = Format.CSV;
		Character delimiter = config.getDelimiter();

		if (format == Format.TEXT) {
			Character delim = delimiter != null ? delimiter : Character.valueOf('|');
			readableTable.setTextFormat(delim, config.nullString, '\\');
		}
		else if (format == Format.CSV) {
			Character delim = delimiter != null ? delimiter : Character.valueOf(',');
			readableTable.setCsvFormat('"', delim, config.nullString, new String[]{}, '"');
		}
// Format type: csv
//Format options: delimiter ',' null '' escape '"' quote '"' header
// FORMAT 'CSV' ( DELIMITER '\u002c' NULL '' ESCAPE '"' QUOTE '"' )
		return readableTable;

	}


	public LoadConfiguration greenplumLoadConfiguration() {
        		ReadableTable externalTable = greenplumReadableTable();
		LoadConfiguration loadConfiguration = new LoadConfiguration(table, columns, columnsWithDataType, externalTable, this.config.insertMode, matchColumns,
				updateColumns, updateCondition);
		loadConfiguration.setSqlBefore(sqlBefore);
		loadConfiguration.setSqlAfter(sqlAfter);
		return loadConfiguration;

	}


	public GreenplumLoad greenplumLoad(DatabaseDialect dialect) {
		LoadConfiguration loadConfiguration = greenplumLoadConfiguration();
//		DataSource dataSource = dataSource();
//		JdbcTemplate jdbcTemplate = new JdbcTemplate(dataSource);
		return new DefaultGreenplumLoad(loadConfiguration, new DefaultLoadService(dialect));
	}


//	public GpfdistMessageHandler gpfdist() {
//		GreenplumLoad greenplumLoad = greenplumLoad();
////		TaskScheduler sqlTaskScheduler = new ThreadPoolTaskScheduler();
////		((ThreadPoolTaskScheduler) sqlTaskScheduler).setPoolSize(1);
////		((ThreadPoolTaskScheduler) sqlTaskScheduler).setThreadNamePrefix("sqlTaskScheduler");
////		((ThreadPoolTaskScheduler) sqlTaskScheduler).initialize();
//		GpfdistMessageHandler handler = new GpfdistMessageHandler(config.getGpfdistPort(), config.gpfFlushCount,
//				config.gpfFlushTime, config.gpfBatchTimeout, config.gpfBatchCount, config.gpfBatchPeriod,
//				config.getDelimiter().toString(), config.getGpfdistHost());
//		//handler.setRateInterval(config.);
////		handler.setGreenplumLoad(greenplumLoad);
////		handler.setSqlTaskScheduler(sqlTaskScheduler);
//		return handler;
//	}
}
