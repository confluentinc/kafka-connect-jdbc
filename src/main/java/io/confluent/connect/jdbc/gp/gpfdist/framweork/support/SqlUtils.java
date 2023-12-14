
package io.confluent.connect.jdbc.gp.gpfdist.framweork.support;

import io.confluent.connect.jdbc.sink.JdbcSinkConfig;


import java.util.List;

/**
 * Utilities creating various types of sql clauses
 * needed with gpfdist.
 *

 */
public abstract class SqlUtils {

	//{"format=csv","delimiter=,","null=","escape="","quote="","header=true","location_uris=","execute_on=ALL_SEGMENTS","reject_limit=999999999","reject_limit_type=rows","log_errors=enable","encoding=UTF8","is_writable=false"}
	//{"format=csv","delimiter=,","null=","escape="","quote="","location_uris=              ","execute_on=ALL_SEGMENTS","log_errors=disable","encoding=UTF8","is_writable=false"}
	public static String createExternalReadableTable(LoadConfiguration config,
			List<String> overrideLocations) {

		// TODO: this function needs a cleanup
		StringBuilder buf = new StringBuilder();

		// unique table name
		buf.append("CREATE READABLE EXTERNAL TABLE ");
		buf.append(config.getExternalTable().getName());
		buf.append(" ( ");

		// column types or like
		ReadableTable externalTable = config.getExternalTable();
		if (externalTable.getLike() != null) {
			buf.append("LIKE ");
			buf.append(config.getTable());
		}
		else if (StringUtils.hasText(externalTable.getColumnsWithDataType())) {
			buf.append(externalTable.getColumnsWithDataType());
		}
		else {
			buf.append("LIKE ");
			buf.append(config.getTable());
		}
		buf.append(" ) ");

		// locations
		buf.append("LOCATION(");
		if (overrideLocations != null && !overrideLocations.isEmpty()) {
			buf.append(createLocationString(overrideLocations.toArray(new String[0])));
		}
		else {
			buf.append(createLocationString(externalTable.getLocations().toArray(new String[0])));
		}
		buf.append(") ");

		// format type
		if (externalTable.getFormat() == Format.TEXT) {
			buf.append("FORMAT 'TEXT'");
		}
		else {
			buf.append("FORMAT 'CSV'");
		}

		// format parameters
		buf.append(" ( ");
		buf.append("DELIMITER '");
		if (externalTable.getDelimiter() != null) {
			buf.append(unicodeEscaped(externalTable.getDelimiter().charValue()));
		}
		else {
			buf.append("|");
		}
		buf.append("'");

		if (externalTable.getNullString() != null) {
			buf.append(" NULL '");
			buf.append(externalTable.getNullString());
			buf.append("'");
		}

		if (externalTable.getEscape() != null) {
			buf.append(" ESCAPE '");
			buf.append(externalTable.getEscape());
			buf.append("'");
		}

		if (externalTable.getQuote() != null) {
			buf.append(" QUOTE '");
			buf.append(externalTable.getQuote());
			buf.append("'");
		}

		// ERROR ENABLED


		if (externalTable.getForceQuote() != null && externalTable.getForceQuote().length > 0) {
			buf.append(" FORCE QUOTE ");
			buf.append(StringUtils.arrayToCommaDelimitedString(externalTable.getForceQuote()));
		}

		buf.append(" )");

		if (externalTable.getEncoding() != null) {
			buf.append(" ENCODING '");
			buf.append(externalTable.getEncoding());
			buf.append("'");
		}

		if (externalTable.getSegmentRejectLimit() != null && externalTable.getSegmentRejectType() != null) {
			if (externalTable.isLogErrors()) {
				buf.append(" LOG ERRORS");
			}
			buf.append(" SEGMENT REJECT LIMIT ");
			buf.append(externalTable.getSegmentRejectLimit());
			buf.append(" ");
			buf.append(externalTable.getSegmentRejectType());
		}

		return buf.toString();
	}

	/**
	 *
	 * @param config the load configuration
	 * @return the drop DDL
	 */
	public static String dropExternalReadableTable(LoadConfiguration config) {
		StringBuilder b = new StringBuilder();

		// unique table name

		b.append("DROP EXTERNAL TABLE ");
		b.append(config.getExternalTable().getName());

		return b.toString();

	}

	/**
	 * Builds sql clause to load data into a database.
	 *
	 * @param config Load configuration.
	 * @return the load DDL
	 */
	public static String load(LoadConfiguration config) {
		if (config.getMode() == JdbcSinkConfig.InsertMode.INSERT) {
			return loadInsert(config);
		}
		else if (config.getMode() == JdbcSinkConfig.InsertMode.UPDATE) {
			return loadUpdate(config);
		}
		throw new IllegalArgumentException("Unsupported mode " + config.getMode());
	}

	private static String loadInsert(LoadConfiguration config) {
		StringBuilder b = new StringBuilder();


		b.append("INSERT INTO ");
		b.append(config.getTable());
		if(config.gpfUseColumnsInInsert && StringUtils.hasText(config.getColumns())){
			b.append(" (");
			b.append(config.getColumns());
			b.append(") ");
		}

		b.append(" SELECT ");
		if (config.gpfUseColumnsInSelect && StringUtils.hasText(config.getColumns())) {
			b.append(config.getColumns());
		}
		else {
			b.append("*");
		}
		b.append(" FROM ");
		b.append(config.getExternalTable().getName());

		return b.toString();
	}

	private static String loadUpdate(LoadConfiguration config) {
		StringBuilder b = new StringBuilder();
		b.append("UPDATE ");
		b.append(config.getTable());
		b.append(" into_table set ");

		for (int i = 0; i < config.getUpdateColumns().size(); i++) {
			b.append(config.getUpdateColumns().get(i) + "=from_table." + config.getUpdateColumns().get(i));
			if (i + 1 < config.getUpdateColumns().size()) {
				b.append(", ");
			}
		}

		b.append(" FROM ");
		b.append(config.getExternalTable().getName());
		b.append(" from_table where ");

		for (int i = 0; i < config.getMatchColumns().size(); i++) {
			b.append("into_table." + config.getMatchColumns().get(i) + "=from_table." + config.getMatchColumns().get(i));
			if (i + 1 < config.getMatchColumns().size()) {
				b.append(" and ");
			}
		}

		if (StringUtils.hasText(config.getUpdateCondition())) {
			b.append(" and " + config.getUpdateCondition());
		}

		return b.toString();
	}


//	def do_method_merge(self):
//			"""insert data not already in the table, update remaining items"""
//
//			self.table_supports_update()
//			self.create_staging_table()
//			self.create_external_table()
//			self.do_insert(self.staging_table_name)
//	self.rowsInserted = 0 # MPP-13024. No rows inserted yet (only to temp table).
//			self.do_update(self.staging_table_name, 0)
//
//			# delete the updated rows in staging table for merge
//        # so we can directly insert new rows left in staging table
//        # and avoid left outer join when insert new rows which is poor in performance
//
//	match = self.map_stuff('gpload:output:match_columns'
//			, lambda x,y:'staging_table.%s=into_table.%s' % (x, y)
//			, 0)
//	sql = 'DELETE FROM %s staging_table '% self.staging_table_name
//	sql += 'USING %s into_table WHERE '% self.get_qualified_tablename()
//	sql += ' %s' % ' AND '.join(match)
//
//        self.log(self.LOG, sql)
//			if not self.options.D:
//			try:
//	with self.conn.cursor() as cur:
//			cur.execute(sql)
//	except Exception as e:
//			self.log(self.ERROR, '{} encountered while running {}'.format(e, sql))
//
//			# insert new rows to the target table
//
//			match = self.map_stuff('gpload:output:match_columns',lambda x,y:'into_table.%s=from_table.%s'%(x,y),0)
//	matchColumns = self.getconfig('gpload:output:match_columns',list)
//
//	cols = [a for a in self.into_columns if a[2] != None]
//	sql = 'INSERT INTO %s ' % self.get_qualified_tablename()
//	sql += '(%s) ' % ','.join([a[0] for a in cols])
//	sql += '(SELECT %s ' % ','.join(['from_table.%s' % a[0] for a in cols])
//	sql += 'FROM (SELECT *, row_number() OVER (PARTITION BY %s) AS gpload_row_number ' % ','.join(matchColumns)
//	sql += 'FROM %s) AS from_table ' % self.staging_table_name
//	sql += 'WHERE gpload_row_number=1)'
//			self.log(self.LOG, sql)
//			if not self.options.D:
//			try:
//	with self.conn.cursor() as cur:
//			cur.execute(sql)
//	self.rowsInserted = cur.rowcount
//	except Exception as e:
//			self.log(self.ERROR, '{} encountered while running {}'.format(e, sql))
//
//



	/**
	 * Converts string array to greenplum friendly string. From new
	 * String[]{"foo","jee"} we get "'foo',jee'".
	 *
	 * @param strings
	 *            String array to explode
	 * @return Comma delimited string with values encapsulated with
	 *         apostropheres. ‘'
	 */
	public static String createLocationString(String[] strings) {
		StringBuilder locString = new StringBuilder();
		for (int i = 0; i < strings.length; i++) {
			String string = strings[i];
			locString.append("'");
			locString.append(string);
			locString.append("'");
			if (i < strings.length - 1) {
				locString.append(",");
			}
		}
		return locString.toString();
	}

	private static String unicodeEscaped(char ch) {
		return String.valueOf(ch);
//		if (ch < 0x10) {
//			return "\\u000" + Integer.toHexString(ch);
//		}
//		else if (ch < 0x100) {
//			return "\\u00" + Integer.toHexString(ch);
//		}
//		else if (ch < 0x1000) {
//			return "\\u0" + Integer.toHexString(ch);
//		}
//		return "\\u" + Integer.toHexString(ch);
	}

}
