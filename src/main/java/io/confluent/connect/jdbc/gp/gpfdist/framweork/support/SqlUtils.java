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

import io.confluent.connect.jdbc.sink.JdbcSinkConfig;
import org.springframework.util.StringUtils;

import java.util.List;

/**
 * Utilities creating various types of sql clauses
 * needed with gpfdist.
 *
 * @author Janne Valkealahti
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
