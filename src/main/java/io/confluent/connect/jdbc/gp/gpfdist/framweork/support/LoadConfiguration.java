
package io.confluent.connect.jdbc.gp.gpfdist.framweork.support;

import io.confluent.connect.jdbc.sink.JdbcSinkConfig;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class LoadConfiguration {

	public boolean gpfUseColumnsInInsert = true; //TODO - this is a hack to get around the fact that the columns are in different order in the external table
	public boolean gpfUseColumnsInSelect = true; //TODO - this is a hack to get around the fact that the columns are in different order in the external table

	private String table;

	private String columns;

	private String columnsWithDataTypes;

	private ReadableTable externalTable;

	private JdbcSinkConfig.InsertMode mode;

	private List<String> matchColumns;

	private List<String> updateColumns;

	private String updateCondition;

	private List<String> sqlBefore;

	private List<String> sqlAfter;

	public LoadConfiguration() {
		super();
	}

	public LoadConfiguration(String table, String columns, String columnsWithDataTypes, ReadableTable externalTable, JdbcSinkConfig.InsertMode mode,
			List<String> matchColumns, List<String> updateColumns, String updateCondition) {
		this.table = table;
		this.columns = columns;
		this.externalTable = externalTable;
		this.mode = mode;
		this.matchColumns = matchColumns;
		this.updateColumns = updateColumns;
		this.updateCondition = updateCondition;
		this.columnsWithDataTypes = columnsWithDataTypes;
	}

	public String getTable() {
		return table;
	}

	public void setTable(String table) {
		this.table = table;
	}

	public String getColumns() {
		return columns;
	}

	public void setColumns(String columns) {
		this.columns = columns;
	}

	public ReadableTable getExternalTable() {
		return externalTable;
	}

	public void setExternalTable(ReadableTable externalTable) {
		this.externalTable = externalTable;
	}

	public JdbcSinkConfig.InsertMode getMode() {
		return mode;
	}

	public void setMode(JdbcSinkConfig.InsertMode mode) {
		this.mode = mode;
	}

	public List<String> getMatchColumns() {
		return matchColumns;
	}

	public void setMatchColumns(List<String> matchColumns) {
		this.matchColumns = matchColumns;
	}

	public List<String> getUpdateColumns() {
		return updateColumns;
	}

	public void setUpdateColumns(List<String> updateColumns) {
		this.updateColumns = updateColumns;
	}

	public String getUpdateCondition() {
		return updateCondition;
	}

	public void setUpdateCondition(String updateCondition) {
		this.updateCondition = updateCondition;
	}

	public List<String> getSqlBefore() {
		return sqlBefore;
	}

	public void setSqlBefore(List<String> sqlBefore) {
		this.sqlBefore = sqlBefore;
	}

	public List<String> getSqlAfter() {
		return sqlAfter;
	}

	public void setSqlAfter(List<String> sqlAfter) {
		this.sqlAfter = sqlAfter;
	}

	public String getColumnsWithDataTypes() {
		return columnsWithDataTypes;
	}

	public void setColumnsWithDataTypes(String columnsWithDataTypes) {
		this.columnsWithDataTypes = columnsWithDataTypes;
	}
}
