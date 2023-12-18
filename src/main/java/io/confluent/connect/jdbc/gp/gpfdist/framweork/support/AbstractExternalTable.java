
package io.confluent.connect.jdbc.gp.gpfdist.framweork.support;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Base settings for all external tables;
 *

 * @author Gary Russell
 */
public abstract class AbstractExternalTable {


	private String name;
	// LOCATION
	private List<String> locations;

	// FORMAT 'TEXT'|'CVS'
	private Format format;

	// [DELIMITER [AS] 'delimiter' | 'OFF']
	private Character delimiter;

	// [NULL [AS] 'null string']
	private String nullString;

	// [ESCAPE [AS] 'escape' | 'OFF']
	private Character escape;

	// [QUOTE [AS] 'quote']
	private Character formatQuote;

	// [FORCE NOT NULL column [, ...]]
	private String[] formatForceQuote;

	// [ ENCODING 'encoding' ]
	private String encoding;

	private boolean like;

	private String columns;

	private String columnsWithDataType;


	public List<String> getLocations() {
		return locations;
	}

	public void setLocations(List<String> locations) {
		this.locations = new ArrayList<String>(locations);
	}

	public void setTextFormat() {
		this.format = Format.TEXT;
	}

	public void setTextFormat(Character delimiter, String nullString, Character escape) {
		this.format = Format.TEXT;
		this.delimiter = delimiter;
		this.nullString = nullString;
		this.escape = escape;
	}

	public void setCsvFormat() {
		this.format = Format.CSV;
	}

	public void setCsvFormat(Character quote, Character delimiter, String nullString, String[] forceQuote,
			Character escape) {
		this.format = Format.CSV;
		this.formatQuote = quote;
		this.delimiter = delimiter;
		this.nullString = nullString;
		this.escape = escape;
		this.formatForceQuote = Arrays.copyOf(forceQuote, forceQuote.length);
	}

	public Format getFormat() {
		return format;
	}

	public Character getDelimiter() {
		return delimiter;
	}

	public void setDelimiter(Character delimiter) {
		this.delimiter = delimiter;
	}

	public String getNullString() {
		return nullString;
	}

	public void setNullString(String nullString) {
		this.nullString = nullString;
	}

	public Character getEscape() {
		return escape;
	}

	public void setEscape(Character escape) {
		this.escape = escape;
	}

	public Character getQuote() {
		return formatQuote;
	}

	public void setQuote(Character quote) {
		this.formatQuote = quote;
	}

	public String[] getForceQuote() {
		return formatForceQuote;
	}

	public void setForceQuote(String[] forceQuote) {
		this.formatForceQuote = Arrays.copyOf(forceQuote, forceQuote.length);
	}

	public String getEncoding() {
		return encoding;
	}

	public void setEncoding(String encoding) {
		this.encoding = encoding;
	}

	public boolean getLike() {
		return like;
	}

	public void setLike(boolean like) {
		this.like = like;
	}

	public String getColumns() {
		return columns;
	}

	public void setColumns(String columns) {
		this.columns = columns;
	}

	public String getColumnsWithDataType() {
		return columnsWithDataType;
	}

	public void setColumnsWithDataType(String columnsWithDataType) {
		this.columnsWithDataType = columnsWithDataType;
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}
}
