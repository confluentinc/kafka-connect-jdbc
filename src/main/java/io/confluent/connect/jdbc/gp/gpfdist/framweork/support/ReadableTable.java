package io.confluent.connect.jdbc.gp.gpfdist.framweork.support;

/**
 * Settings for readable external table.
 *

 */
public class ReadableTable extends AbstractExternalTable {

	// [LOG ERRORS]
	private boolean logErrors;

	// SEGMENT REJECT LIMIT count
	private Integer segmentRejectLimit;

	// [ROWS | PERCENT]
	private SegmentRejectType segmentRejectType;

	// FORMAT 'TEXT|CVS' [( [HEADER]
	private boolean formatHeader;

	public boolean isFormatHeader() {
		return formatHeader;
	}

	public void setFormatHeader(boolean formatHeader) {
		this.formatHeader = formatHeader;
	}

	public boolean isLogErrors() {
		return logErrors;
	}

	public void setLogErrors(boolean logErrors) {
		this.logErrors = logErrors;
	}

	public Integer getSegmentRejectLimit() {
		return segmentRejectLimit;
	}

	public void setSegmentRejectLimit(Integer segmentRejectLimit) {
		this.segmentRejectLimit = segmentRejectLimit;
	}

	public SegmentRejectType getSegmentRejectType() {
		return segmentRejectType;
	}

	public void setSegmentRejectType(SegmentRejectType segmentRejectType) {
		this.segmentRejectType = segmentRejectType;
	}

}
