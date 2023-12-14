package io.confluent.connect.jdbc.gp.gpfdist.framweork.support;

/**
 * Enumeration of a possible values in a clause section
 * `SEGMENT REJECT LIMIT count [ROWS | PERCENT]`
 *

 */
public enum SegmentRejectType {

	/** Rows reject type */
	ROWS,

	/** Percent reject type */
	PERCENT
}
