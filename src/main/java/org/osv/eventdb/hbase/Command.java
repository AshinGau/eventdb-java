package org.osv.eventdb.hbase;

/**
 * Commnad name
 */
public enum Command{
	MDINSERT,
	MDQUERY,
	INDEXONLY,
	APPEND,
	PREPEND,
	GETROW,
	SCANSTART,
	SCANEND,
	META_INITSPLIT,
	META_SPLIT,
	META_REGIONS,
	META_TIMEBUCKET,
}