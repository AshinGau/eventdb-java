package org.osv.eventdb.hbase;

import org.apache.hadoop.hbase.util.Bytes;

/**
 * Commnad name
 */
public enum Command {
	// 0: insert opration
	MDINSERT,
	// 1: query operation
	MDQUERY,
	// 2: return index only
	INDEXONLY,
	// 3: append when put
	APPEND,
	// 4: prepend when put
	PREPEND,
	// 5: get single row when get
	GETROW,
	// 6: set start row when get
	SCANSTART,
	// 7: set end row(inclusive) when get
	SCANEND,
	// 8: get meta information of initial split number
	META_INITSPLIT,
	// 9: get meta information of current region prefix of the split
	META_SPLIT,
	// 10: get meta information of number of total regions
	META_REGIONS,
	// 11: get meta informatin of region prefix of the timeBucket
	META_BUCKET;

	public static byte[] dataBytes = Bytes.toBytes("data");
	public static byte[] valueBytes = Bytes.toBytes("value");
	public static byte[] countBytes = Bytes.toBytes("count");
	public static byte[] metaZeroBytes = Bytes.toBytes((int) 0);

	public static int mdInsertInt = Command.MDINSERT.ordinal();
	public static byte[] mdInsertBytes = Bytes.toBytes(mdInsertInt);

	public static int mdQueryInt = Command.MDQUERY.ordinal();
	public static byte[] mdQueryBytes = Bytes.toBytes(mdQueryInt);

	public static int indexOnlyInt = Command.INDEXONLY.ordinal();
	public static byte[] indexOnlyBytes = Bytes.toBytes(indexOnlyInt);

	public static int appendInt = Command.APPEND.ordinal();
	public static byte[] appendBytes = Bytes.toBytes(appendInt);

	public static int prependInt = Command.PREPEND.ordinal();
	public static byte[] prependBytes = Bytes.toBytes(prependInt);

	public static int getRowInt = Command.GETROW.ordinal();
	public static byte[] getRowBytes = Bytes.toBytes(getRowInt);

	public static int scanStartInt = Command.SCANSTART.ordinal();
	public static byte[] scanStartBytes = Bytes.toBytes(scanStartInt);

	public static int scanEndInt = Command.SCANEND.ordinal();
	public static byte[] scanEndBytes = Bytes.toBytes(scanEndInt);

	public static int metaInitSplitInt = Command.META_INITSPLIT.ordinal();
	public static byte[] metaInitSplitBytes = Bytes.toBytes(metaInitSplitInt);

	public static int metaSplitInt = Command.META_SPLIT.ordinal();
	public static byte[] metaSplitBytes = Bytes.toBytes(metaSplitInt);

	public static int metaRegionsInt = Command.META_REGIONS.ordinal();
	public static byte[] metaRegionsBytes = Bytes.toBytes(metaRegionsInt);

	public static int metaBucketInt = Command.META_BUCKET.ordinal();
	public static byte[] metaBucketBytes = Bytes.toBytes(metaBucketInt);
}