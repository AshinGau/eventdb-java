package org.osv.eventdb.hbase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NavigableSet;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.coprocessor.BaseRegionObserver;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.InclusiveStopFilter;
import org.apache.hadoop.hbase.regionserver.Region;
import org.apache.hadoop.hbase.regionserver.RegionScanner;
import org.apache.hadoop.hbase.regionserver.wal.WALEdit;
import org.apache.hadoop.hbase.util.Bytes;
import org.osv.eventdb.util.BitArray;
import org.xerial.snappy.Snappy;

public abstract class MDRegionObserver extends BaseRegionObserver {
	/**
	 * bitArray saves the bit-information of the result of the query in current time
	 * bucket.
	 * 
	 * @param events       byte array of events of the current time bucket
	 * @param bitArray     bit-information of the result of the query
	 * @param eventCount   the number of events in current time bucket
	 * @param extractCount equals bitArray.count()
	 * @return byte[] byte array of events of the result in current time bucket
	 * @throws IOException if bitArray.getBitLength() < eventCount or
	 *                     bitArray.getBitLength() > eventCount + 8
	 */
	protected abstract byte[] extractEvents(byte[] events, BitArray bitArray, int eventCount, int extractCount)
			throws IOException;

	// and-operation
	private static void and(byte[] left, byte[] right) {
		if (left == null || right == null)
			return;
		if (left.length != right.length)
			return;
		int length = left.length;
		for (int i = 0; i < length; i++)
			left[i] &= right[i];
	}

	// or-operation
	private static void or(byte[] left, byte[] right) {
		if (left == null || right == null)
			return;
		if (left.length != right.length)
			return;
		int length = left.length;
		for (int i = 0; i < length; i++)
			left[i] |= right[i];
	}

	private static int getCommand(byte[] b) {
		if (b.length < 4)
			return -1;
		byte[] command = new byte[4];
		for (int i = 0; i < 4; i++)
			command[i] = b[i];
		return Bytes.toInt(command);
	}

	private static int getPropertyNameID(byte[] b) {
		if (b.length < 8)
			return -1;
		byte[] id = new byte[4];
		for (int i = 0; i < 4; i++)
			id[i] = b[i + 4];
		return Bytes.toInt(id);
	}

	private static byte[] getValueBytes(byte[] b) {
		if (b.length < 9)
			return null;
		int length = b.length - 8;
		byte[] value = new byte[length];
		for (int i = 0; i < length; i++)
			value[i] = b[i + 8];
		return value;
	}

	private static void checkParam(int nameID, byte[] value) throws IOException {
		if (nameID < 0 || value == null)
			throw new IOException("PARAMETER ERROR!");
	}

	private static void doGetRowCommand(int nameID, byte[] value, Map<Integer, List<byte[]>> op) throws IOException {
		checkParam(nameID, value);
		List<byte[]> paramList = op.get(nameID);
		if (paramList == null) {
			paramList = new ArrayList<byte[]>();
			paramList.add(value);
			op.put(nameID, paramList);
		} else {
			paramList.add(value);
		}
	}

	private static void doScanStartCommand(int nameID, byte[] value, Map<Integer, List<byte[]>> op) throws IOException {
		List<byte[]> paramList = op.get(nameID);
		if (paramList == null) {
			paramList = new ArrayList<byte[]>();
			paramList.add(value);
			op.put(nameID, paramList);
		} else {
			paramList.add(0, value);
		}
	}

	private static void doScanEndCommand(int nameID, byte[] value, Map<Integer, List<byte[]>> op) throws IOException {
		doGetRowCommand(nameID, value, op);
	}

	/**
	 * Called before the client performs a Get.
	 * <p>
	 * customer commands: get 'tableName', 'bucketRowkey', 'data:__MDQUERY__',
	 * ['data:__INDEXONLY__',] 'data:p#property#start#startKey',
	 * 'data:p#property#end#endKey', 'data:p#property#get#rowkey'
	 * <p>
	 * Call CoprocessorEnvironment#bypass to skip default actions
	 * <p>
	 * Call CoprocessorEnvironment#complete to skip any subsequent chained
	 * coprocessors
	 * 
	 * @param c      the environment provided by the region server
	 * @param get    the Get request
	 * @param result The result to return to the client if default processing is
	 *               bypassed. Can be modified. Will not be used if default
	 *               processing is not bypassed.
	 * @throws IOException if an error occurred on the coprocessor
	 */
	@Override
	public void preGetOp(final ObserverContext<RegionCoprocessorEnvironment> c, final Get get, final List<Cell> result)
			throws IOException {
		Map<byte[], NavigableSet<byte[]>> colMap = get.getFamilyMap();
		if (colMap == null)
			return;
		NavigableSet<byte[]> cols = colMap.get(Command.dataBytes);
		if (cols == null)
			return;
		// common get or mdQuery
		boolean hasMdQuery = false;
		// return bit-index or event result
		boolean indexOnly = false;
		for (byte[] col : cols) {
			if (getCommand(col) == Command.mdQueryInt)
				hasMdQuery = true;
			if (getCommand(col) == Command.indexOnlyInt)
				indexOnly = true;
		}
		if (!hasMdQuery)
			return;

		// get single row
		Map<Integer, List<byte[]>> getOp = new HashMap<Integer, List<byte[]>>();
		// scan multiple rows
		Map<Integer, List<byte[]>> scanOp = new HashMap<Integer, List<byte[]>>();
		// fomat query commands of perperties
		for (byte[] col : cols) {
			int command = getCommand(col);
			int nameID = getPropertyNameID(col);
			byte[] value = getValueBytes(col);
			if (command == Command.getRowInt)
				doGetRowCommand(nameID, value, getOp);
			else if (command == Command.scanStartInt)
				doScanStartCommand(nameID, value, scanOp);
			else if (command == Command.scanEndInt)
				doScanEndCommand(nameID, value, scanOp);

		}

		// check scan operation
		for (Map.Entry<Integer, List<byte[]>> entry : scanOp.entrySet()) {
			List<byte[]> paramList = entry.getValue();
			if (paramList.size() != 2)
				throw new IOException("PARAMETER ERROR!");
		}

		// get region
		Region hregion = c.getEnvironment().getRegion();

		// scan commands
		byte[] scanResult = null;
		for (Map.Entry<Integer, List<byte[]>> entry : scanOp.entrySet()) {
			byte[] singleScanResult = null;
			List<byte[]> paramList = entry.getValue();
			byte[] start = Bytes.add(get.getRow(), Bytes.toBytes(entry.getKey()), paramList.get(0));
			byte[] end = Bytes.add(get.getRow(), Bytes.toBytes(entry.getKey()), paramList.get(1));
			Filter filter = new InclusiveStopFilter(end);
			Scan scan = new Scan();
			scan.setStartRow(start);
			scan.addColumn(Command.dataBytes, Command.valueBytes);
			scan.setFilter(filter);
			RegionScanner regionScanner = hregion.getScanner(scan);
			List<Cell> internalResult = null;
			boolean hasnext = true;
			do {
				internalResult = new ArrayList<Cell>();
				hasnext = regionScanner.next(internalResult);
				if (internalResult != null && internalResult.size() > 0) {
					byte[] singleRowResult = Snappy.uncompress(CellUtil.cloneValue(internalResult.get(0)));
					if (singleScanResult == null)
						singleScanResult = singleRowResult;
					else
						or(singleScanResult, singleRowResult);
				}
			} while (hasnext);
			regionScanner.close();
			if (scanResult == null)
				scanResult = singleScanResult;
			else
				and(scanResult, singleScanResult);
		}
		// get commands
		byte[] getResult = null;
		for (Map.Entry<Integer, List<byte[]>> entry : getOp.entrySet()) {
			List<byte[]> paramList = entry.getValue();
			byte[] singleGet = null;
			for (byte[] param : paramList) {
				Result paramResult = hregion
						.get(new Get(Bytes.add(get.getRow(), Bytes.toBytes(entry.getKey()), param)));
				if (paramResult != null && paramResult.size() > 0) {
					byte[] paramBits = Snappy.uncompress(CellUtil
							.cloneValue(paramResult.getColumnLatestCell(Command.dataBytes, Command.valueBytes)));
					if (singleGet == null)
						singleGet = paramBits;
					else
						or(singleGet, paramBits);
				}
			}
			if (getResult == null)
				getResult = singleGet;
			else
				and(getResult, singleGet);
		}
		// and
		if (scanResult == null)
			scanResult = getResult;
		else
			and(scanResult, getResult);

		if (indexOnly && scanResult != null) {
			result.add(new KeyValue(get.getRow(), Command.dataBytes, Command.valueBytes, scanResult));
		} else if (!indexOnly && scanResult != null) {
			// get events
			BitArray bitArray = new BitArray(scanResult);
			Result eventsResult = hregion.get(new Get(get.getRow()));
			byte[] events = Snappy.uncompress(
					CellUtil.cloneValue(eventsResult.getColumnLatestCell(Command.dataBytes, Command.valueBytes)));
			int eventsCnt = Bytes.toInt(
					CellUtil.cloneValue(eventsResult.getColumnLatestCell(Command.dataBytes, Command.countBytes)));
			int finalCnt = bitArray.count();
			byte[] finalEvents = extractEvents(events, bitArray, eventsCnt, finalCnt);
			result.add(new KeyValue(get.getRow(), Command.dataBytes, Command.valueBytes, Snappy.compress(finalEvents)));
			result.add(new KeyValue(get.getRow(), Command.dataBytes, Command.countBytes, Bytes.toBytes(finalCnt)));
		}

		c.bypass();
	}

	/**
	 * Called before the client stores a value.
	 * <p>
	 * customer commands: put 'tableName', 'rowkey', 'data:__MDINSERT__' => 'append'
	 * | 'prepend', 'data:value' => 'value'
	 * <p>
	 * Call CoprocessorEnvironment#bypass to skip default actions
	 * <p>
	 * Call CoprocessorEnvironment#complete to skip any subsequent chained
	 * coprocessors
	 * 
	 * @param c          the environment provided by the region server
	 * @param put        The Put object
	 * @param edit       The WALEdit object that will be written to the wal
	 * @param durability Persistence guarantee for this Put
	 * @throws IOException if an error occurred on the coprocessor
	 */
	@Override
	public void prePut(final ObserverContext<RegionCoprocessorEnvironment> c, final Put put, final WALEdit edit,
			final Durability durability) throws IOException {
		// command put
		List<Cell> mdInsert = put.get(Command.dataBytes, Command.mdInsertBytes);
		if (mdInsert == null || mdInsert.size() == 0)
			return;
		try {
            int mdOp = Bytes.toInt(CellUtil.cloneValue(mdInsert.get(0)));
            // append or prepend
            if (mdOp == Command.appendInt || mdOp == Command.prependInt) {
                List<Cell> valueCells = put.get(Command.dataBytes, Command.valueBytes);
                if (valueCells == null || valueCells.size() == 0)
                    return;
                byte[] value = Snappy.uncompress(CellUtil.cloneValue(valueCells.get(0)));
                byte[] rowkey = put.getRow();
                Region hregion = c.getEnvironment().getRegion();
                Result oldValueResult = hregion.get(new Get(rowkey));
                Cell oldValueCell = oldValueResult.getColumnLatestCell(Command.dataBytes, Command.valueBytes);
                byte[] oldValue = Snappy.uncompress(CellUtil.cloneValue(oldValueCell));
                byte[] newValue;
                if (mdOp == Command.appendInt)
                    newValue = Bytes.add(oldValue, value);
                else
                    newValue = Bytes.add(value, oldValue);
                Put newPut = new Put(rowkey);
                newPut.addColumn(Command.dataBytes, Command.valueBytes, Snappy.compress(newValue));
                hregion.put(newPut);
            }
        }catch (Exception e){
			c.complete();
		    return;
        }
		c.bypass();
	}
}
