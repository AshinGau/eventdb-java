package org.osv.eventdb.hbase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.NavigableSet;
import java.util.Set;

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

public class MDRegionObserver extends BaseRegionObserver {
	private static byte[] dataBytes = Bytes.toBytes("data");
	private static byte[] valueBytes = Bytes.toBytes("value");

	private static void and(byte[] left, byte[] right) {
		if (left == null || right == null)
			return;
		if (left.length != right.length)
			return;
		int length = left.length;
		for (int i = 0; i < length; i++)
			left[i] &= right[i];
	}

	private static void or(byte[] left, byte[] right) {
		if (left == null || right == null)
			return;
		if (left.length != right.length)
			return;
		int length = left.length;
		for (int i = 0; i < length; i++)
			left[i] |= right[i];
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
		NavigableSet<byte[]> cols = colMap.get(dataBytes);
		if (cols == null)
			return;
		Set<String> colsSet = new HashSet<String>();
		for (byte[] col : cols)
			colsSet.add(Bytes.toString(col));
		// common get
		if (!colsSet.contains("__MDQUERY__"))
			return;
		// return bit-index or event result
		boolean indexOnly = colsSet.contains("__INDEXONLY__");

		// get single row
		Map<String, List<Integer>> getOp = new HashMap<String, List<Integer>>();
		// scan multiple rows
		Map<String, List<Integer>> scanOp = new HashMap<String, List<Integer>>();
		Map<String, List<Integer>> op = null;
		// fomat query commands of perperties
		for (String propStr : colsSet) {
			if (propStr.startsWith("p#")) {
				String[] splits = propStr.split("#");
				if (splits.length != 4)
					throw new IOException("__MDQUERY__ PARAMETER(" + propStr + ") ERROR!");
				if ("get".equals(splits[2]))
					op = getOp;
				else
					op = scanOp;
				List<Integer> paramList = op.get(splits[1]);
				if (paramList == null) {
					paramList = new ArrayList<Integer>();
					paramList.add(Integer.valueOf(splits[3]));
					op.put(splits[1], paramList);
				} else {
					paramList.add(Integer.valueOf(splits[3]));
				}
			}
		}

		// query commands
		for (Map.Entry<String, List<Integer>> entry : getOp.entrySet()) {
			List<Integer> paramList = entry.getValue();
			Collections.sort(paramList);
		}
		for (Map.Entry<String, List<Integer>> entry : scanOp.entrySet()) {
			List<Integer> paramList = entry.getValue();
			if (paramList.size() != 2)
				throw new IOException("__MDQUERY__ PARAMETER(" + entry.getKey() + ") ERROR!");
			Collections.sort(paramList);
		}

		// region operation
		Region hregion = c.getEnvironment().getRegion();

		// scan commands
		byte[] scanResult = null;
		for (Map.Entry<String, List<Integer>> entry : scanOp.entrySet()) {
			byte[] singleScanResult = null;
			List<Integer> paramList = entry.getValue();
			byte[] start = Bytes.add(get.getRow(), Bytes.toBytes("#" + entry.getKey() + "#"),
					Bytes.toBytes(String.format("%03d", paramList.get(0))));
			byte[] end = Bytes.add(get.getRow(), Bytes.toBytes("#" + entry.getKey() + "#"),
					Bytes.toBytes(String.format("%03d", paramList.get(1))));
			Filter filter = new InclusiveStopFilter(end);
			Scan scan = new Scan();
			scan.setStartRow(start);
			scan.addColumn(dataBytes, valueBytes);
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
		for (Map.Entry<String, List<Integer>> entry : getOp.entrySet()) {
			List<Integer> paramList = entry.getValue();
			byte[] singleGet = null;
			for (int param : paramList) {
				Result paramResult = hregion.get(new Get(Bytes.add(get.getRow(),
						Bytes.toBytes("#" + entry.getKey() + "#"), Bytes.toBytes(String.format("%03d", param)))));
				if (paramResult != null && paramResult.size() > 0) {
					byte[] paramBits = Snappy
							.uncompress(CellUtil.cloneValue(paramResult.getColumnLatestCell(dataBytes, valueBytes)));
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

		if (indexOnly || scanResult == null) {
			if (scanResult == null)
				scanResult = Bytes.toBytes("");
			result.add(new KeyValue(get.getRow(), dataBytes, valueBytes, scanResult));
		} else {
			// get events
			BitArray bitArray = new BitArray(scanResult);
			Result eventsResult = hregion.get(new Get(get.getRow()));
			byte[] events = Snappy
					.uncompress(CellUtil.cloneValue(eventsResult.getColumnLatestCell(dataBytes, valueBytes)));
			int eventsCnt = Bytes
					.toInt(CellUtil.cloneValue(eventsResult.getColumnLatestCell(dataBytes, Bytes.toBytes("length"))));
			int evtLen = events.length / eventsCnt;
			List<byte[]> eventsList = new LinkedList<byte[]>();
			for (int i = 0; i < eventsCnt; i++)
				if (bitArray.get(i)) {
					byte[] evt = new byte[evtLen];
					int index = i * evtLen;
					for (int j = 0; j < evtLen; j++)
						evt[j] = events[index + j];
					eventsList.add(evt);
				}
			int finalCnt = eventsList.size();
			byte[] finalEvents = new byte[finalCnt * evtLen];
			int index = 0;
			for (byte[] evt : eventsList) {
				int indexTemp = index * evtLen;
				for (int j = 0; j < evtLen; j++)
					finalEvents[indexTemp + j] = evt[j];
				index++;
			}
			result.add(new KeyValue(get.getRow(), dataBytes, valueBytes, finalEvents));
			result.add(new KeyValue(get.getRow(), dataBytes, Bytes.toBytes("count"), Bytes.toBytes(String.valueOf(finalCnt))));
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
		List<Cell> mdInsert = put.get(dataBytes, Bytes.toBytes("__MDINSERT__"));
		if (mdInsert == null || mdInsert.size() == 0)
			return;
		String mdOp = Bytes.toString(CellUtil.cloneValue(mdInsert.get(0)));
		// append or prepend
		if ("append".equals(mdOp) || "prepend".equals(mdOp)) {
			List<Cell> valueCells = put.get(dataBytes, valueBytes);
			if (valueCells == null || valueCells.size() == 0)
				return;
			byte[] value = Snappy.uncompress(CellUtil.cloneValue(valueCells.get(0)));
			byte[] rowkey = put.getRow();
			Region hregion = c.getEnvironment().getRegion();
			Result oldValueResult = hregion.get(new Get(rowkey));
			Cell oldValueCell = oldValueResult.getColumnLatestCell(dataBytes, valueBytes);
			byte[] oldValue = Snappy.uncompress(CellUtil.cloneValue(oldValueCell));
			byte[] newValue;
			if ("append".equals(mdOp))
				newValue = Bytes.add(oldValue, value);
			else
				newValue = Bytes.add(value, oldValue);
			Put newPut = new Put(rowkey);
			newPut.addColumn(dataBytes, valueBytes, Snappy.compress(newValue));
			hregion.put(newPut);
		}
		c.bypass();
	}
}
