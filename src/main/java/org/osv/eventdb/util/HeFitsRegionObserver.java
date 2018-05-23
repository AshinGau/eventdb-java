package org.osv.eventdb.util;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.NavigableSet;
import java.util.Set;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.coprocessor.BaseRegionObserver;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.regionserver.Region;
import org.apache.hadoop.hbase.util.Bytes;

public class HeFitsRegionObserver extends BaseRegionObserver {
	@Override
	public void preGetOp(final ObserverContext<RegionCoprocessorEnvironment> c, final Get get, final List<Cell> result)
			throws IOException {
		Map<byte[], NavigableSet<byte[]>> colMap = get.getFamilyMap();
		if (colMap == null)
			return;
		NavigableSet<byte[]> cols = colMap.get(Bytes.toBytes("data"));
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
			String value = String.valueOf(paramList.get(0));
			for (int i = 1; i < paramList.size(); i++)
				value += "," + String.valueOf(paramList.get(i));
			result.add(new KeyValue(Bytes.toBytes("__MDQUERY__"), Bytes.toBytes("data"), Bytes.toBytes(entry.getKey()),
					Bytes.toBytes(value)));
		}
		for (Map.Entry<String, List<Integer>> entry : scanOp.entrySet()) {
			List<Integer> paramList = entry.getValue();
			if (paramList.size() != 2)
				throw new IOException("__MDQUERY__ PARAMETER(" + entry.getKey() + ") ERROR!");
			Collections.sort(paramList);
			String value = String.valueOf(paramList.get(0)) + "~" + String.valueOf(paramList.get(1));
			result.add(new KeyValue(Bytes.toBytes("__MDQUERY__"), Bytes.toBytes("data"), Bytes.toBytes(entry.getKey()),
					Bytes.toBytes(value)));
		}

		//region operation
		Region hregion = c.getEnvironment().getRegion();
		//addition test
		Get testGet = new Get(Bytes.toBytes("testRowkey"));
		Result testResult = hregion.get(testGet);
		result.add(testResult.getColumnLatestCell(Bytes.toBytes("data"), Bytes.toBytes("col1")));

		c.bypass();
	}
}
