package org.osv.eventdb;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.junit.Test;
import org.osv.eventdb.event.PropertyValue;
import org.osv.eventdb.fits.BytePropertyValue;
import org.osv.eventdb.hbase.MDQuery;
import org.osv.eventdb.util.ConfigProperties;

public class ExperimentTest {
	@Test
	public void testFunc() throws Exception {
		List<Integer> timeBuckets = new LinkedList<Integer>();
		timeBuckets.add(2973845);
		timeBuckets.add(2973846);
		timeBuckets.add(2973847);

		Map<Integer, List<PropertyValue>> getOp = new HashMap<Integer, List<PropertyValue>>();
		int detIDInt = 0;
		List<PropertyValue> detIDValues = new LinkedList<PropertyValue>();
		detIDValues.add(new BytePropertyValue((byte) 1));
		detIDValues.add(new BytePropertyValue((byte) 2));
		getOp.put(detIDInt, detIDValues);

		MDQuery query = new MDQuery(new ConfigProperties(), "HeFits");
		query.query(timeBuckets, getOp, null);
	}
}