package org.osv.eventdb.fits;

import java.io.IOException;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.osv.eventdb.event.PropertyValue;
import org.osv.eventdb.util.Pair;

public abstract class FitsQueryFormater {
	public List<Integer> buckets = new LinkedList<Integer>();
	public Map<Integer, List<PropertyValue>> getOp = new HashMap<Integer, List<PropertyValue>>();
	public Map<Integer, Pair<PropertyValue, PropertyValue>> scanOp = new HashMap<Integer, Pair<PropertyValue, PropertyValue>>();
	public String query;
	public double startTime;
	public double endTime;

	private static Pattern keyValuePattern = Pattern.compile("([\\w\\.]+)=([\\d\\.,~]+)");
	private static Pattern scanPattern = Pattern.compile("(\\d*\\.?\\d*)~(\\d*\\.?\\d*)");
	private static double timeBucketInterval = 60.0;

	public static void setTimeBucketInterval(double time) {
		timeBucketInterval = time;
	}

	public static double getTimeBucketInterval() {
		return timeBucketInterval;
	}

	private static int getTimeBucket(double time) {
		return (int) Math.floor(time / timeBucketInterval);
	}

	private static int getPropertyNameID(String name) {
		if ("detID".equals(name))
			return 0;
		else if ("channel".equals(name))
			return 1;
		else if ("pulse".equals(name))
			return 2;
		else if ("eventType".equals(name))
			return 3;
		else
			return -1;
	}

	protected abstract PropertyValue getPropertyValue(int property, String valueStr);

	public FitsQueryFormater(String queryString) throws IOException {
		query = queryString.replace(" ", "");
		String[] keyvalues = query.split("&");
		for (String keyvalue : keyvalues) {
			Matcher keyValueMatcher = keyValuePattern.matcher(keyvalue);
			if (!keyValueMatcher.matches()) {
				throw new IOException("QUERY STRING ERROR!");
			}
			String property = keyValueMatcher.group(1);
			String valueStr = keyValueMatcher.group(2);
			Matcher scanMatcher = scanPattern.matcher(valueStr);
			// time
			if ("time".equals(property)) {
				if (!scanMatcher.matches())
					throw new IOException("QUERY STRING TIME RANGE ERROR!");
				startTime = Double.valueOf(scanMatcher.group(1));
				endTime = Double.valueOf(scanMatcher.group(2));
				int startTimeBucket = getTimeBucket(startTime);
				int endTimeBucket = getTimeBucket(endTime);
				for (int i = startTimeBucket; i <= endTimeBucket; i++)
					buckets.add(i);
			}
			// scan
			else if (scanMatcher.matches()) {
				int propertyID = getPropertyNameID(property);
				PropertyValue start = getPropertyValue(propertyID, scanMatcher.group(1));
				PropertyValue end = getPropertyValue(propertyID, scanMatcher.group(2));
				scanOp.put(propertyID, new Pair<PropertyValue, PropertyValue>(start, end));
			}
			// get
			else {
				int propertyID = getPropertyNameID(property);
				String[] valueList = valueStr.split(",");
				List<PropertyValue> byteList = new LinkedList<PropertyValue>();
				for (String valueItem : valueList) {
					byteList.add(getPropertyValue(propertyID, valueItem));
				}
				getOp.put(propertyID, byteList);
			}
		}
		if (buckets.size() < 0)
			throw new IOException("NO TIME RANGE ERROR!");
	}
}