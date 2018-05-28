package org.osv.eventdb.fits;

import java.io.IOException;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.osv.eventdb.event.PropertyValue;
import org.osv.eventdb.hbase.MDQuery;
import org.osv.eventdb.util.ConfigProperties;
import org.osv.eventdb.util.Pair;

public class FitsQueryClient {
	private static double timeBucketInterval;
	private MDQuery mdQuery;

	public FitsQueryClient(ConfigProperties configProp, String tableName) throws IOException {
		timeBucketInterval = Double.valueOf(configProp.getProperty("fits.timeBucketInterval"));
		mdQuery = new MDQuery(configProp, tableName);
	}

	public FitsQueryClient(String tableName) throws IOException {
		ConfigProperties configProp = new ConfigProperties();
		timeBucketInterval = Double.valueOf(configProp.getProperty("fits.timeBucketInterval"));
		mdQuery = new MDQuery(configProp, tableName);
	}

	public static int getPropertyNameID(String name) {
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

	public static int getTimeBucket(double time) {
		return (int) Math.floor(time / timeBucketInterval);
	}

	public FitsQueryFormat newFormater(String query) throws IOException {
		return new FitsQueryFormat(query);
	}

	public List<byte[]> query(String query) throws IOException{
		FitsQueryFormat format = newFormater(query);
		LinkedList<byte[]> result = (LinkedList<byte[]>)mdQuery.query(format.buckets, format.getOp, format.scanOp);
		List<byte[]> finalResult = new LinkedList<byte[]>();
		if(result.size() == 1){
			byte[] bytes = result.getFirst();

		}
		return null;
	}

	public static class FitsQueryFormat {
		public List<Integer> buckets = new LinkedList<Integer>();
		public Map<Integer, List<PropertyValue>> getOp = new HashMap<Integer, List<PropertyValue>>();
		public Map<Integer, Pair<PropertyValue, PropertyValue>> scanOp = new HashMap<Integer, Pair<PropertyValue, PropertyValue>>();
		public String query;
		public double startTime;
		public double endTime;

		private static Pattern keyValuePattern = Pattern.compile("([\\w\\.]+)=([\\d\\.,~]+)");
		private static Pattern scanPattern = Pattern.compile("(\\d*\\.?\\d*)~(\\d*\\.?\\d*)");

		public FitsQueryFormat(String queryString) throws IOException {
			query = queryString.trim();
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
					int startInt = Integer.valueOf(scanMatcher.group(1));
					PropertyValue start = new BytePropertyValue((byte) startInt);
					int endInt = Integer.valueOf(scanMatcher.group(2));
					PropertyValue end = new BytePropertyValue((byte) endInt);
					scanOp.put(propertyID, new Pair<PropertyValue, PropertyValue>(start, end));
				}
				// get
				else {
					int propertyID = getPropertyNameID(property);
					String[] valueList = valueStr.split(",");
					List<PropertyValue> byteList = new LinkedList<PropertyValue>();
					for (String valueItem : valueList) {
						int item = Integer.valueOf(valueItem);
						byteList.add(new BytePropertyValue((byte) item));
					}
					getOp.put(propertyID, byteList);
				}
			}
			if (buckets.size() < 0)
				throw new IOException("NO TIME RANGE ERROR!");
		}
	}
}