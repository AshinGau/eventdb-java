package org.osv.eventdb.fits.io;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.IdLock;
import org.osv.eventdb.fits.evt.Evt;
import org.osv.eventdb.hbase.TableAction;
import org.osv.eventdb.util.BitArray;
import org.osv.eventdb.util.ConfigProperties;
import org.osv.eventdb.util.ConsistentHashRouter;
import org.osv.eventdb.util.PhysicalNode;
import org.xerial.snappy.Snappy;

public abstract class Fits2Hbase<E extends Evt> implements Runnable {
	protected FitsFileSet fits;
	protected int evtLength;
	protected Configuration hconf;
	protected Connection hconn;
	protected Table htable;
	protected ConsistentHashRouter conHashRouter;
	protected Thread thread;
	protected int splits;
	protected SimpleDateFormat timeformat;
	protected long maxFileThreshold;
	protected ConfigProperties configProp;
	protected FileSystem hdfs;
	protected static byte[] dataBytes = Bytes.toBytes("data");
	protected static byte[] valueBytes = Bytes.toBytes("value");
	protected double timeBucketInterval;

	public Fits2Hbase(FitsFileSet fits, ConfigProperties configProp, String tablename, int evtLength) throws Exception {
		init(fits, configProp, tablename, evtLength);
	}

	public Fits2Hbase(FitsFileSet fits, String tablename, int evtLength) throws Exception {
		ConfigProperties configProp = new ConfigProperties();
		init(fits, configProp, tablename, evtLength);
	}

	private void init(FitsFileSet fits, ConfigProperties configProp, String tablename, int evtLength) throws Exception {
		this.evtLength = evtLength;
		this.fits = fits;
		this.configProp = configProp;
		this.timeBucketInterval = Double.valueOf(configProp.getProperty("fits.timeBucketInterval"));

		hconf = HBaseConfiguration.create();
		hconf.set("hbase.zookeeper.property.clientPort", configProp.getProperty("hbase.zookeeper.property.clientPort"));
		hconf.set("hbase.zookeeper.quorum", configProp.getProperty("hbase.zookeeper.quorum"));
		hconf.set("hbase.client.keyvalue.maxsize", configProp.getProperty("hbase.client.keyvalue.maxsize"));
		hconn = ConnectionFactory.createConnection(hconf);
		htable = hconn.getTable(TableName.valueOf(tablename));
		maxFileThreshold = Long.valueOf(configProp.getProperty("hbase.hregion.filesize.threshold"));

		splits = Bytes.toInt(CellUtil.cloneValue(
				htable.get(new Get(Bytes.toBytes("meta#initSplit"))).getColumnLatestCell(dataBytes, valueBytes)));
		List<PhysicalNode> regionPrefix = new ArrayList<PhysicalNode>();
		for (int i = 0; i < splits; i++)
			regionPrefix.add(new PhysicalNode(String.valueOf(i)));
		conHashRouter = new ConsistentHashRouter(regionPrefix, 5);

		Configuration config = new Configuration();
		config.set("fs.hdfs.impl", org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
		this.hdfs = FileSystem.get(new URI(configProp.getProperty("fs.defaultFS")), config,
				configProp.getProperty("fs.user"));

		timeformat = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");
	}

	public void close() throws IOException {
		htable.close();
		hconn.close();
	}

	public void run() {
		try {
			insertFitsFile();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public void start() {
		if (thread == null)
			thread = new Thread(this);
		thread.start();
	}

	public Thread getThread() {
		return thread;
	}

	protected abstract FitsFile<E> getFitsFile(String filename);

	protected abstract E getEvt(byte[] evtBin) throws IOException;

	public void insertFitsFile() throws Exception {
		int preBucket = 0;
		int timeBucket = 0;
		double time;
		List<E> bucketEvts = new LinkedList<E>();
		File currFile = null;
		FitsFile<E> ff = null;
		while ((currFile = fits.nextFile()) != null) {
			ff = getFitsFile(currFile.getAbsolutePath());
			for (E he : ff) {
				time = he.getTime();
				timeBucket = (int) Math.floor(time / timeBucketInterval);
				if (preBucket == timeBucket) {
					bucketEvts.add(he);
				} else {
					if (preBucket != 0) {
						// put
						put(bucketEvts, preBucket);
						System.out.printf("(%.2f%%)%s Finished to insert timeBucket: %s - %d\n",
								fits.getPercentDone() * 100.0, timeformat.format(new Date()),
								currFile.getAbsolutePath(), preBucket);
						bucketEvts.clear();
					}
					preBucket = timeBucket;
					bucketEvts.add(he);
				}
			}
			System.out.printf("(%.2f%%)%s Finished to insert fits file: %s\n", fits.getPercentDone() * 100.0,
					timeformat.format(new Date()), currFile.getAbsolutePath());
			fits.incDone();
			ff.close();
		}
		// put remain
		put(bucketEvts, timeBucket);
		bucketEvts.clear();
		System.out.printf("(%.2f%%)%s Finished to insert the remaining bucket - %d\n", fits.getPercentDone() * 100.0,
				timeformat.format(new Date()), timeBucket);
	}

	private void put(List<E> bucketEvts, int timeBucket) throws Exception {
		// bucket information
		double startTime = 0.0;
		double endTime = 0.0;

		Map<String, BitArray> bitMap = new HashMap<String, BitArray>();
		int length = bucketEvts.size();
		byte[] evtsBin = new byte[length * evtLength];
		int index = 0;
		int eventType, detID, channel, pulse, i, binIndex;
		byte[] bin = null;
		for (E evt : bucketEvts) {
			if (index == 0)
				startTime = evt.getTime();
			else if (index == length - 1)
				endTime = evt.getTime();
			bin = evt.getBin();
			binIndex = index * evtLength;
			for (i = 0; i < evtLength; i++)
				evtsBin[binIndex + i] = bin[i];

			eventType = (int) (evt.getEventType() & 0x00ff);
			detID = (int) (evt.getDetID() & 0x00ff);
			channel = (int) (evt.getChannel() & 0x00ff);
			pulse = (int) (evt.getPulseWidth() & 0x00ff);
			String eventTypeKey = String.format("%s#%03d", "eventType", eventType);
			String detIDKey = String.format("%s#%03d", "detID", detID);
			String channelKey = String.format("%s#%03d", "channel", channel);
			String pulseKey = String.format("%s#%03d", "pulse", pulse);
			String[] keys = { eventTypeKey, detIDKey, channelKey, pulseKey };
			for (String key : keys) {
				BitArray bitArray = bitMap.get(key);
				if (bitArray == null) {
					bitArray = new BitArray(length);
					bitArray.set(index);
					bitMap.put(key, bitArray);
				} else {
					bitArray.set(index);
				}
			}

			index++;
		}

		// get region prefix
		String regionIndex = conHashRouter.getNode(String.valueOf(timeBucket)).getId();
		byte[] regionPrefix = CellUtil.cloneValue(htable.get(new Get(Bytes.toBytes("meta#split#" + regionIndex)))
				.getColumnLatestCell(dataBytes, valueBytes));
		String regionPrefixStr = Bytes.toString(regionPrefix);
		// check the capacity of region
		if (beyondThreshold(regionPrefix)) {
			long regionId = Long.valueOf(regionPrefixStr);
			IdLock idLock = new IdLock();
			IdLock.Entry lockEntry = idLock.getLockEntry(regionId);
			try {
				// mount region
				String regionPrefixCurr = Bytes
						.toString(CellUtil.cloneValue(htable.get(new Get(Bytes.toBytes("meta#split#" + regionIndex)))
								.getColumnLatestCell(dataBytes, valueBytes)));
				if (regionPrefixStr.equals(regionPrefixCurr)) {
					TableAction tableAction = new TableAction(configProp, htable.getName().getNameAsString());
					int regionNum = (int) Bytes.toLong(CellUtil.cloneValue(htable
							.get(new Get(Bytes.toBytes("meta#regions"))).getColumnLatestCell(dataBytes, valueBytes)));
					String regionPrefixNew = String.format("%04d", regionNum);
					tableAction.createRegion(regionPrefixNew, String.format("%04d", regionNum + 1));
					htable.incrementColumnValue(Bytes.toBytes("meta#regions"), dataBytes, valueBytes, 1);
					System.out.printf("%s mounted a new region: %s\n", timeformat.format(new Date()), regionPrefixNew);
					// get new region prefix
					regionPrefixStr = regionPrefixNew;
				}
			} finally {
				idLock.releaseLockEntry(lockEntry);
			}
		}

		// old bucket
		String putCommand = "normal";
		Get oldBucket = new Get(Bytes.toBytes(regionPrefixStr + "#" + timeBucket));
		oldBucket.addColumn(dataBytes, Bytes.toBytes("startTime"));
		oldBucket.addColumn(dataBytes, Bytes.toBytes("endTime"));
		Result oldBucketResult = htable.get(oldBucket);
		Cell startCell = oldBucketResult.getColumnLatestCell(dataBytes, Bytes.toBytes("startTime"));
		Cell endCell = oldBucketResult.getColumnLatestCell(dataBytes, Bytes.toBytes("endTime"));
		double oldStartTime = 0.0;
		double oldEndTime = 0.0;
		if (startCell != null && endCell != null) {
			oldStartTime = Bytes.toDouble(CellUtil.cloneValue(startCell));
			oldEndTime = Bytes.toDouble(CellUtil.cloneValue(endCell));
			if (endTime <= oldStartTime) {
				endTime = oldEndTime;
				putCommand = "prepend";
			} else if (startTime >= oldEndTime) {
				startTime = oldStartTime;
				putCommand = "append";
			}
			System.out.printf("(%.2f%%)%s has to fix(%s) the timeBucket: %d\n", fits.getPercentDone() * 100.0,
					timeformat.format(new Date()), putCommand, timeBucket);
		}

		// exists old bucket
		boolean existsOldBucket = !putCommand.equals("normal");
		// put events
		Put eventsPut = new Put(Bytes.toBytes(regionPrefixStr + "#" + timeBucket));
		eventsPut.addColumn(dataBytes, valueBytes, Snappy.compress(evtsBin));
		if (existsOldBucket)
			eventsPut.addColumn(dataBytes, Bytes.toBytes("__MDINSERT__"), Bytes.toBytes(putCommand));
		htable.put(eventsPut);

		// put index
		List<Put> indexPuts = new LinkedList<Put>();
		for (Map.Entry<String, BitArray> ent : bitMap.entrySet()) {
			String rowkey = String.format("%s#%d#%s", regionPrefixStr, timeBucket, ent.getKey());
			Put indexput = new Put(Bytes.toBytes(rowkey));
			indexput.addColumn(dataBytes, valueBytes, Snappy.compress(ent.getValue().getBits()));
			if (existsOldBucket)
				indexput.addColumn(dataBytes, Bytes.toBytes("__MDINSERT__"), Bytes.toBytes(putCommand));
			indexPuts.add(indexput);
		}

		// events length && startTime && endTime
		Put lengthPut = new Put(Bytes.toBytes(regionPrefixStr + "#" + timeBucket));
		lengthPut.addColumn(dataBytes, Bytes.toBytes("length"), Bytes.toBytes(length));
		lengthPut.addColumn(dataBytes, Bytes.toBytes("startTime"), Bytes.toBytes(startTime));
		lengthPut.addColumn(dataBytes, Bytes.toBytes("endTime"), Bytes.toBytes(endTime));
		indexPuts.add(lengthPut);

		// put meta information
		Put metaPut = new Put(Bytes.toBytes("meta#" + timeBucket));
		metaPut.addColumn(dataBytes, valueBytes, Bytes.toBytes(regionPrefixStr));
		indexPuts.add(metaPut);
		htable.put(indexPuts);

		// garbage collection
		evtsBin = null;
		bitMap.clear();
	}

	private boolean beyondThreshold(byte[] startKey) throws Exception {
		HRegionLocation location = ((HTable) htable).getRegionLocator().getRegionLocation(startKey);
		TableName tableName = htable.getName();
		// get region information
		HRegionInfo regionInfo = location.getRegionInfo();
		String name = tableName.getNameAsString();
		String namespace = tableName.getNamespaceAsString();
		// path of hdfs directory
		Path regionDir = new Path(configProp.getProperty("hbase.rootdir") + "/data/" + namespace + "/" + name + "/"
				+ regionInfo.getEncodedName());

		long size = hdfs.getContentSummary(regionDir).getLength();
		double sizem = size / (double) (1024 * 1024);
		System.out.printf("region %s size: %fM\n", regionInfo.getRegionNameAsString(), sizem);

		return size > maxFileThreshold;
	}
}
