package org.osv.eventdb.fits;

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
import org.osv.eventdb.event.PropertyValue;
import org.osv.eventdb.hbase.Command;
import org.osv.eventdb.hbase.TableAction;
import org.osv.eventdb.util.BitArray;
import org.osv.eventdb.util.ConfigProperties;
import org.osv.eventdb.util.ConsistentHashRouter;
import org.osv.eventdb.util.PhysicalNode;
import org.xerial.snappy.Snappy;

public abstract class Fits2Hbase implements Runnable {
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

		splits = Bytes.toInt(
				CellUtil.cloneValue(htable.get(new Get(Bytes.add(Command.metaZeroBytes, Command.metaInitSplitBytes)))
						.getColumnLatestCell(Command.dataBytes, Command.valueBytes)));
		List<PhysicalNode> regionPrefix = new ArrayList<PhysicalNode>();
		for (int i = 1; i <= splits; i++)
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

	protected abstract FitsFile getFitsFile(String filename);

	protected abstract FitsEvent getEvt(byte[] evtBin) throws IOException;

	public void insertFitsFile() throws Exception {
		int preBucket = 0;
		int timeBucket = 0;
		List<FitsEvent> bucketEvts = new LinkedList<FitsEvent>();
		File currFile = null;
		FitsFile ff = null;
		while ((currFile = fits.nextFile()) != null) {
			ff = getFitsFile(currFile.getAbsolutePath());
			for (FitsEvent he : ff) {
				timeBucket = he.getBucketID();
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

	private void put(List<FitsEvent> bucketEvts, int timeBucket) throws Exception {
		// bucket information
		double startTime = 0.0;
		double endTime = 0.0;
		byte[] timeBucketBytes = Bytes.toBytes(timeBucket);

		Map<String, BitArray> bitMap = new HashMap<String, BitArray>();
		int length = bucketEvts.size();
		byte[] evtsBin = new byte[length * evtLength];
		int index = 0;
		int i, binIndex;
		byte[] bin = null;
		for (FitsEvent evt : bucketEvts) {
			if (index == 0)
				startTime = evt.getTime();
			else if (index == length - 1)
				endTime = evt.getTime();
			bin = evt.getBin();
			binIndex = index * evtLength;
			for (i = 0; i < evtLength; i++)
				evtsBin[binIndex + i] = bin[i];

			int[] nameID = evt.getPropertyNamesID();
			PropertyValue[] propertyValues = evt.getPropertyValues();
			String[] keys = new String[nameID.length];
			for (i = 0; i < nameID.length; i++)
				keys[i] = Bytes.toString(Bytes.add(Bytes.toBytes(nameID[i]), propertyValues[i].getSerializedValue()));

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
		byte[] regionPrefix = CellUtil.cloneValue(htable
				.get(new Get(Bytes.add(Command.metaZeroBytes, Command.metaSplitBytes,
						Bytes.toBytes(Integer.valueOf(regionIndex)))))
				.getColumnLatestCell(Command.dataBytes, Command.valueBytes));
		int regionPrefixInt = Bytes.toInt(regionPrefix);
		// check the capacity of region
		if (beyondThreshold(regionPrefix)) {
			long regionId = (long) Bytes.toInt(regionPrefix);
			IdLock idLock = new IdLock();
			IdLock.Entry lockEntry = idLock.getLockEntry(regionId);
			try {
				// mount region
				int regionPrefixCurr = Bytes.toInt(CellUtil.cloneValue(htable
						.get(new Get(Bytes.add(Command.metaZeroBytes, Command.metaSplitBytes,
								Bytes.toBytes(Integer.valueOf(regionIndex)))))
						.getColumnLatestCell(Command.dataBytes, Command.valueBytes)));
				if (regionPrefixInt == regionPrefixCurr) {
					TableAction tableAction = new TableAction(configProp, htable.getName().getNameAsString());
					int regionPrefixNew = (int) htable.incrementColumnValue(
							Bytes.add(Command.metaZeroBytes, Command.metaRegionsBytes), Command.dataBytes,
							Command.valueBytes, 1);
					tableAction.createRegion(Bytes.toBytes(regionPrefixNew),
							Bytes.toBytes((int) (regionPrefixNew + 1)));
					System.out.printf("%s mounted a new region: %s\n", timeformat.format(new Date()), regionPrefixNew);
					// get new region prefix
					regionPrefixInt = regionPrefixNew;
					regionPrefix = Bytes.toBytes(regionPrefixInt);
				}
			} finally {
				idLock.releaseLockEntry(lockEntry);
			}
		}

		// old bucket
		int putCommand = -1;
		Get oldBucket = new Get(Bytes.add(regionPrefix, timeBucketBytes));
		oldBucket.addColumn(Command.dataBytes, Bytes.toBytes("startTime"));
		oldBucket.addColumn(Command.dataBytes, Bytes.toBytes("endTime"));
		Result oldBucketResult = htable.get(oldBucket);
		Cell startCell = oldBucketResult.getColumnLatestCell(Command.dataBytes, Bytes.toBytes("startTime"));
		Cell endCell = oldBucketResult.getColumnLatestCell(Command.dataBytes, Bytes.toBytes("endTime"));
		double oldStartTime = 0.0;
		double oldEndTime = 0.0;
		if (startCell != null && endCell != null) {
			oldStartTime = Bytes.toDouble(CellUtil.cloneValue(startCell));
			oldEndTime = Bytes.toDouble(CellUtil.cloneValue(endCell));
			if (endTime <= oldStartTime) {
				endTime = oldEndTime;
				putCommand = Command.PREPEND.ordinal();
			} else if (startTime >= oldEndTime) {
				startTime = oldStartTime;
				putCommand = Command.APPEND.ordinal();
			}
			System.out.printf("(%.2f%%)%s has to fix(%s) the timeBucket: %d\n", fits.getPercentDone() * 100.0,
					timeformat.format(new Date()), putCommand, timeBucket);
		}

		// exists old bucket
		boolean existsOldBucket = (putCommand != -1);
		// put events
		Put eventsPut = new Put(Bytes.add(regionPrefix, timeBucketBytes));
		eventsPut.addColumn(Command.dataBytes, Command.valueBytes, Snappy.compress(evtsBin));
		if (existsOldBucket)
			eventsPut.addColumn(Command.dataBytes, Command.mdInsertBytes, Bytes.toBytes(putCommand));
		htable.put(eventsPut);

		// put index
		List<Put> indexPuts = new LinkedList<Put>();
		for (Map.Entry<String, BitArray> ent : bitMap.entrySet()) {
			Put indexput = new Put(Bytes.add(regionPrefix, timeBucketBytes, Bytes.toBytes(ent.getKey())));
			indexput.addColumn(Command.dataBytes, Command.valueBytes, Snappy.compress(ent.getValue().getBits()));
			if (existsOldBucket)
				indexput.addColumn(Command.dataBytes, Command.mdInsertBytes, Bytes.toBytes(putCommand));
			indexPuts.add(indexput);
		}

		// events length && startTime && endTime
		Put lengthPut = new Put(Bytes.add(regionPrefix, timeBucketBytes));
		lengthPut.addColumn(Command.dataBytes, Command.countBytes, Bytes.toBytes(length));
		lengthPut.addColumn(Command.dataBytes, Bytes.toBytes("startTime"), Bytes.toBytes(startTime));
		lengthPut.addColumn(Command.dataBytes, Bytes.toBytes("endTime"), Bytes.toBytes(endTime));
		indexPuts.add(lengthPut);

		// put meta information
		Put metaPut = new Put(Bytes.add(Command.metaZeroBytes, Command.metaBucketBytes, timeBucketBytes));
		metaPut.addColumn(Command.dataBytes, Command.valueBytes, regionPrefix);
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
