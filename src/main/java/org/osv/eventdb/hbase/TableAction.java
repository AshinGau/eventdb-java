package org.osv.eventdb.hbase;

import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.MetaTableAccessor;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.filter.PrefixFilter;
import org.apache.hadoop.hbase.io.encoding.DataBlockEncoding;
import org.apache.hadoop.hbase.regionserver.BloomType;
import org.apache.hadoop.hbase.regionserver.ConstantSizeRegionSplitPolicy;
import org.apache.hadoop.hbase.regionserver.HRegionFileSystem;
import org.apache.hadoop.hbase.util.Bytes;
import org.osv.eventdb.util.ConfigProperties;
//import org.apache.hadoop.hbase.io.compress.Compression;

/**
 * Integrate 'createTable', 'deleteTable', 'createRegion', 'deleteRegion', and
 * other operations for a specific eventdb table
 */
public class TableAction {
	// Hbase configuration
	private Configuration conf;
	// TableName of the specific hbase table
	private TableName tableName;
	// maxFileSize
	private long maxFileSize;
	private ConfigProperties configProp;

	/**
	 * Construct a tableAction specifiying the tableName
	 * 
	 * @param tableName the name of a hbase table
	 */
	public TableAction(String tableName) throws IOException {
		configProp = new ConfigProperties();
		init(configProp, tableName);
	}

	/**
	 * Construct a tableAction specifiying its' configuration and the tableName
	 * 
	 * @param configProp a configuration for the tableAction
	 */
	public TableAction(ConfigProperties configProp, String tableName) throws IOException {
		this.configProp = configProp;
		init(configProp, tableName);
	}

	private void init(ConfigProperties configProp, String tableName) {
		conf = HBaseConfiguration.create();
		conf.set("hbase.zookeeper.property.clientPort", configProp.getProperty("hbase.zookeeper.property.clientPort"));
		conf.set("hbase.zookeeper.quorum", configProp.getProperty("hbase.zookeeper.quorum"));
		conf.set("fs.hdfs.impl", org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
		conf.set("hbase.master", configProp.getProperty("hbase.master"));
		conf.set("zookeeper.znode.parent", configProp.getProperty("zookeeper.znode.parent"));
		this.tableName = TableName.valueOf(tableName);
		maxFileSize = Long.valueOf(configProp.getProperty("hbase.hregion.max.filesize"));
	}

	/**
	 * Create a region whose rowkey within [startKey, endKey)
	 * 
	 * @param startKey startKey(inclusive) of the region
	 * @param endKey   endKey(exclusive) of the region
	 * @throws Exception
	 */
	public void createRegion(byte[] startKey, byte[] endKey) throws Exception {
		Connection conn = ConnectionFactory.createConnection(conf);
		Admin admin = conn.getAdmin();
		// get meta table
		Table metaTable = conn.getTable(TableName.META_TABLE_NAME);
		String name = tableName.getNameAsString();
		String namespace = tableName.getNamespaceAsString();
		// hdfs directory of this region
		Path tableDir = new Path(configProp.getProperty("hbase.rootdir") + "/data/" + namespace + "/" + name);

		HRegionInfo regionInfo = new HRegionInfo(tableName, startKey, endKey);
		// create region directory on hdfs
		HRegionFileSystem.createRegionOnFileSystem(conf, FileSystem.get(new URI(configProp.getProperty("fs.defaultFS")),
				conf, configProp.getProperty("fs.user")), tableDir, regionInfo);

		// put region information to meta table
		Put put = MetaTableAccessor.makePutFromRegionInfo(regionInfo);
		metaTable.put(put);
		// enable the region
		admin.assign(regionInfo.getRegionName());

		admin.close();
		metaTable.close();
		conn.close();
	}

	/**
	 * Delete a region
	 * 
	 * @param startKey the startKey of the region
	 * @throws Exception
	 */
	public void deleteRegion(byte[] startKey) throws Exception {
		Connection conn = ConnectionFactory.createConnection(conf);
		HTable table = (HTable) conn.getTable(tableName);
		HRegionLocation location = table.getRegionLocator().getRegionLocation(startKey);
		// get region information
		HRegionInfo regionInfo = location.getRegionInfo();
		Admin admin = conn.getAdmin();
		// get meta table
		Table metaTable = conn.getTable(TableName.META_TABLE_NAME);
		String name = tableName.getNameAsString();
		String namespace = tableName.getNamespaceAsString();
		// path of its' hdfs directory
		// path of its' hdfs directory
		Path tableDir = new Path(conf.get("hbase.rootdir") + "/data/" + namespace + "/" + name);

		// delete on hdfs
		HRegionFileSystem.deleteRegionFromFileSystem(conf, FileSystem.get(conf), tableDir, regionInfo);
		byte[] regionName = regionInfo.getRegionName();
		ServerName serverName = location.getServerName();
		// close on region server
		admin.closeRegion(regionName, serverName.getServerName());
		// delete on meta table
		Delete delete = MetaTableAccessor.makeDeleteFromRegionInfo(regionInfo);
		metaTable.delete(delete);

		admin.close();
		metaTable.close();
		conn.close();
	}

	/**
	 * Create an eventdb table, and init meta information
	 * 
	 * @param initSplit initial number of splits of the eventdb table
	 * @throws IOException
	 */
	public void createTable(int initSplit) throws Exception {
		Connection conn = ConnectionFactory.createConnection(conf);
		Admin admin = conn.getAdmin();
		// get meta table
		Table metaTable = conn.getTable(TableName.META_TABLE_NAME);
		String name = tableName.getNameAsString();
		String namespace = tableName.getNamespaceAsString();
		// hdfs directory path
		Path tableDir = new Path(configProp.getProperty("hbase.rootdir") + "/data/" + namespace + "/" + name);

		if (admin.tableExists(tableName)) {
			admin.disableTable(tableName);
			admin.deleteTable(tableName);
		}
		HTableDescriptor tdesc = new HTableDescriptor(tableName);
		HColumnDescriptor cdesc = new HColumnDescriptor(Command.dataBytes);
		cdesc.setMaxVersions(1).setBlocksize(65536).setBlockCacheEnabled(true).setBloomFilterType(BloomType.ROW)
				.setDataBlockEncoding(DataBlockEncoding.PREFIX_TREE);
		// .setCompressionType(Compression.Algorithm.SNAPPY);
		// split when the biggest store file grows beyond the maxFileSize
		tdesc.setValue(HTableDescriptor.SPLIT_POLICY, ConstantSizeRegionSplitPolicy.class.getName());
		tdesc.setMaxFileSize(maxFileSize);
		tdesc.addFamily(cdesc);
		admin.createTable(tdesc);

		// create regions
		for (int startRegion = 1; startRegion <= initSplit; startRegion++) {
			byte[] startKey = Bytes.toBytes(startRegion);
			byte[] endKey = Bytes.toBytes(startRegion + 1);
			HRegionInfo regionInfo = new HRegionInfo(tableName, startKey, endKey);
			HRegionFileSystem.createRegionOnFileSystem(conf, FileSystem
					.get(new URI(configProp.getProperty("fs.defaultFS")), conf, configProp.getProperty("fs.user")),
					tableDir, regionInfo);

			Put put = MetaTableAccessor.makePutFromRegionInfo(regionInfo);
			metaTable.put(put);
			admin.assign(regionInfo.getRegionName());
		}

		// create a meta region that stores meta information
		HRegionInfo regionInfo = new HRegionInfo(tableName, Bytes.toBytes((int) 0), Bytes.toBytes((int) 1));
		HRegionFileSystem.createRegionOnFileSystem(conf, FileSystem.get(new URI(configProp.getProperty("fs.defaultFS")),
				conf, configProp.getProperty("fs.user")), tableDir, regionInfo);
		Put put = MetaTableAccessor.makePutFromRegionInfo(regionInfo);
		metaTable.put(put);
		admin.assign(regionInfo.getRegionName());

		// meta infomartion
		Table thisTable = conn.getTable(tableName);
		// initial number of splits: meta + initSplit
		Put initSplitPut = new Put(Bytes.add(Command.metaZeroBytes, Command.metaInitSplitBytes));
		initSplitPut.addColumn(Command.dataBytes, Command.valueBytes, Bytes.toBytes(initSplit));
		thisTable.put(initSplitPut);
		// current region for the consistent hash: meta + split + splitID
		List<Put> splitRegionList = new ArrayList<Put>();
		for (int i = 1; i <= initSplit; i++) {
			Put regionPut = new Put(Bytes.add(Command.metaZeroBytes, Command.metaSplitBytes, Bytes.toBytes(i)));
			regionPut.addColumn(Command.dataBytes, Command.valueBytes, Bytes.toBytes(i));
			splitRegionList.add(regionPut);
		}
		thisTable.put(splitRegionList);
		// total event regions of the table: meta + regions
		thisTable.incrementColumnValue(Bytes.add(Command.metaZeroBytes, Command.metaRegionsBytes), Command.dataBytes,
				Command.valueBytes, initSplit);

		thisTable.close();
		admin.close();
		metaTable.close();
		conn.close();
	}

	/**
	 * Delete a table
	 * 
	 * @param tableName the name of the table
	 * @throws IOException
	 */
	public void deleteTable() throws IOException {
		Connection con = ConnectionFactory.createConnection(conf);
		Admin admin = con.getAdmin();
		if (admin.tableExists(tableName)) {
			admin.disableTable(tableName);
			admin.deleteTable(tableName);
		}
		Table eventMetaTable = con.getTable(TableName.valueOf(configProp.getProperty("fits.meta.table")));
		Get delTableGet = new Get(Bytes.toBytes("table#" + tableName.getNameAsString()));
		Result delRow = eventMetaTable.get(delTableGet);
		long delEvents = Bytes
				.toLong(CellUtil.cloneValue(delRow.getColumnLatestCell(Command.dataBytes, Bytes.toBytes("events"))));
		long delFiles = Bytes
				.toLong(CellUtil.cloneValue(delRow.getColumnLatestCell(Command.dataBytes, Bytes.toBytes("files"))));

		List<Delete> dels = new LinkedList<Delete>();
		dels.add(new Delete(Bytes.toBytes("table#" + tableName.getNameAsString())));
		Scan prefixScan = new Scan();
		prefixScan.setFilter(new PrefixFilter(tableName.getName()));
		ResultScanner results = eventMetaTable.getScanner(prefixScan);
		for (Result result : results)
			dels.add(new Delete(result.getRow()));
		eventMetaTable.delete(dels);

		eventMetaTable.incrementColumnValue(Bytes.toBytes("total"), Command.dataBytes, Bytes.toBytes("events"),
				-delEvents);
		eventMetaTable.incrementColumnValue(Bytes.toBytes("total"), Command.dataBytes, Bytes.toBytes("files"),
				-delFiles);

		admin.close();
		con.close();
	}

	public void initMetaTable() throws IOException {
		Connection conn = ConnectionFactory.createConnection(conf);
		Admin admin = conn.getAdmin();
		if (admin.tableExists(tableName)) {
			throw new IOException("the meta table " + tableName.getNameAsString() + " exists already!");
		}
		HTableDescriptor tdesc = new HTableDescriptor(tableName);
		HColumnDescriptor cdesc = new HColumnDescriptor(Command.dataBytes);
		cdesc.setMaxVersions(1).setBlocksize(65536).setBlockCacheEnabled(true).setBloomFilterType(BloomType.ROW)
				.setDataBlockEncoding(DataBlockEncoding.PREFIX_TREE);
		tdesc.addFamily(cdesc);
		admin.createTable(tdesc);
		admin.close();
		conn.close();
	}
}
