package org.osv.eventdb.hbase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
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
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
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

	/**
	 * Construct a tableAction specifiying the tableName
	 * 
	 * @param tableName the name of a hbase table
	 */
	public TableAction(String tableName) throws IOException {
		ConfigProperties configProp = new ConfigProperties();
		init(configProp, tableName);
	}

	/**
	 * Construct a tableAction specifiying its' configuration and the tableName
	 * 
	 * @param configProp a configuration for the tableAction
	 */
	public TableAction(ConfigProperties configProp, String tableName) throws IOException {
		init(configProp, tableName);
	}

	/**
	 * Construct a tableAction specifiying its' HbaseConfiguration and the tableName
	 * 
	 * @param conf a HbaseConfiguration for the tableAction
	 */
	public TableAction(Configuration conf, String tableName) {
		this.conf = conf;
		this.tableName = TableName.valueOf(tableName);
		maxFileSize = conf.getLong("hbase.hregion.max.filesize", 10737418240L);
	}

	private void init(ConfigProperties configProp, String tableName) {
		conf = HBaseConfiguration.create();
		conf.set("hbase.zookeeper.property.clientPort", configProp.getProperty("hbase.zookeeper.property.clientPort"));
		conf.set("hbase.zookeeper.quorum", configProp.getProperty("hbase.zookeeper.quorum"));
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
	public void createRegion(String startKey, String endKey) throws Exception {
		Connection conn = ConnectionFactory.createConnection(conf);
		Admin admin = conn.getAdmin();
		// get meta table
		Table metaTable = conn.getTable(TableName.META_TABLE_NAME);
		String name = tableName.getNameAsString();
		String namespace = tableName.getNamespaceAsString();
		// hdfs directory of this region
		Path tableDir = new Path(conf.get("root.dir") + "/data/" + namespace + "/" + name);

		HRegionInfo regionInfo = new HRegionInfo(tableName, Bytes.toBytes(startKey), Bytes.toBytes(endKey));
		// create region directory on hdfs
		HRegionFileSystem.createRegionOnFileSystem(conf, FileSystem.get(conf), tableDir, regionInfo);

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
	public void deleteRegion(String startKey) throws Exception {
		Connection conn = ConnectionFactory.createConnection(conf);
		HTable table = (HTable) conn.getTable(tableName);
		HRegionLocation location = table.getRegionLocator().getRegionLocation(Bytes.toBytes(startKey));
		// get region information
		HRegionInfo regionInfo = location.getRegionInfo();
		Admin admin = conn.getAdmin();
		// get meta table
		Table metaTable = conn.getTable(TableName.META_TABLE_NAME);
		String name = tableName.getNameAsString();
		String namespace = tableName.getNamespaceAsString();
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
	public void createTable(int initSplit) throws IOException {
		if (initSplit < 1 || initSplit > 9999)
			throw new IOException("the initial number of regions should be in 1~9999");

		Connection conn = ConnectionFactory.createConnection(conf);
		Admin admin = conn.getAdmin();
		// get meta table
		Table metaTable = conn.getTable(TableName.META_TABLE_NAME);
		String name = tableName.getNameAsString();
		String namespace = tableName.getNamespaceAsString();
		// hdfs directory path
		Path tableDir = new Path(conf.get("root.dir") + "/data/" + namespace + "/" + name);

		if (admin.tableExists(tableName)) {
			admin.disableTable(tableName);
			admin.deleteTable(tableName);
		}
		HTableDescriptor tdesc = new HTableDescriptor(tableName);
		HColumnDescriptor cdesc = new HColumnDescriptor(Bytes.toBytes("data"));
		cdesc.setMaxVersions(1).setBlocksize(65536).setBlockCacheEnabled(true).setBloomFilterType(BloomType.ROW)
				.setTimeToLive(259200).setDataBlockEncoding(DataBlockEncoding.PREFIX_TREE);
		// .setCompressionType(Compression.Algorithm.SNAPPY);
		// split when the biggest store file grows beyond the maxFileSize
		tdesc.setValue(HTableDescriptor.SPLIT_POLICY, ConstantSizeRegionSplitPolicy.class.getName());
		tdesc.setMaxFileSize(maxFileSize);
		tdesc.addFamily(cdesc);
		admin.createTable(tdesc);

		// create regions
		for (int startRegion = 0; startRegion < initSplit; startRegion++) {
			byte[] startKey = Bytes.toBytes(String.format("%04d", startRegion));
			byte[] endKey = Bytes.toBytes(String.format("%04d", startRegion + 1));
			HRegionInfo regionInfo = new HRegionInfo(tableName, startKey, endKey);
			HRegionFileSystem.createRegionOnFileSystem(conf, FileSystem.get(conf), tableDir, regionInfo);

			Put put = MetaTableAccessor.makePutFromRegionInfo(regionInfo);
			metaTable.put(put);
			admin.assign(regionInfo.getRegionName());
		}

		// create a meta region that stores meta information
		HRegionInfo regionInfo = new HRegionInfo(tableName, Bytes.toBytes("meta"), Bytes.toBytes("meta|"));
		HRegionFileSystem.createRegionOnFileSystem(conf, FileSystem.get(conf), tableDir, regionInfo);
		Put put = MetaTableAccessor.makePutFromRegionInfo(regionInfo);
		metaTable.put(put);
		admin.assign(regionInfo.getRegionName());

		// meta infomartion
		Table thisTable = conn.getTable(tableName);
		// initial number of splits
		Put initSplitPut = new Put(Bytes.toBytes("meta#initSplit"));
		initSplitPut.addColumn(Bytes.toBytes("data"), Bytes.toBytes("value"), Bytes.toBytes(initSplit));
		thisTable.put(initSplitPut);
		// current region for the consistent hash
		List<Put> splitRegionList = new ArrayList<Put>();
		for (int i = 0; i < initSplit; i++) {
			Put regionPut = new Put(Bytes.toBytes("meta#split#" + i));
			regionPut.addColumn(Bytes.toBytes("data"), Bytes.toBytes("value"), Bytes.toBytes(String.format("%04d", i)));
			splitRegionList.add(regionPut);
		}
		thisTable.put(splitRegionList);
		// total regions of the table
		thisTable.incrementColumnValue(Bytes.toBytes("meta#regions"), Bytes.toBytes("data"), Bytes.toBytes("value"),
				initSplit);

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
	public void deleteTable(String tableName) throws IOException {
		Connection con = ConnectionFactory.createConnection(conf);
		Admin admin = con.getAdmin();
		TableName tname = TableName.valueOf(tableName);
		if (admin.tableExists(tname)) {
			admin.disableTable(tname);
			admin.deleteTable(tname);
		}
		admin.close();
		con.close();
	}
}
