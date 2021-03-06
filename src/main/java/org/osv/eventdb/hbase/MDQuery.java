package org.osv.eventdb.hbase;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.osv.eventdb.event.PropertyValue;
import org.osv.eventdb.util.ConfigProperties;
import org.osv.eventdb.util.Pair;
import org.xerial.snappy.Snappy;

public class MDQuery {
	private ConfigProperties configProp;
	private Configuration conf;
	private Connection conn;
	private Table table;
	protected Table eventMetaTable;

	public MDQuery(String tableName) throws IOException {
		this(new ConfigProperties(), tableName);
	}

	public void close() throws IOException {
		conn.close();
		table.close();
		eventMetaTable.close();
	}

	public MDQuery(ConfigProperties configProperties, String tableName) throws IOException {
		this.configProp = configProperties;
		conf = HBaseConfiguration.create();
		conf.set("hbase.zookeeper.property.clientPort", configProp.getProperty("hbase.zookeeper.property.clientPort"));
		conf.set("hbase.zookeeper.quorum", configProp.getProperty("hbase.zookeeper.quorum"));
		conf.set("hbase.client.keyvalue.maxsize", configProp.getProperty("hbase.client.keyvalue.maxsize"));
		conf.set("hbase.master", configProp.getProperty("hbase.master"));
		conf.set("zookeeper.znode.parent", configProp.getProperty("zookeeper.znode.parent"));
		conn = ConnectionFactory.createConnection(conf);
		table = conn.getTable(TableName.valueOf(Bytes.toBytes(tableName)));
		eventMetaTable = conn.getTable(TableName.valueOf(configProp.getProperty("fits.meta.table")));
	}

	public List<byte[]> query(List<Integer> buckets, Map<Integer, List<PropertyValue>> getOp,
			Map<Integer, Pair<PropertyValue, PropertyValue>> scanOp) throws IOException {
		long startTime = System.currentTimeMillis();

		List<Get> regionPrefixGets = new LinkedList<Get>();
		for (int bucketID : buckets)
			regionPrefixGets
					.add(new Get(Bytes.add(Command.metaZeroBytes, Command.metaBucketBytes, Bytes.toBytes(bucketID))));
		Result[] regionPrefixResults = table.get(regionPrefixGets);
		List<byte[]> regionPrefixs = new LinkedList<byte[]>();
		int index = 0;
		for (Result regionPrefixResult : regionPrefixResults) {
			Cell cell = regionPrefixResult.getColumnLatestCell(Command.dataBytes, Command.valueBytes);
			if (cell != null)
				regionPrefixs.add(Bytes.add(CellUtil.cloneValue(cell), Bytes.toBytes(buckets.get(index))));
			index++;
		}

		List<Get> gets = new LinkedList<Get>();
		for (byte[] reginoPrefix : regionPrefixs) {
			Get get = new Get(reginoPrefix);
			if ((getOp != null && getOp.size() > 0) || (scanOp != null && scanOp.size() > 0))
				get.addColumn(Command.dataBytes, Command.mdQueryBytes);

			if (getOp != null && getOp.size() > 0)
				for (Map.Entry<Integer, List<PropertyValue>> entry : getOp.entrySet()) {
					byte[] property = Bytes.toBytes(entry.getKey());
					List<PropertyValue> values = entry.getValue();
					for (PropertyValue value : values)
						get.addColumn(Command.dataBytes,
								Bytes.add(Command.getRowBytes, property, value.getSerializedValue()));
				}

			if (scanOp != null && scanOp.size() > 0)
				for (Map.Entry<Integer, Pair<PropertyValue, PropertyValue>> entry : scanOp.entrySet()) {
					byte[] property = Bytes.toBytes(entry.getKey());
					Pair<PropertyValue, PropertyValue> pair = entry.getValue();
					get.addColumn(Command.dataBytes,
							Bytes.add(Command.scanStartBytes, property, pair.getFirst().getSerializedValue()));
					get.addColumn(Command.dataBytes,
							Bytes.add(Command.scanEndBytes, property, pair.getSecond().getSerializedValue()));
				}

			gets.add(get);
		}

		Result[] eventResults = table.get(gets);
		List<byte[]> result = new LinkedList<byte[]>();
		for (Result eventResult : eventResults) {
			Cell eventCell = eventResult.getColumnLatestCell(Command.dataBytes, Command.valueBytes);
			if (eventCell != null)
				result.add(Snappy.uncompress(CellUtil.cloneValue(eventCell)));

		}

		long endTime = System.currentTimeMillis();
		eventMetaTable.put(new Put(Bytes.toBytes("d#" + endTime)).addColumn(Command.dataBytes, Bytes.toBytes("delay"),
				Bytes.toBytes(endTime - startTime)));

		return result;
	}
}