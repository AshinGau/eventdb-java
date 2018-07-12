package org.osv.eventdb.fits;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.filter.KeyOnlyFilter;
import org.apache.hadoop.hbase.filter.PrefixFilter;
import org.apache.hadoop.hbase.util.Bytes;
import org.osv.eventdb.hbase.Command;
import org.osv.eventdb.util.ConfigProperties;

public class FitsEventDBClient {
	private ConfigProperties configProp;
	private Configuration conf;
	private Connection conn;
	protected Table eventMetaTable;

	public FitsEventDBClient() throws IOException {
		this(new ConfigProperties());
	}

	public void close() throws IOException {
		conn.close();
		eventMetaTable.close();
	}

	public FitsEventDBClient(ConfigProperties configProperties) throws IOException {
		this.configProp = configProperties;
		conf = HBaseConfiguration.create();
		conf.set("hbase.zookeeper.property.clientPort", configProp.getProperty("hbase.zookeeper.property.clientPort"));
		conf.set("hbase.zookeeper.quorum", configProp.getProperty("hbase.zookeeper.quorum"));
		conf.set("hbase.client.keyvalue.maxsize", configProp.getProperty("hbase.client.keyvalue.maxsize"));
		conf.set("hbase.master", configProp.getProperty("hbase.master"));
		conf.set("zookeeper.znode.parent", configProp.getProperty("zookeeper.znode.parent"));
		conn = ConnectionFactory.createConnection(conf);
		eventMetaTable = conn.getTable(TableName.valueOf(configProp.getProperty("fits.meta.table")));
	}

	public String[] list() throws IOException {
		Scan scan = new Scan(Bytes.toBytes("table#"), Bytes.toBytes("table|"));
		PrefixFilter tablepre = new PrefixFilter(Bytes.toBytes("table#"));
		KeyOnlyFilter keyonly = new KeyOnlyFilter();
		scan.setFilter(keyonly);
		scan.setFilter(tablepre);
		ResultScanner results = eventMetaTable.getScanner(scan);
		List<String> tables = new ArrayList<String>();
		for (Result row : results)
			tables.add(Bytes.toString(row.getRow()).substring(6));
		String[] arr = new String[tables.size()];
		for (int i = 0; i < arr.length; i++)
			arr[i] = tables.get(i);
		return arr;
	}

	public long getTotalEvents() throws IOException {
		return Bytes.toLong(eventMetaTable.get(new Get(Bytes.toBytes("total"))).getValue(Command.dataBytes,
				Bytes.toBytes("events")));
	}

	public long getTotalFiles() throws IOException {
		return Bytes.toLong(eventMetaTable.get(new Get(Bytes.toBytes("total"))).getValue(Command.dataBytes,
				Bytes.toBytes("files")));
	}

	public long getEventsOfTable(String tableName) throws IOException {
		return Bytes.toLong(eventMetaTable.get(new Get(Bytes.toBytes("table#" + tableName))).getValue(Command.dataBytes,
				Bytes.toBytes("events")));
	}

	public long getFilesOfTable(String tableName) throws IOException {
		return Bytes.toLong(eventMetaTable.get(new Get(Bytes.toBytes("table#" + tableName))).getValue(Command.dataBytes,
				Bytes.toBytes("files")));
	}

	public String[] getFileListOfTable(String tableName) throws IOException {
		Scan scan = new Scan(Bytes.toBytes(tableName + "#"), Bytes.toBytes(tableName + "|"));
		PrefixFilter tablepre = new PrefixFilter(Bytes.toBytes(tableName + "#"));
		KeyOnlyFilter keyonly = new KeyOnlyFilter();
		scan.setFilter(keyonly);
		scan.setFilter(tablepre);
		ResultScanner results = eventMetaTable.getScanner(scan);
		List<String> files = new ArrayList<String>();
		for (Result row : results) {
			String key = Bytes.toString(row.getRow());
			files.add(key.substring(tableName.length() + 1));
		}
		String[] arr = new String[files.size()];
		for (int i = 0; i < arr.length; i++)
			arr[i] = files.get(i);
		return arr;
	}

	public int[] getWriteSpeed(long startTimeStamp, long endTimeStamp) throws IOException {
		Scan scan = new Scan(Bytes.toBytes("et#" + startTimeStamp), Bytes.toBytes("et#" + endTimeStamp));
		ResultScanner results = eventMetaTable.getScanner(scan);
		int start = (int) (startTimeStamp / 1000);
		int end = (int) (endTimeStamp / 1000);
		int num = end - start + 1;
		int[] speed = new int[num];

		long eventsPre = 0;
		for (Result row : results) {
			long time = new Long(Bytes.toString(row.getRow()).substring(3));
			long events = Bytes.toLong(row.getValue(Command.dataBytes, Bytes.toBytes("events")));
			int tip = (int) ((time - startTimeStamp) / 1000);
			if (eventsPre != 0 && tip < num)
				speed[tip] += (int) (events - eventsPre);
			eventsPre = events;
		}
		return speed;
	}

	public int[] getWriteSpeed(int lastSeconds) throws IOException {
		long now = System.currentTimeMillis();
		return getWriteSpeed(now - lastSeconds * 1000, now);
	}

	public int[] getReadLatency(long startTimeStamp, long endTimeStamp) throws IOException {
		Scan scan = new Scan(Bytes.toBytes("d#" + startTimeStamp), Bytes.toBytes("d#" + endTimeStamp));
		ResultScanner results = eventMetaTable.getScanner(scan);
		int start = (int) (startTimeStamp / 1000);
		int end = (int) (endTimeStamp / 1000);
		int num = end - start + 1;
		int[] delay = new int[num];
		int[] cnt = new int[num];

		for (Result row : results) {
			long time = new Long(Bytes.toString(row.getRow()).substring(2));
			long dl = Bytes.toLong(row.getValue(Command.dataBytes, Bytes.toBytes("delay")));
			int tip = (int) ((time - startTimeStamp) / 1000);
			if (tip < num) {
				cnt[tip] += 1;
				delay[tip] += dl;
			}
		}
		for (int i = 0; i < num; i++) {
			if(cnt[i] != 0)
				delay[i] /= cnt[i];
		}

		return delay;
	}

	public int[] getReadLatency(int lastSeconds) throws IOException {
		long now = System.currentTimeMillis();
		return getReadLatency(now - lastSeconds * 1000, now);
	}
}
