package org.osv.eventdb.fits;

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
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.filter.KeyOnlyFilter;
import org.apache.hadoop.hbase.filter.PrefixFilter;
import org.apache.hadoop.hbase.util.Bytes;
import org.osv.eventdb.event.PropertyValue;
import org.osv.eventdb.util.ConfigProperties;
import org.osv.eventdb.util.Pair;
import org.xerial.snappy.Snappy;

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

	public List<String> list() throws IOException{
		Scan scan = new Scan();
		PrefixFilter tablepre = new PrefixFilter(Bytes.toBytes("table#"));
		KeyOnlyFilter keyonly = new KeyOnlyFilter();
		scan.setFilter(tablepre).setFilter(keyonly);
		ResultScanner results =  eventMetaTable.getScanner(scan);
		return null;
	}


}