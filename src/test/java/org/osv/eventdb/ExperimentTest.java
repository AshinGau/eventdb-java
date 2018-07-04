package org.osv.eventdb;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.junit.Test;
import org.osv.eventdb.util.ConfigProperties;

public class ExperimentTest {
	@Test
	public void testFunc() throws Exception {
		// List<byte[]> result = client.query("time = 178430700.79 ~ 178430820.82 &
		// detID = 1, 2, 3 & channel = 23 ~ 24 & pulse = 35 ~ 64");
		ConfigProperties configProp = new ConfigProperties();
		Configuration conf = HBaseConfiguration.create();
		conf.set("hbase.zookeeper.property.clientPort", configProp.getProperty("hbase.zookeeper.property.clientPort"));
		conf.set("hbase.zookeeper.quorum", configProp.getProperty("hbase.zookeeper.quorum"));
		conf.set("fs.hdfs.impl", org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
		conf.set("hbase.master", configProp.getProperty("hbase.master"));
		conf.set("zookeeper.znode.parent", configProp.getProperty("zookeeper.znode.parent"));

		Connection connection = null;
		Admin admin = null;

		try {
			connection = ConnectionFactory.createConnection(conf);
			admin = connection.getAdmin();

			String tableName = "testTable";

			if (!admin.isTableAvailable(TableName.valueOf(tableName))) {
				HTableDescriptor hbaseTable = new HTableDescriptor(TableName.valueOf(tableName));
				hbaseTable.addFamily(new HColumnDescriptor("value"));
				admin.createTable(hbaseTable);
			}
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			try {
				if (admin != null) {
					admin.close();
				}

				if (connection != null && !connection.isClosed()) {
					connection.close();
				}
			} catch (Exception e2) {
				e2.printStackTrace();
			}
		}
	}
}