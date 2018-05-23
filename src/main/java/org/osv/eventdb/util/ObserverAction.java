package org.osv.eventdb.util;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.HBaseAdmin;

public class ObserverAction {
	private Configuration hconf;
	private String table;

	public ObserverAction(String table) throws IOException {
		hconf = HBaseConfiguration.create();
		ConfigProperties configProp = new ConfigProperties();
		hconf.set("hbase.zookeeper.property.clientPort", configProp.getProperty("hbase.zookeeper.property.clientPort"));
		hconf.set("hbase.zookeeper.quorum", configProp.getProperty("hbase.zookeeper.quorum"));
		this.table = table;
	}

	public ObserverAction(Configuration config, String table) {
		hconf = config;
		this.table = table;
	}

	public void addCoprocessor(String coprocessorClass, String jarPath) throws IOException {
		Connection conn = ConnectionFactory.createConnection(hconf);
		HBaseAdmin admin = (HBaseAdmin) conn.getAdmin();
		HTableDescriptor tableDesc = admin.getTableDescriptor(TableName.valueOf(table));
		admin.disableTable(TableName.valueOf(table));
		if (tableDesc.hasCoprocessor(coprocessorClass))
			tableDesc.removeCoprocessor(coprocessorClass);
		tableDesc.addCoprocessor(coprocessorClass, new Path(jarPath), 1001, null);
		admin.modifyTable(TableName.valueOf(table), tableDesc);
		admin.enableTable(TableName.valueOf(table));
		System.out.printf("coprocessor %s is successfully mounted.\n", coprocessorClass);
		conn.close();
	}

	public void removeCoprocessor(String coprocessorClass) throws IOException {
		Connection conn = ConnectionFactory.createConnection(hconf);
		HBaseAdmin admin = (HBaseAdmin) conn.getAdmin();
		HTableDescriptor tableDesc = admin.getTableDescriptor(TableName.valueOf(table));
		admin.disableTable(TableName.valueOf(table));
		tableDesc.removeCoprocessor(coprocessorClass);
		admin.modifyTable(TableName.valueOf(table), tableDesc);
		admin.enableTable(TableName.valueOf(table));
		System.out.printf("coprocessor %s is successfully removed.\n", coprocessorClass);
		conn.close();
	}
}