package org.osv.eventdb.fits.mapred;

import java.util.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.filter.*;

public class HeQuery{
	public static List<byte[]> query(String tableName,
			double start, double stop, 
			int detector[],
			int channelBegin, int channelEnd,
			int pulseWidthBegin, int pulseWidthEnd,
			int eventType[]) throws Exception{
		String sstart = String.format("%09d", (int)Math.floor(start / 10.0));
		String sstop = String.format("%09d", (int)Math.floor(stop / 10.0));
		byte[] family = Bytes.toBytes("data");
		int qualifierLen = (channelEnd - channelBegin + 1) * (pulseWidthEnd - pulseWidthBegin + 1);
		byte[][] qualifier = new byte[qualifierLen][];
		int qualifierIndex = 0;
		for(int channel = channelBegin; channel <= channelEnd; channel++)
			for(int pulse = pulseWidthBegin; pulse <= pulseWidthEnd; pulse++)
				qualifier[qualifierIndex++] = Bytes.toBytes(String.format("%03d#%03d", channel, pulse));

		List<byte[]> ans = new ArrayList<byte[]>();
		Configuration conf = HBaseConfiguration.create();
		conf.set("hbase.zookeeper.property.clientPort", "2181");
		conf.set("hbase.zookeeper.quorum", "sbd03,sbd05.ihep.ac.cn,sbd07.ihep.ac.cn");
		Connection connection = ConnectionFactory.createConnection(conf);
		Table hTable = connection.getTable(TableName.valueOf(tableName));

		int eventCount = 0;
		for(int eType: eventType)
			for(int det: detector){
				String rowkeyStart = String.format("%d#%d#%s", eType, det, sstart);
				String rowkeyStop = String.format("%d#%d#%s", eType, det, sstop);
				Scan fitsScan = new Scan();
				for(byte[] qlf: qualifier)
					fitsScan.addColumn(family, qlf);
				Filter inStopFilter = new InclusiveStopFilter(Bytes.toBytes(rowkeyStop));
				fitsScan.setStartRow(Bytes.toBytes(rowkeyStart));
				fitsScan.setFilter(inStopFilter);
				ResultScanner scanner = hTable.getScanner(fitsScan);
				for(Result result: scanner){
					eventCount += qualifierLen;
					System.out.println(result);
				}
				scanner.close();
			}
		System.out.printf("事例数: %d\n", eventCount);
		return ans;
	}

}
