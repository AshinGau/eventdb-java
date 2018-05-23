package org.osv.eventdb.fits.mapred;

import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


public class HeFitsMapper1 extends Mapper<LongWritable, HeEvt, Text, HeEvtData>{
	@Override
	public void map(LongWritable key, HeEvt value, Context context) throws IOException, InterruptedException{
		HeEvtData evt = new HeEvtData(value.getEvt());
		double time = evt.getTime();
		byte eventType = evt.getEventType();
		byte detID = evt.getDetID();
		//rowkey scheme: eventType#detID#timeRange
		//timeRange = Math.floor(time), include data within 1s
		Text rowkey = new Text(String.format("%d#%d#%010d", eventType & 0x00ff, detID & 0x00ff, (int)Math.floor(time)));
		context.write(rowkey, evt);
	}
}
