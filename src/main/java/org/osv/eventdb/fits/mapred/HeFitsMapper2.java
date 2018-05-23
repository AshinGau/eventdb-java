package org.osv.eventdb.fits.mapred;

import java.io.IOException;
import java.util.*; 
import org.apache.hadoop.io.Text; 
import org.apache.hadoop.mapreduce.Mapper;  
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
//import org.davidmoten.hilbert.*;

public class HeFitsMapper2 extends Mapper<Text, HeEvtArray, ImmutableBytesWritable, Put>{
	@Override
	public void map(Text key, HeEvtArray value, Context context) throws IOException, InterruptedException{
		int length = value.getLength();
		byte[] evts = value.getEvts();
		HashMap<String, ArrayList<HeEvtData>> colMap = new HashMap<String, ArrayList<HeEvtData>>();
		for(int i = 0; i < length; i++){
			byte[] evtBytes = new byte[16];
			int index = i * 16;
			for(int j = 0; j < 16; j++)
				evtBytes[j] = evts[index + j];
			HeEvtData evt = new HeEvtData(evtBytes);
			int channel = (int)(evt.getChannel() & 0x00ff);
			int pulse = (int)(evt.getPulseWidth() & 0x00ff);
			String col = String.format("%03d#%03d", channel, pulse);
			ArrayList<HeEvtData> colarr = colMap.get(col);
			if(colarr == null){
				colarr = new ArrayList<HeEvtData>();
				colarr.add(evt);
				colMap.put(col, colarr);
			}else{
				colarr.add(evt);
			}
		}

		Put put = new Put(key.getBytes());
		byte[] family = Bytes.toBytes("data");
		
		for(Map.Entry<String, ArrayList<HeEvtData>> ent: colMap.entrySet()){
			String col = ent.getKey();
			ArrayList<HeEvtData> colarr = ent.getValue();
			byte[] colName = Bytes.toBytes(col);
			int size = colarr.size();
			byte[] coldata = new byte[16 * size];
			for(int i  = 0; i < size; i++){
				byte[] eb = colarr.get(i).getEvt();
				int index = i * 16;
				for(int j = 0; j < 16; j++)
					coldata[index + j] = eb[j];
			}
			put.addColumn(family, colName, coldata);
		}
		ImmutableBytesWritable hkey = new ImmutableBytesWritable(key.getBytes());

		context.write(hkey, put);
	}
}
