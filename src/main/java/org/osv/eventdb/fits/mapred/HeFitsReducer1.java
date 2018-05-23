package org.osv.eventdb.fits.mapred;

import java.io.IOException;
import java.util.ArrayList;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
//import org.davidmoten.hilbert.*;

public class HeFitsReducer1 extends Reducer<Text, HeEvtData, Text, HeEvtArray>{
	@Override
	public void reduce(Text key, Iterable<HeEvtData> values, Context context) throws IOException, InterruptedException{
		ArrayList<HeEvt> arr = new ArrayList<HeEvt>();
		for(HeEvtData evt: values){
			HeEvt evtTmp = new HeEvt();
			evtTmp.setEvt(evt.getEvt());
			arr.add(evtTmp);
		}
		int length = arr.size();
		byte[] evts = new byte[length * 16];
		for(int i = 0; i < length; i++){
			byte[] evtBytes = arr.get(i).getEvt();
			int index = i * 16;
			for(int j = 0; j < 16; j++)
				evts[index + j] = evtBytes[j];
		}
		HeEvtArray fitsarr = new HeEvtArray(evts);
		context.write(key, fitsarr);
	}
}
