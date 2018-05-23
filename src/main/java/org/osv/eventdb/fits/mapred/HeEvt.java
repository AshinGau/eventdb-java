package org.osv.eventdb.fits.mapred;

import java.io.DataInput; 
import java.io.DataOutput; 
import java.io.IOException;
import org.apache.hadoop.io.Writable;

public class HeEvt implements Writable{
	private byte[] evt = new byte[16];
	public HeEvt(){}
	public void write(DataOutput out) throws IOException{
		out.write(evt);
	}
	public void readFields(DataInput in) throws IOException{
		for(int i = 0; i < 16; i++)
			evt[i] = in.readByte();
	}
	public byte[] getEvt(){
		return evt;
	}
	public void setEvt(byte[] evt){
		for(int i = 0; i < 16; i++)
			this.evt[i] = evt[i];
	}
}
