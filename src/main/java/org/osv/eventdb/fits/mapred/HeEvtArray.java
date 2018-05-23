package org.osv.eventdb.fits.mapred;

import java.io.DataInput; 
import java.io.DataOutput; 
import java.io.IOException;
import org.apache.hadoop.io.Writable;

public class HeEvtArray implements Writable{
	private int length;
	private byte[] evts;
	public HeEvtArray(){}
	public HeEvtArray(byte[] evts){
		this.length = evts.length / 16;
		this.evts = new byte[evts.length];
		for(int i = 0; i < evts.length; i++)
			this.evts[i] = evts[i];
	}
	public void write(DataOutput out) throws IOException{
		out.writeInt(length);
		out.write(evts);
	}
	public void readFields(DataInput in) throws IOException{
		length = in.readInt();
		evts = new byte[16 * length];
		for(int i = 0; i < 16 * length; i++)
			evts[i] = in.readByte();
	}
	public int getLength(){
		return length;
	}
	public byte[] getEvts(){
		return evts;
	}
}
