package org.osv.eventdb.fits.mapred;

import java.io.DataInput; 
import java.io.DataOutput; 
import java.io.IOException;
import org.apache.hadoop.io.WritableComparable;

import nom.tam.util.*;
import java.io.ByteArrayInputStream;

public class HeEvtData implements WritableComparable<HeEvtData>{
	private byte[] evt = new byte[16];
	private double time;
	private byte detID;
	private byte channel;
	private byte pulseWidth;
	private byte eventType;
	public HeEvtData(){}
	public HeEvtData(byte[] evt) throws IOException{
		setEvt(evt);
		initialize();
	}
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
	public double getTime(){
		return time;
	}
	public byte getDetID(){
		return detID;
	}
	public byte getChannel(){
		return channel;
	}
	public byte getPulseWidth(){
		return pulseWidth;
	}
	public byte getEventType(){
		return eventType;
	}
	public void initialize() throws IOException{
		BufferedDataInputStream evtParser = new BufferedDataInputStream(
				new ByteArrayInputStream(getEvt()));
		time = evtParser.readDouble();
		detID = evtParser.readByte();
		channel = evtParser.readByte();
		pulseWidth = evtParser.readByte();
		for(int i = 0; i < 3; i++)
			evtParser.readByte();
		eventType = evtParser.readByte();
		evtParser.readByte();
		evtParser.close();
	}
	public int compareTo(HeEvtData evtData){
		Double x = getTime();
		return x.compareTo(evtData.getTime());
	}
}
