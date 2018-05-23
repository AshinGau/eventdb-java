package org.osv.eventdb.fits.evt;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

/**
 * Used to deserialize fits event.
 */
public abstract class Evt implements WritableComparable<Evt> {
	protected byte[] bin; // Serialized byte stream of fits event
	protected int length; // The length of bin
	protected double time;
	protected byte detID;
	protected byte channel;
	protected byte pulseWidth;
	protected byte eventType;

	public Evt() {
	}

	/**
	 * Creates an object by deserializing byte stream.
	 * 
	 * @param bin Serialized byte stream.
	 * @throws IOException If failed to deserialize.
	 */
	public Evt(byte[] bin) throws IOException {
		setBin(bin);
		deserialize();
	}

	/**
	 * The method to deserialize byte stream and implemented by subclass.
	 * 
	 * @throws IOException If failed to deserialize;
	 */
	public abstract void deserialize() throws IOException;

	/**
	 * Get serialized byte stream.
	 */
	public byte[] getBin() {
		return bin;
	}

	/**
	 * Set serialized byte stream.
	 * 
	 * @param bin Serialized byte stream.
	 */
	public void setBin(byte[] bin) {
		length = bin.length;
		this.bin = new byte[length];
		for (int i = 0; i < length; i++)
			this.bin[i] = bin[i];
	}

	public int getLength() {
		return length;
	}

	public double getTime() {
		return time;
	}

	public byte getDetID() {
		return detID;
	}

	public byte getChannel() {
		return channel;
	}

	public byte getPulseWidth() {
		return pulseWidth;
	}

	public byte getEventType() {
		return eventType;
	}

	public void write(DataOutput out) throws IOException {
		out.writeInt(length);
		out.write(bin);
	}

	public void readFields(DataInput in) throws IOException {
		length = in.readInt();
		bin = new byte[length];
		for (int i = 0; i < length; i++)
			bin[i] = in.readByte();
	}

	public int compareTo(Evt evt) {
		Double x = getTime();
		return x.compareTo(evt.getTime());
	}
}
