package org.osv.eventdb.fits;

import org.osv.eventdb.event.Event;
import org.osv.eventdb.event.PropertyValue;
import org.osv.eventdb.util.Pair;

/**
 * Event data object of fits file
 */
public abstract class FitsEvent implements Event, Comparable<FitsEvent> {
	protected byte[] bin;
	protected int length;
	protected double time;
	protected PropertyValue detID;
	protected PropertyValue channel;
	protected PropertyValue pulse;
	protected PropertyValue eventType;
	protected PropertyValue[] values;
	protected static int[] names = { 0, 1, 2, 3 };

	public FitsEvent() {
	}

	public FitsEvent(byte[] bin) {
		setBin(bin);
		deserialize();
	}

	public void setBin(byte[] bin) {
		this.bin = bin;
		length = bin.length;
	}

	public byte[] getBin() {
		return bin;
	}

	public int getLength() {
		return length;
	}

	public double getTime() {
		return time;
	}

	public int compareTo(FitsEvent evt) {
		Double x = getTime();
		return x.compareTo(evt.getTime());
	}

	/**
	 * Get property nameID of this event
	 */
	public int[] getPropertyNamesID() {
		return names;
	}

	/**
	 * Get values of this event
	 */
	public PropertyValue[] getPropertyValues() {
		return values;
	}

	/**
	 * Get value of the specific property
	 */
	public PropertyValue getPropertyValue(int nameID) {
		switch (nameID) {
		case 0:
			return detID;
		case 1:
			return channel;
		case 2:
			return pulse;
		case 3:
			return eventType;
		default:
			return null;

		}
	}

	/**
	 * Get property names of this event. Unsupported Currently.
	 */
	public String[] getPropertyNames() throws UnsupportedOperationException{
		throw new UnsupportedOperationException("getPropertyNames is unsupported by FitsEvent");
	}

	/**
	 * Get property family of this event. Unsupported Currently.
	 */
	public Pair<String, PropertyValue>[] getPropertyFamily() throws UnsupportedOperationException{
		throw new UnsupportedOperationException("getPropertyFamily is unsupported by FitsEvent");
	}

	/**
	 * Get property family of this event. Unsupported Currently.
	 */
	public Pair<Integer, PropertyValue>[] getPropertyFamily2() throws UnsupportedOperationException{
		throw new UnsupportedOperationException("getPropertyFamily is unsupported by FitsEvent");
	}

	/**
	 * Get value of the specific property. Unsupported Currently.
	 */
	public PropertyValue getPropertyValue(String propertyName) throws UnsupportedOperationException{
		throw new UnsupportedOperationException("getPropertyValue(String propertyName) is unsupported by FitsEvent");
	}
}