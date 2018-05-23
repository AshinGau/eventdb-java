package org.osv.eventdb.fits;

import org.osv.eventdb.event.PropertyValue;

/**
 * Fits has four properties of byte type: eventType, detID, channel, pulse
 */
public class BytePropertyValue implements PropertyValue {
	private byte value;

	public BytePropertyValue(byte val) {
		value = val;
	}

	public void setValue(byte val) {
		value = val;
	}

	public byte getValue() {
		return value;
	}

	/**
	 * Get the encoded byte array, whose lexicographic order is consistent with the
	 * numerical order.
	 */
	public byte[] getSerializedValue() {
		return new byte[] { value };
	}
}