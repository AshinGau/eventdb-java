package org.osv.eventdb.fits;

import org.apache.hadoop.hbase.util.Bytes;
import org.osv.eventdb.event.PropertyValue;

public class ShortPropertyValue implements PropertyValue {
	private short value;

	public ShortPropertyValue(short val) {
		value = val;
	}

	public void setValue(short val) {
		value = val;
	}

	public short getValue() {
		return value;
	}

	/**
	 * Get the encoded byte array, whose lexicographic order is consistent with the
	 * numerical order.
	 */
	public byte[] getSerializedValue() {
		return Bytes.toBytes(value);
	}
}