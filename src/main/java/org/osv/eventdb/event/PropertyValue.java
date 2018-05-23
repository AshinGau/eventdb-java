package org.osv.eventdb.event;

/**
 * Encode the byte array of a property value, to ensure that the lexicographic
 * order is consistent with the numerical order.
 */
public interface PropertyValue {
	/**
	 * Get the encoded byte array, whose lexicographic order is consistent with the
	 * numerical order.
	 */
	byte[] getSerializedValue();
}