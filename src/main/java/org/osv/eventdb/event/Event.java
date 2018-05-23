package org.osv.eventdb.event;

import org.osv.eventdb.util.Pair;

/**
 * Data object inserted into eventdb should implement this interface.
 */
public interface Event {
	/**
	 * Get byte array of this event
	 */
	byte[] getBin();

	/**
	 * Set byte array of this event
	 * 
	 * @param bin Byte array
	 */
	void setBin(byte[] bin);

	/**
	 * Method to deserialize byte array
	 */
	void deserialize();

	/**
	 * Get property nameID of this event
	 */
	int[] getPropertyNamesID();

	/**
	 * Get values of this event
	 */
	PropertyValue[] getPropertyValues();

	/**
	 * Get value of the specific property
	 */
	PropertyValue getPropertyValue(int nameID);

	/**
	 * Calculate and return the BucketID of this event
	 */
	int getBucketID();

	/**
	 * Get property names of this event. Can be Unsupported if necessary.
	 */
	String[] getPropertyNames() throws UnsupportedOperationException;

	/**
	 * Get property family of this event. Can be Unsupported if necessary.
	 */
	Pair<String, PropertyValue>[] getPropertyFamily() throws UnsupportedOperationException;

	/**
	 * Get property family of this event. Can be Unsupported if necessary.
	 */
	Pair<Integer, PropertyValue>[] getPropertyFamily2() throws UnsupportedOperationException;

	/**
	 * Get value of the specific property. Can be Unsupported if necessary.
	 */
	PropertyValue getPropertyValue(String propertyName) throws UnsupportedOperationException;
}