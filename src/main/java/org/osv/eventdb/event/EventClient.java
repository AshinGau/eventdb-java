package org.osv.eventdb.event;

/**
 * Provide methods mapping between name and nameID
 */
public interface EventClient {
	/**
	 * map nameID to name
	 */
	String getPropertyName(int nameID);

	/**
	 * map name to nameID
	 */
	int getPropertyNameID(String name);
}