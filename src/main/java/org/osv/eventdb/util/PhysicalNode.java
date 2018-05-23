/**
 * Implemented by fanfish@https://github.com/fanfish/ConsistentHash
 * Reference http://www.codeproject.com/Articles/56138/Consistent-hashing
 */
package org.osv.eventdb.util;

/**
 * Physical node for consistent hash algorithm, assign a String object as its
 * identification.
 */
public class PhysicalNode {
	private String id;

	/**
	 * Create a physicalNode with a specific identification
	 */
	public PhysicalNode(String id) {
		this.id = id;
	}

	@Override
	public String toString() {
		return id;
	}

	public void setId(String id) {
		this.id = id;
	}

	public String getId() {
		return id;
	}
}