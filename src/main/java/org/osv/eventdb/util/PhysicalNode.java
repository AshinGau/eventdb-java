package org.osv.eventdb.util;

public class PhysicalNode {
	private String id;

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