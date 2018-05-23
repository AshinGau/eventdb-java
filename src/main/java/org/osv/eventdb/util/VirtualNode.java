package org.osv.eventdb.util;

public class VirtualNode {
	private int replicaNumber;
	private PhysicalNode parent;

	public VirtualNode(PhysicalNode parent, int replicaNumber) {
		this.replicaNumber = replicaNumber;
		this.parent = parent;
	}

	public boolean matches(String id) {
		return parent.toString().equalsIgnoreCase(id);
	}

	@Override
	public String toString() {
		return parent.toString().toLowerCase() + ":" + replicaNumber;
	}

	public int getReplicaNumber() {
		return replicaNumber;
	}

	public void setReplicaNumber(int replicaNumber) {
		this.replicaNumber = replicaNumber;
	}

	public PhysicalNode getParent() {
		return parent;
	}
}