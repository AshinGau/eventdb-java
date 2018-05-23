package org.osv.eventdb.util;

/**
 * The Pair class represents a tuple consisting of object E and object F.
 */
public class Pair<E, F> {
	private E first;
	private F second;

	public Pair(E first, F second) {
		this.first = first;
		this.second = second;
	}

	public Pair() {
	}

	public E getFirst() {
		return first;
	}

	public void setFirst(E first) {
		this.first = first;
	}

	public F getSecond() {
		return second;
	}

	public void setSecond(F second) {
		this.second = second;
	}
}