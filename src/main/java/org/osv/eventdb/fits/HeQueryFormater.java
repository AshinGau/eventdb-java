package org.osv.eventdb.fits;

import java.io.IOException;

import org.osv.eventdb.event.PropertyValue;

public class HeQueryFormater extends FitsQueryFormater {
	public HeQueryFormater(String queryString) throws IOException {
		super(queryString);
	}

	protected PropertyValue getPropertyValue(int property, String valueStr) {
		int value = Integer.valueOf(valueStr);
		return new BytePropertyValue((byte) value);
	}
}