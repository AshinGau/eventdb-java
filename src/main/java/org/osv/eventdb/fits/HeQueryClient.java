package org.osv.eventdb.fits;

import java.io.IOException;

public class HeQueryClient extends FitsQueryClient {
	public HeQueryClient(String tableName) throws IOException {
		super(tableName, 16);
	}

	protected FitsEvent getEvt(byte[] bin) {
		return new HeEvent(bin);
	}
}