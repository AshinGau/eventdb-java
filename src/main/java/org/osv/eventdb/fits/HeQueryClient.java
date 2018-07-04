package org.osv.eventdb.fits;

import java.io.IOException;

import org.osv.eventdb.hbase.MDQuery;

public class HeQueryClient extends FitsQueryClient {
	public HeQueryClient(String tableName) throws IOException {
		super(tableName, 16);
	}

	public HeQueryClient(MDQuery mdQuery) {
		super(mdQuery, 16);
	}

	protected FitsEvent getEvt(byte[] bin) {
		return new HeEvent(bin);
	}
}