package org.osv.eventdb.fits;

import java.io.IOException;

import org.osv.eventdb.util.ConfigProperties;

public class HeFits2Hbase extends Fits2Hbase {
	public HeFits2Hbase(FitsFileSet fits, ConfigProperties configProp, String tablename) throws Exception {
		super(fits, configProp, tablename, 16);
	}

	public HeFits2Hbase(FitsFileSet fits, String tablename) throws Exception {
		super(fits, tablename, 16);
	}

	@Override
	protected FitsEvent getEvt(byte[] evtBin) throws IOException {
		return new HeEvent(evtBin);
	}

	@Override
	protected FitsFile getFitsFile(String filename) {
		return new HeFitsFile(filename);
	}
}
