package org.osv.eventdb.fits.io;

import java.io.IOException;

import org.osv.eventdb.fits.evt.HeEvt;
import org.osv.eventdb.util.ConfigProperties;

public class HeFits2Hbase extends Fits2Hbase<HeEvt> {
	public HeFits2Hbase(FitsFileSet fits, ConfigProperties configProp, String tablename) throws Exception {
		super(fits, configProp, tablename, 16);
	}

	public HeFits2Hbase(FitsFileSet fits, String tablename) throws Exception {
		super(fits, tablename, 16);
	}

	@Override
	protected HeEvt getEvt(byte[] evtBin) throws IOException {
		return new HeEvt(evtBin);
	}

	@Override
	protected FitsFile<HeEvt> getFitsFile(String filename) {
		return new HeFitsFile(filename);
	}
}
