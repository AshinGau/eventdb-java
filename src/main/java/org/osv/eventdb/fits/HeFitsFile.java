package org.osv.eventdb.fits;

public class HeFitsFile extends FitsFile {
	public HeFitsFile(String filepath) {
		super(filepath, 2880 * 2 + 80 * 4 + 9, 2880 * 4, 16);
	}

	protected FitsEvent getEvt(byte[] evtBin) {
		return new HeEvent(evtBin);
	}
}
