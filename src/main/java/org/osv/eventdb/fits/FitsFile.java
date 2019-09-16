package org.osv.eventdb.fits;

import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.Iterator;
import java.util.NoSuchElementException;

import nom.tam.fits.BinaryTableHDU;
import nom.tam.fits.Fits;
import nom.tam.fits.FitsException;
import nom.tam.fits.Header;

public class FitsFile implements Iterable<byte[]> {
	public int rows;
	public int evtLength;
	public long tstart;
	public long tstop;
	public String dateStart;
	public String dateStop;
	public String type;
	private long offset;
	private BufferedInputStream bufIn;
	private String file;

	public FitsFile(String filepath) throws FitsException, IOException {
		this.file = filepath;
		Fits fits = new Fits(filepath);
		BinaryTableHDU hdu = (BinaryTableHDU) fits.getHDU(1);

		Header header = hdu.getHeader();
		rows = header.getIntValue("NAXIS2");
		evtLength = header.getIntValue("NAXIS1");
		// tstart = header.getBigIntegerValue("TSTART").longValue();
		tstart = (long)header.getDoubleValue("TSTART");
		// tstop = header.getBigIntegerValue("TSTOP").longValue();
		tstop = (long)header.getDoubleValue("TSTOP");
		dateStart = header.getStringValue("DATE-OBS").trim();
		dateStop = header.getStringValue("DATE-END").trim();
		type = header.getStringValue("INSTRUME").trim();

		offset = hdu.getData().getFileOffset();

		fits.close();
	}

	public void close() throws IOException {
		bufIn.close();
	}

	public Iterator<byte[]> iterator() {
		try {
			bufIn = new BufferedInputStream(new FileInputStream(file));
			bufIn.skip(offset);
		} catch (IOException e) {
			return null;
		}
		return new EvtIterator();
	}

	private class EvtIterator implements Iterator<byte[]> {
		private int index = 0;

		public boolean hasNext() {
			return index != rows;
		}

		public byte[] next() throws NoSuchElementException {
			if (index == rows)
				throw new NoSuchElementException("Has read to the end of fits file");
			byte[] evt = new byte[evtLength];
			try {
				bufIn.read(evt);
				index++;
				return evt;
			} catch (IOException e) {
				return null;
			}
		}
	}
}
