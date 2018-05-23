package org.osv.eventdb.fits;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Iterator;
import java.util.NoSuchElementException;

public abstract class FitsFile implements Iterable<FitsEvent> {
	private int rowStart;
	private int evtStart;
	private int evtLength;
	private int rowCount;
	private FileInputStream fin;
	private String filepath;
	private int bufferSize = 1024 * 1024;
	private byte[] buffer;
	private int evtStartIndex;
	private byte[] evtBin;

	public FitsFile(String filepath, int rowStart, int evtStart, int evtLength) {
		this.filepath = filepath;
		this.rowStart = rowStart;
		this.evtStart = evtStart;
		this.evtLength = evtLength;
	}

	public void setBufferSize(int size) {
		bufferSize = size;
	}

	public void close() throws IOException {
		fin.close();
		buffer = null;
	}

	protected abstract FitsEvent getEvt(byte[] evtBin);

	private void readToBuffer() {
		try {
			if ((evtStartIndex + bufferSize) < rowCount) {
				evtStartIndex += bufferSize;
				int remain = rowCount - evtStartIndex;
				int readSize = remain > bufferSize ? bufferSize : remain;
				fin.read(buffer, 0, readSize * evtLength);
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public Iterator<FitsEvent> iterator() {
		try {
			fin = new FileInputStream(filepath);
			// get row number
			fin.skip(rowStart);
			byte[] brows = new byte[22];
			fin.read(brows);
			String rows = new String(brows);
			rowCount = Integer.valueOf(rows.trim());
			// seek at the beginning of event array
			fin.skip(evtStart - rowStart - 22);

			buffer = new byte[bufferSize * evtLength];
			evtStartIndex = -bufferSize;
			readToBuffer();
		} catch (IOException e) {
			e.printStackTrace();
		}
		return new EvtIterator();
	}

	private class EvtIterator implements Iterator<FitsEvent> {
		private int index = 0;

		public boolean hasNext() {
			return index != rowCount;
		}

		public FitsEvent next() throws NoSuchElementException {
			if (index == rowCount)
				throw new NoSuchElementException("Has read to the end of fits file");
			if (index == (evtStartIndex + bufferSize))
				readToBuffer();
			int start = (index - evtStartIndex) * evtLength;
			evtBin = new byte[evtLength];
			for (int i = 0; i < evtLength; i++)
				evtBin[i] = buffer[start + i];
			index++;
			return getEvt(evtBin);
		}
	}
}
