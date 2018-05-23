package org.osv.eventdb.fits.io;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

public class FitsFileSet {
	private int total;
	private int done = 0;
	private List<File> files;
	private int index = 0;

	public FitsFileSet(String pathname) {
		File fits = new File(pathname);
		files = new ArrayList<File>();
		if (fits.isFile()) {
			files.add(fits);
			total = 1;
		} else {
			File[] fileArray = fits.listFiles();
			for (File file : fileArray)
				files.add(file);
			total = fileArray.length;
		}
	}

	public synchronized int getTotal() {
		return total;
	}

	public synchronized int getDone() {
		return done;
	}

	public synchronized double getPercentDone() {
		return (double) done / (double) total;
	}

	public synchronized int incDone() {
		return ++done;
	}

	public synchronized File nextFile() {
		if (index < total) {
			return files.get(index++);
		}
		return null;
	}
}