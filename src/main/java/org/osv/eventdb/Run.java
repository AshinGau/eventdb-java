package org.osv.eventdb;

import org.osv.eventdb.fits.FitsFileSet;
import org.osv.eventdb.fits.HeFits2Hbase;
import org.osv.eventdb.hbase.TableAction;
import org.osv.eventdb.util.ObserverAction;

public class Run {
	public static void main(String[] args) throws Exception {
		if (args[0].equals("insertHeFits")) {
			// pathname, talbename, [threads]
			FitsFileSet fits = new FitsFileSet(args[1]);
			HeFits2Hbase fits2Hbase = new HeFits2Hbase(fits, args[2]);
			fits2Hbase.insertFitsFile();

		} else if (args[0].equals("createTable")) {
			TableAction taction = new TableAction(args[1]);
			// tablename regions
			taction.createTable(Integer.valueOf(args[2]));
			System.out.printf("success to create table(%s)\n", args[1]);

		} else if (args[0].equals("deleteTable")) {
			TableAction taction = new TableAction(args[1]);
			// talbename
			taction.deleteTable(args[1]);
			System.out.printf("success to delete table(%s)\n", args[1]);

		} else if (args[0].equals("observer")) {
			// tablename coprocessorClass jarPath
			// org.osv.eventdb.hbase.MDRegionObserver
			ObserverAction action = new ObserverAction(args[1]);
			action.addCoprocessor(args[2], args[3]);

		}
	}
}
