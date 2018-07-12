package org.osv.eventdb;

import org.osv.eventdb.fits.FitsFileSet;
import org.osv.eventdb.fits.HeFits2Hbase;
import org.osv.eventdb.fits.util.RestServer;
import org.osv.eventdb.hbase.TableAction;
import org.osv.eventdb.util.ConfigProperties;
import org.osv.eventdb.util.ObserverAction;

public class Run {
	public static void main(String[] args) throws Exception {
		ConfigProperties prop = new ConfigProperties();

		if (args[0].equals("insertHeFits")) {
			// pathname, talbename, [threads]
			String fitsFile = args[1];
			String tableName = args[2];
			int threads = Integer.valueOf(args[3]);

			FitsFileSet fits = new FitsFileSet(fitsFile);
			int fileNum = fits.getTotal();
			threads = fileNum > threads ? threads : fileNum;
			System.out.printf("Insert HeFits file in %d threads\n", threads);
			for (int i = 0; i < threads; i++) {
				HeFits2Hbase fits2Hbase = new HeFits2Hbase(fits, tableName);
				fits2Hbase.start();
			}
		} else if (args[0].equals("createTable")) {
			TableAction taction = new TableAction(args[1]);
			// tablename regions
			taction.createTable(Integer.valueOf(args[2]));
			System.out.printf("success to create table(%s)\n", args[1]);

		} else if (args[0].equals("deleteTable")) {
			TableAction taction = new TableAction(args[1]);
			// talbename
			taction.deleteTable();
			System.out.printf("success to delete table(%s)\n", args[1]);

		} else if (args[0].equals("observer")) {
			// tablename coprocessorClass jarPath
			// org.osv.eventdb.hbase.MDRegionObserver
			ObserverAction action = new ObserverAction(args[1]);
			action.addCoprocessor(args[2], args[3]);
			System.out.printf("success to create eventdb formatted table(%s)\n", args[1]);

		} else if (args[0].equals("HeFitsQuery")) {
			HeFitsShell.console(args[1]);

		} else if (args[0].equals("server")) {
			RestServer.runAtPort(new Integer(args[1]));

		} else if (args[0].equals("init")) {// init meta table
			TableAction taction = new TableAction(prop.getProperty("fits.meta.table"));
			taction.initMetaTable();
			System.out.printf("success to init meta table\n");

		}
	}
}
