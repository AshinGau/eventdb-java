package org.osv.eventdb;

import org.junit.Test;
import org.osv.eventdb.fits.FitsFile;
import org.osv.eventdb.fits.FitsEventDBClient;
import org.osv.eventdb.fits.util.HeEventDecoder.He;

public class FitsTest {
	@Test
	public void testFunc() throws Exception {
		FitsEventDBClient client = new FitsEventDBClient();
		String[] tables = client.list();
		for(String t: tables) {
			System.out.println(t);
		}
	}
}
