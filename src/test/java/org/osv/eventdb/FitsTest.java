package org.osv.eventdb;

import org.junit.Test;
import org.osv.eventdb.fits.FitsEventDBClient;

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
