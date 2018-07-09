package org.osv.eventdb;

import org.junit.Test;
import org.osv.eventdb.fits.FitsFile;
import org.osv.eventdb.fits.util.HeEventDecoder.He;

public class FitsTest {
	@Test
	public void testFunc() throws Exception {
		FitsFile fits = new FitsFile("data/R-ID0301-00000439-20180703-v1.FITS");
		int index = 0;
		for (byte[] evt : fits) {
			if (index > 10)
				break;
			index++;
			He he = new He(evt);
			System.out.printf("%f\t%d\t%d\t%d\t%d\n", he.time, he.detID & 0x00ff, he.channel & 0x00ff,
					he.pulse & 0x00ff, he.eventType & 0x00ff);
		}
	}
}
