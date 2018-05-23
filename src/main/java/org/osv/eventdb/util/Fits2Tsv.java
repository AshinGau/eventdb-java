package org.osv.eventdb.util;

import java.io.*;
import org.osv.eventdb.fits.io.*;
import org.osv.eventdb.fits.evt.*;

public class Fits2Tsv {
	public static void heConvert(String fitsFilePath, String tsvFilePath) throws IOException {
		HeFitsFile he = new HeFitsFile(fitsFilePath);
		OutputStream outStream = new FileOutputStream(tsvFilePath);
		BufferedOutputStream out = new BufferedOutputStream(outStream);
		out.write("time\tdetID\tchannel\tpulseWidth\teventType".getBytes());
		for (HeEvt evt : he) {
			out.write("\n".getBytes());
			out.write(String.valueOf(evt.getTime()).getBytes());
			out.write("\t".getBytes());
			out.write(String.valueOf(evt.getDetID() & 0x00ff).getBytes());
			out.write("\t".getBytes());
			out.write(String.valueOf(evt.getChannel() & 0x00ff).getBytes());
			out.write("\t".getBytes());
			out.write(String.valueOf(evt.getPulseWidth() & 0x00ff).getBytes());
			out.write("\t".getBytes());
			out.write(String.valueOf(evt.getEventType() & 0x00ff).getBytes());
		}
		out.close();
		outStream.close();
	}
}