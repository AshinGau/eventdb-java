package org.osv.eventdb.fits.evt;

import java.io.IOException;

//import nom.tam.util.BufferedDataInputStream;

public class HeEvt extends Evt {
	public HeEvt() {
	}

	public HeEvt(byte[] bin) throws IOException {
		super(bin);
	}

	@Override
	public void deserialize() throws IOException {
		EvtDecoder evtParser = new EvtDecoder(bin);
		//BufferedDataInputStream evtParser = new BufferedDataInputStream(new ByteArrayInputStream(bin));
		time = evtParser.readDouble();
		detID = evtParser.readByte();
		channel = evtParser.readByte();
		pulseWidth = evtParser.readByte();
		for (int i = 0; i < 3; i++)
			evtParser.readByte();
		eventType = evtParser.readByte();
	}
}
