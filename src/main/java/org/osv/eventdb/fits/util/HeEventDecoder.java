package org.osv.eventdb.fits.util;

import java.util.LinkedList;
import java.util.List;

import org.osv.eventdb.fits.EventDecoder;

public class HeEventDecoder {
	public static class He {
		public double time;
		public byte detID;
		public byte channel;
		public byte pulse;
		public byte eventType;

		public He(byte[] bin) {
			EventDecoder eventDecoder = new EventDecoder(bin);
			time = eventDecoder.readDouble();
			detID = eventDecoder.readByte();
			channel = eventDecoder.readByte();
			pulse = eventDecoder.readByte();
			for (int i = 0; i < 3; i++)
				eventDecoder.readByte();
			eventType = eventDecoder.readByte();
		}

		@Override
		public String toString() {
			return String.format("%f,%d,%d,%d,%d\n", time, detID & 0x00ff, channel & 0x00ff, pulse & 0x00ff,
					eventType & 0x00ff);
		}
	}

	public static List<He> decode(List<byte[]> result) {
		List<He> list = new LinkedList<He>();
		for (byte[] events : result) {
			byte[] evtBin = new byte[16];
			int count = events.length / 16;
			for (int i = 0; i < count; i++) {
				int index = i * 16;
				for (int j = 0; j < 16; j++)
					evtBin[j] = events[index + j];
				list.add(new He(evtBin));
			}
		}
		return list;
	}
}