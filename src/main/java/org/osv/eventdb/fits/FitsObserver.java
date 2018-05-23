package org.osv.eventdb.fits;

import java.io.IOException;

import org.osv.eventdb.hbase.MDRegionObserver;
import org.osv.eventdb.util.BitArray;

public class FitsObserver extends MDRegionObserver {
	/**
	 * bitArray saves the bit-information of the result of the query in current time
	 * bucket.
	 * 
	 * @param events       byte array of events of the current time bucket
	 * @param bitArray     bit-information of the result of the query
	 * @param eventCount   the number of events in current time bucket
	 * @param extractCount equals bitArray.count()
	 * @return byte[] byte array of events of the result in current time bucket
	 * @throws IOException if bitArray.getBitLength() < eventCount or
	 *                     bitArray.getBitLength() > eventCount + 8
	 */
	protected byte[] extractEvents(byte[] events, BitArray bitArray, int eventCount, int extractCount)
			throws IOException {
		if (bitArray.getBitLength() < eventCount || bitArray.getBitLength() > eventCount + 8)
			throw new IOException("eventCount error!");
		int evtLength = events.length / eventCount;
		byte[] finalEvents = new byte[evtLength * extractCount];
		int index = 0;
		for (int i = 0; i < eventCount; i++)
			if (bitArray.get(i)) {
				int eventsIndex = i * evtLength;
				for (int j = 0; j < evtLength; j++)
					finalEvents[index + j] = events[eventsIndex + j];
				index += evtLength;
			}
		return finalEvents;
	}
}