package org.osv.eventdb.fits;

import org.osv.eventdb.event.PropertyValue;

/**
 * High-energy event data object of fits file
 */
public class HeEvent extends FitsEvent {
	private static double timeBucketInterval = 60.0;

	public HeEvent() {
	}

	public HeEvent(byte[] bin) {
		super(bin);
	}

	/**
	 * Method to deserialize byte array
	 */
	public void deserialize() {
		EventDecoder eventDecoder = new EventDecoder(bin);
		time = eventDecoder.readDouble();
		detID = new BytePropertyValue(eventDecoder.readByte());
		channel = new BytePropertyValue(eventDecoder.readByte());
		pulse = new BytePropertyValue(eventDecoder.readByte());
		for (int i = 0; i < 3; i++)
			eventDecoder.readByte();
		eventType = new BytePropertyValue(eventDecoder.readByte());

		values = new PropertyValue[] { detID, channel, pulse, eventType };
	}

	/**
	 * Calculate and return the BucketID of this event
	 */
	public int getBucketID() {
		return (int) Math.floor(time / timeBucketInterval);
	}
}