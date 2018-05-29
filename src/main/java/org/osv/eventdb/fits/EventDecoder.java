package org.osv.eventdb.fits;

import nom.tam.util.FitsIO;

/**
 * Class that encapsulates methods to deserialize event data.
 */
public class EventDecoder {
	private int offset = 0;
	private byte[] bin;

	/**
	 * @param bin byte array to be deserialized
	 */
	public EventDecoder(byte[] bin) {
		this.bin = bin;
	}

	public short readShort() {
		return (short) (bin[offset++] << FitsIO.BITS_OF_1_BYTE | //
				bin[offset++] & FitsIO.BYTE_MASK);
	}

	public int readInt() {
		return bin[offset++] << FitsIO.BITS_OF_3_BYTES | //
				(bin[offset++] & FitsIO.BYTE_MASK) << FitsIO.BITS_OF_2_BYTES | //
				(bin[offset++] & FitsIO.BYTE_MASK) << FitsIO.BITS_OF_1_BYTE | //
				bin[offset++] & FitsIO.BYTE_MASK;
	}

	public double readDouble() {
		int i1 = readInt();
		int i2 = readInt();
		long bits = (long) i1 << FitsIO.BITS_OF_4_BYTES | i2 & FitsIO.INTEGER_MASK;
		return Double.longBitsToDouble(bits);
	}

	public byte readByte() {
		return (byte) (bin[offset++] & 0xff);
	}
}