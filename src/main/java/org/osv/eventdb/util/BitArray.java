package org.osv.eventdb.util;

public class BitArray {
	private byte[] bits;
	private int length;
	private int bitLength;

	public BitArray(int bitLength) {
		this.bitLength = bitLength;
		length = (int) Math.ceil(bitLength / 8.0);
		bits = new byte[length];
	}

	public BitArray(byte[] bits) {
		this(bits.length * 8);
		for (int i = 0; i < length; i++)
			this.bits[i] = bits[i];
	}

	public BitArray(String bitsString) {
		this(bitsString.length());
		int strLen = bitsString.length();
		for (int i = 0; i < strLen; i++)
			if (bitsString.charAt(i) == '1')
				set(i);
	}

	public BitArray(byte[] bits, int bitLength) {
		this(bits.length * 8);
		for (int i = 0; i < length; i++)
			this.bits[i] = bits[i];
		int lengthCompute = (int) Math.ceil(bitLength / 8.0);
		if (lengthCompute == length)
			this.bitLength = bitLength;
	}

	public BitArray(BitArray bitArray) {
		this(bitArray.getBits());
	}

	public int getLength() {
		return length;
	}

	public int getBitLength() {
		return bitLength;
	}

	public byte[] getBits() {
		return bits;
	}

	public BitArray set(int position) {
		if (position >= bitLength)
			return null;
		int byteOffset = position / 8;
		int bitOffset = position % 8;
		bits[byteOffset] |= (byte) (0x80 >> bitOffset);
		return this;
	}

	public BitArray clear(int position) {
		if (position >= bitLength)
			return null;
		int byteOffset = position / 8;
		int bitOffset = position % 8;
		byte complement = (byte) ~(0x80 >> bitOffset);
		bits[byteOffset] &= complement;
		return this;
	}

	public boolean get(int position) {
		if (position >= bitLength)
			return false;
		int byteOffset = position / 8;
		int bitOffset = position % 8;
		return (byte) (bits[byteOffset] & (0x80 >> bitOffset)) != 0;
	}

	public BitArray not() {
		for (int i = 0; i < length; i++)
			bits[i] = (byte) ~bits[i];
		return this;
	}

	public BitArray and(byte[] bits) {
		if (bits == null || bits.length != length)
			return null;
		for (int i = 0; i < length; i++)
			this.bits[i] &= bits[i];
		return this;
	}

	public BitArray and(BitArray bitArray) {
		if (bitArray == null || bitArray.getLength() != length)
			return null;
		return and(bitArray.getBits());
	}

	public BitArray or(byte[] bits) {
		if (bits == null || bits.length != length)
			return null;
		for (int i = 0; i < length; i++)
			this.bits[i] |= bits[i];
		return this;
	}

	public BitArray or(BitArray bitArray) {
		if (bitArray == null || bitArray.getLength() != length)
			return null;
		return or(bitArray.getBits());
	}

	public BitArray xor(byte[] bits) {
		if (bits == null || bits.length != length)
			return null;
		for (int i = 0; i < length; i++)
			this.bits[i] ^= bits[i];
		return this;
	}

	public BitArray xor(BitArray bitArray) {
		if (bitArray == null || bitArray.getLength() != length)
			return null;
		return xor(bitArray.getBits());
	}

	public BitArray nor(byte[] bits) {
		if (bits == null || bits.length != length)
			return null;
		for (int i = 0; i < length; i++)
			this.bits[i] = (byte) ~(this.bits[i] ^ bits[i]);
		return this;
	}

	public BitArray nor(BitArray bitArray) {
		if (bitArray == null || bitArray.getLength() != length)
			return null;
		return nor(bitArray.getBits());
	}

	@Override
	public String toString() {
		StringBuilder result = new StringBuilder();
		for (int i = 0; i < bitLength; i++)
			if (get(i))
				result.append('1');
			else
				result.append('0');
		return result.toString();
	}
}