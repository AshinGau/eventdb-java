package org.osv.eventdb.util;

/**
 * The BitArray class wraps a bit array and provides all kinds of bit oprations.
 */
public class BitArray {
	private byte[] bits;
	private int length;
	private int bitLength;

	/**
	 * Initialize a BitArray object, and assigns every bit to zero
	 * 
	 * @param bitLength the length of bit array
	 */
	public BitArray(int bitLength) {
		this.bitLength = bitLength;
		length = (int) Math.ceil(bitLength / 8.0);
		bits = new byte[length];
	}

	/**
	 * Create a BitArray object with the specified byte array
	 * 
	 * @param bits the byte array assigned to the bit array
	 */
	public BitArray(byte[] bits) {
		this.bits = bits;
		length = bits.length;
		bitLength = length * 8;
	}

	/**
	 * Format a "01"-like string to a BitArray object
	 */
	public BitArray(String bitsString) {
		this(bitsString.length());
		int strLen = bitsString.length();
		for (int i = 0; i < strLen; i++)
			if (bitsString.charAt(i) == '1')
				set(i);
	}

	/**
	 * Copy a BitArray object
	 */
	public BitArray(BitArray bitArray) {
		this(bitArray.getBitLength());
		byte[] _bits = bitArray.getBits();
		for (int i = 0; i < length; i++)
			bits[i] = _bits[i];
	}

	/**
	 * Get length of the byte array
	 */
	public int getLength() {
		return length;
	}

	/**
	 * Get length of the bit array
	 */
	public int getBitLength() {
		return bitLength;
	}

	/**
	 * Get the byte array
	 */
	public byte[] getBits() {
		return bits;
	}

	/**
	 * Set the position-th bit to 1
	 */
	public BitArray set(int position) {
		if (position >= bitLength)
			return null;
		int byteOffset = position / 8;
		int bitOffset = position % 8;
		bits[byteOffset] |= (byte) (0x80 >> bitOffset);
		return this;
	}

	/**
	 * Set the position-th bit to 0
	 */
	public BitArray clear(int position) {
		if (position >= bitLength)
			return null;
		int byteOffset = position / 8;
		int bitOffset = position % 8;
		byte complement = (byte) ~(0x80 >> bitOffset);
		bits[byteOffset] &= complement;
		return this;
	}

	/**
	 * Get the position-th value of the bit array
	 */
	public boolean get(int position) {
		if (position >= bitLength)
			return false;
		int byteOffset = position / 8;
		int bitOffset = position % 8;
		return (byte) (bits[byteOffset] & (0x80 >> bitOffset)) != 0;
	}

	/**
	 * Inverse the bit array
	 */
	public BitArray not() {
		for (int i = 0; i < length; i++)
			bits[i] = (byte) ~bits[i];
		return this;
	}

	/**
	 * Do and-operation with a byte array and return itself for chain operation
	 */
	public BitArray and(byte[] bits) {
		if (bits == null || bits.length != length)
			return null;
		for (int i = 0; i < length; i++)
			this.bits[i] &= bits[i];
		return this;
	}

	/**
	 * Do and-operation with another bitArray and return itself for chain operation
	 */
	public BitArray and(BitArray bitArray) {
		if (bitArray == null || bitArray.getLength() != length)
			return null;
		return and(bitArray.getBits());
	}

	/**
	 * Do or-operation with a byte array and return itself for chain operation
	 */
	public BitArray or(byte[] bits) {
		if (bits == null || bits.length != length)
			return null;
		for (int i = 0; i < length; i++)
			this.bits[i] |= bits[i];
		return this;
	}

	/**
	 * Do or-operation with another bitArray and return itself for chain operation
	 */
	public BitArray or(BitArray bitArray) {
		if (bitArray == null || bitArray.getLength() != length)
			return null;
		return or(bitArray.getBits());
	}

	/**
	 * Do xor-operation with a byte array and return itself for chain operation
	 */
	public BitArray xor(byte[] bits) {
		if (bits == null || bits.length != length)
			return null;
		for (int i = 0; i < length; i++)
			this.bits[i] ^= bits[i];
		return this;
	}

	/**
	 * Do xor-operation with another bitArray and return itself for chain operation
	 */
	public BitArray xor(BitArray bitArray) {
		if (bitArray == null || bitArray.getLength() != length)
			return null;
		return xor(bitArray.getBits());
	}

	/**
	 * Do nor-operation with a byte array and return itself for chain operation
	 */
	public BitArray nor(byte[] bits) {
		if (bits == null || bits.length != length)
			return null;
		for (int i = 0; i < length; i++)
			this.bits[i] = (byte) ~(this.bits[i] ^ bits[i]);
		return this;
	}

	/**
	 * Do nor-operation with another bitArray and return itself for chain operation
	 */
	public BitArray nor(BitArray bitArray) {
		if (bitArray == null || bitArray.getLength() != length)
			return null;
		return nor(bitArray.getBits());
	}

	public int count() {
		int cnt = 0;
		for (byte b : bits)
			for (int i = 0; i < 8; i++)
				cnt += (b >> i) & 0x01;
		return cnt;
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