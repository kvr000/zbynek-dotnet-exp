package com.github.kvr000.java.exp.iot.netty.wire;

import io.netty.buffer.ByteBuf;


/**
 * 7-bit encoded integers (LEB128, as .NET BinaryWriter.Write7BitEncodedInt): 7 bits per byte, least significant group
 * first, high bit set on every byte but the last. Signed values use ZigZag encoding (as protobuf sint32/sint64).
 */
public final class VarInt
{
	public static final int MAX_LENGTH_32 = 5;
	public static final int MAX_LENGTH_64 = 10;

	private VarInt()
	{
	}

	/**
	 * Reads value of at most {@code bits} (32 or 64) bits.
	 *
	 * @throws ProtocolException
	 * 	if the value does not fit
	 */
	public static long read(ByteBuf buf, int bits)
	{
		long value = 0;
		for (int shift = 0; ; shift += 7) {
			int b = buf.readByte() & 0xFF;
			if (shift + 7 > bits && (b & 0x7F) >>> (bits - shift) != 0) {
				throw new ProtocolException("varint exceeds " + bits + " bits");
			}
			value |= (long) (b & 0x7F) << shift;
			if ((b & 0x80) == 0) {
				return value;
			}
			if (shift + 7 >= bits) {
				throw new ProtocolException("varint exceeds " + bits + " bits");
			}
		}
	}

	/** Writes unsigned value (the long is treated as unsigned 64 bits). */
	public static void write(ByteBuf buf, long value)
	{
		while ((value & ~0x7FL) != 0) {
			buf.writeByte((int) (value & 0x7F) | 0x80);
			value >>>= 7;
		}
		buf.writeByte((int) value);
	}

	public static int zigZagEncode(int value)
	{
		return (value << 1) ^ (value >> 31);
	}

	public static long zigZagEncode(long value)
	{
		return (value << 1) ^ (value >> 63);
	}

	public static int zigZagDecode(int value)
	{
		return (value >>> 1) ^ -(value & 1);
	}

	public static long zigZagDecode(long value)
	{
		return (value >>> 1) ^ -(value & 1);
	}
}
