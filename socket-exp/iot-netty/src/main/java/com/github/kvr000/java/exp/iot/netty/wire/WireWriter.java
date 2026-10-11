package com.github.kvr000.java.exp.iot.netty.wire;

import io.netty.buffer.ByteBuf;

import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;


/**
 * Encodes protocol values into a {@link ByteBuf}, matching {@link MessageReader}. All methods return {@code this}.
 */
public final class WireWriter
{
	private final ByteBuf buf;

	private ByteOrder byteOrder;

	public WireWriter(ByteBuf buf)
	{
		this(buf, ByteOrder.LITTLE_ENDIAN);
	}

	public WireWriter(ByteBuf buf, ByteOrder byteOrder)
	{
		this.buf = buf;
		this.byteOrder = byteOrder;
	}

	public ByteBuf buffer()
	{
		return buf;
	}

	public WireWriter byteOrder(ByteOrder byteOrder)
	{
		this.byteOrder = byteOrder;
		return this;
	}

	private boolean le()
	{
		return byteOrder == ByteOrder.LITTLE_ENDIAN;
	}

	// ---- fixed size ---------------------------------------------------------------------------------------------

	public WireWriter writeByte(int value)
	{
		buf.writeByte(value);
		return this;
	}

	public WireWriter writeBoolean(boolean value)
	{
		buf.writeByte(value ? 1 : 0);
		return this;
	}

	public WireWriter writeShort(int value)
	{
		if (le()) buf.writeShortLE(value); else buf.writeShort(value);
		return this;
	}

	public WireWriter writeInt(int value)
	{
		if (le()) buf.writeIntLE(value); else buf.writeInt(value);
		return this;
	}

	public WireWriter writeLong(long value)
	{
		if (le()) buf.writeLongLE(value); else buf.writeLong(value);
		return this;
	}

	public WireWriter writeFloat(float value)
	{
		return writeInt(Float.floatToRawIntBits(value));
	}

	public WireWriter writeDouble(double value)
	{
		return writeLong(Double.doubleToRawLongBits(value));
	}

	// ---- varints ------------------------------------------------------------------------------------------------

	/** Unsigned 7-bit encoded int, see {@link VarInt}. */
	public WireWriter writeVarUInt32(long value)
	{
		if ((value & ~0xFFFF_FFFFL) != 0) {
			throw new IllegalArgumentException("value does not fit in 32 bits: " + value);
		}
		VarInt.write(buf, value);
		return this;
	}

	/** Unsigned 7-bit encoded long (the long is treated as unsigned), see {@link VarInt}. */
	public WireWriter writeVarUInt64(long value)
	{
		VarInt.write(buf, value);
		return this;
	}

	/** Signed (ZigZag) 7-bit encoded int, see {@link VarInt}. */
	public WireWriter writeVarInt32(int value)
	{
		VarInt.write(buf, VarInt.zigZagEncode(value) & 0xFFFF_FFFFL);
		return this;
	}

	/** Signed (ZigZag) 7-bit encoded long, see {@link VarInt}. */
	public WireWriter writeVarInt64(long value)
	{
		VarInt.write(buf, VarInt.zigZagEncode(value));
		return this;
	}

	// ---- byte blocks --------------------------------------------------------------------------------------------

	/** Raw bytes, no length. */
	public WireWriter writeBytes(byte[] value)
	{
		buf.writeBytes(value);
		return this;
	}

	/** Varint length followed by the bytes. */
	public WireWriter writeLengthPrefixedBytes(byte[] value)
	{
		VarInt.write(buf, value.length);
		buf.writeBytes(value);
		return this;
	}

	/** Varint byte length followed by the UTF-8 text. */
	public WireWriter writeLengthPrefixedString(String value)
	{
		return writeLengthPrefixedBytes(value.getBytes(StandardCharsets.UTF_8));
	}
}
