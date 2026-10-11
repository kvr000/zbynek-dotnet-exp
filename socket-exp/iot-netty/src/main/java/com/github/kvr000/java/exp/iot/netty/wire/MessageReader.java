package com.github.kvr000.java.exp.iot.netty.wire;

import io.netty.buffer.ByteBuf;

import java.nio.ByteOrder;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.CodingErrorAction;
import java.nio.charset.StandardCharsets;


/**
 * Protocol value reader over a {@link ByteBuf}: byte order, varints, length-prefixed bytes and strings.
 *
 * Meant for the buffer passed to {@link io.netty.handler.codec.ReplayingDecoder#decode}: when a read runs past the
 * received bytes, that buffer throws Netty's replay signal (a preallocated {@code Error} without stack trace), the
 * decoder rewinds to its last checkpoint and runs again once more bytes arrive. So parsers are straight-line code
 * that assumes the whole message is there. Malformed data throws {@link ProtocolException}.
 */
public final class MessageReader
{
	private final ByteBuf buf;

	private ByteOrder byteOrder;

	public MessageReader(ByteBuf buf)
	{
		this(buf, ByteOrder.LITTLE_ENDIAN);
	}

	public MessageReader(ByteBuf buf, ByteOrder byteOrder)
	{
		this.buf = buf;
		this.byteOrder = byteOrder;
	}

	/** Byte order of fixed-size numbers; may be switched mid-message. */
	public ByteOrder byteOrder()
	{
		return byteOrder;
	}

	public MessageReader byteOrder(ByteOrder byteOrder)
	{
		this.byteOrder = byteOrder;
		return this;
	}

	// ---- fixed size ---------------------------------------------------------------------------------------------

	public byte readByte()
	{
		return buf.readByte();
	}

	public int readUnsignedByte()
	{
		return readByte() & 0xFF;
	}

	/** One byte: 0 is false, 1 is true, anything else is a {@link ProtocolException}. */
	public boolean readBoolean()
	{
		int b = readUnsignedByte();
		switch (b) {
		case 0:
			return false;
		case 1:
			return true;
		default:
			throw new ProtocolException(String.format("invalid boolean 0x%02X", b));
		}
	}

	public short readShort()
	{
		return byteOrder == ByteOrder.LITTLE_ENDIAN ? buf.readShortLE() : buf.readShort();
	}

	public int readUnsignedShort()
	{
		return readShort() & 0xFFFF;
	}

	public int readInt()
	{
		return byteOrder == ByteOrder.LITTLE_ENDIAN ? buf.readIntLE() : buf.readInt();
	}

	public long readUnsignedInt()
	{
		return readInt() & 0xFFFF_FFFFL;
	}

	public long readLong()
	{
		return byteOrder == ByteOrder.LITTLE_ENDIAN ? buf.readLongLE() : buf.readLong();
	}

	public float readFloat()
	{
		return Float.intBitsToFloat(readInt());
	}

	public double readDouble()
	{
		return Double.longBitsToDouble(readLong());
	}

	// ---- varints ------------------------------------------------------------------------------------------------

	/** Unsigned 7-bit encoded int (0 .. 2^32-1), see {@link VarInt}. */
	public long readVarUInt32()
	{
		return VarInt.read(buf, 32);
	}

	/** Unsigned 7-bit encoded long (all 64 bits, compare with Long.compareUnsigned), see {@link VarInt}. */
	public long readVarUInt64()
	{
		return VarInt.read(buf, 64);
	}

	/** Signed (ZigZag) 7-bit encoded int, see {@link VarInt}. */
	public int readVarInt32()
	{
		return VarInt.zigZagDecode((int) readVarUInt32());
	}

	/** Signed (ZigZag) 7-bit encoded long, see {@link VarInt}. */
	public long readVarInt64()
	{
		return VarInt.zigZagDecode(readVarUInt64());
	}

	// ---- byte blocks --------------------------------------------------------------------------------------------

	/** Exactly {@code count} bytes, copied. */
	public byte[] readBytes(int count)
	{
		byte[] bytes = new byte[count];
		buf.readBytes(bytes);
		return bytes;
	}

	/** Fills {@code destination}. */
	public void readBytes(byte[] destination)
	{
		buf.readBytes(destination);
	}

	/**
	 * Exactly {@code count} bytes without copying. Valid only until the parser returns: the memory belongs to the
	 * input buffer and is reused afterwards.
	 */
	public ByteBuf readSlice(int count)
	{
		return buf.readSlice(count);
	}

	public void skip(int count)
	{
		buf.skipBytes(count);
	}

	/** Varint length, checked against {@code maxLength}. */
	public int readLength(int maxLength, String what)
	{
		long length = readVarUInt32();
		if (length > maxLength) {
			throw new ProtocolException(what + " too long: " + length + " > " + maxLength);
		}
		return (int) length;
	}

	/** Varint length (at most {@code maxLength}) followed by that many bytes. */
	public byte[] readLengthPrefixedBytes(int maxLength, String what)
	{
		return readBytes(readLength(maxLength, what));
	}

	/** Varint byte length (at most {@code maxLength}) followed by that many bytes of UTF-8; invalid UTF-8 is rejected. */
	public String readLengthPrefixedString(int maxLength, String what)
	{
		int length = readLength(maxLength, what);
		try {
			String value = StandardCharsets.UTF_8.newDecoder()
				.onMalformedInput(CodingErrorAction.REPORT)
				.onUnmappableCharacter(CodingErrorAction.REPORT)
				.decode(buf.nioBuffer(buf.readerIndex(), length))
				.toString();
			buf.skipBytes(length);
			return value;
		}
		catch (CharacterCodingException ex) {
			throw new ProtocolException(what + " is not valid UTF-8", ex);
		}
	}
}
