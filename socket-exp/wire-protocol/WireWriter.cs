using System;
using System.Buffers;
using System.Buffers.Binary;
using System.Text;

namespace WireProtocol;

/// <summary>
/// Encodes protocol values into an <see cref="IBufferWriter{T}"/>: a <see cref="System.IO.Pipelines.PipeWriter"/>
/// (flush it afterwards) or an <see cref="ArrayBufferWriter{T}"/> to build bytes in memory, e.g. for signing.
/// The encoding matches <see cref="WireReader"/>.
/// </summary>
public sealed class WireWriter
{
    private readonly IBufferWriter<byte> output;

    public WireWriter(IBufferWriter<byte> output, ByteOrder byteOrder = ByteOrder.LittleEndian)
    {
        this.output = output;
        ByteOrder = byteOrder;
    }

    public ByteOrder ByteOrder { get; set; }

    public IBufferWriter<byte> Output => output;

    // ---- fixed size -----------------------------------------------------------------------------------------------

    public void WriteByte(byte value)
    {
        output.GetSpan(1)[0] = value;
        output.Advance(1);
    }

    public void WriteSByte(sbyte value) => WriteByte((byte)value);

    public void WriteBoolean(bool value) => WriteByte(value ? (byte)1 : (byte)0);

    public void WriteInt16(short value)
    {
        Span<byte> span = output.GetSpan(sizeof(short));
        if (ByteOrder == ByteOrder.LittleEndian)
        {
            BinaryPrimitives.WriteInt16LittleEndian(span, value);
        }
        else
        {
            BinaryPrimitives.WriteInt16BigEndian(span, value);
        }
        output.Advance(sizeof(short));
    }

    public void WriteUInt16(ushort value) => WriteInt16((short)value);

    public void WriteInt32(int value)
    {
        Span<byte> span = output.GetSpan(sizeof(int));
        if (ByteOrder == ByteOrder.LittleEndian)
        {
            BinaryPrimitives.WriteInt32LittleEndian(span, value);
        }
        else
        {
            BinaryPrimitives.WriteInt32BigEndian(span, value);
        }
        output.Advance(sizeof(int));
    }

    public void WriteUInt32(uint value) => WriteInt32((int)value);

    public void WriteInt64(long value)
    {
        Span<byte> span = output.GetSpan(sizeof(long));
        if (ByteOrder == ByteOrder.LittleEndian)
        {
            BinaryPrimitives.WriteInt64LittleEndian(span, value);
        }
        else
        {
            BinaryPrimitives.WriteInt64BigEndian(span, value);
        }
        output.Advance(sizeof(long));
    }

    public void WriteUInt64(ulong value) => WriteInt64((long)value);

    public void WriteSingle(float value) => WriteInt32(BitConverter.SingleToInt32Bits(value));

    public void WriteDouble(double value) => WriteInt64(BitConverter.DoubleToInt64Bits(value));

    // ---- varints --------------------------------------------------------------------------------------------------

    /// <summary>Unsigned 7-bit encoded int, see <see cref="VarInt"/>.</summary>
    public void WriteVarUInt32(uint value) => WriteVarUInt64(value);

    /// <summary>Unsigned 7-bit encoded long, see <see cref="VarInt"/>.</summary>
    public void WriteVarUInt64(ulong value)
    {
        output.Advance(VarInt.Write(output.GetSpan(VarInt.MaxLength64), value));
    }

    /// <summary>Signed (ZigZag) 7-bit encoded int, see <see cref="VarInt"/>.</summary>
    public void WriteVarInt32(int value) => WriteVarUInt64(VarInt.ZigZagEncode(value));

    /// <summary>Signed (ZigZag) 7-bit encoded long, see <see cref="VarInt"/>.</summary>
    public void WriteVarInt64(long value) => WriteVarUInt64(VarInt.ZigZagEncode(value));

    // ---- byte blocks ----------------------------------------------------------------------------------------------

    /// <summary>Raw bytes, no length.</summary>
    public void WriteBytes(ReadOnlySpan<byte> value) => output.Write(value);

    /// <summary>Varint length followed by the bytes.</summary>
    public void WriteLengthPrefixedBytes(ReadOnlySpan<byte> value)
    {
        WriteVarUInt32((uint)value.Length);
        output.Write(value);
    }

    /// <summary>Varint byte length followed by the encoded text (UTF-8 by default).</summary>
    public void WriteLengthPrefixedString(string value, Encoding? encoding = null)
    {
        encoding ??= Encoding.UTF8;
        int length = encoding.GetByteCount(value);
        WriteVarUInt32((uint)length);
        output.Advance(encoding.GetBytes(value, output.GetSpan(length)));
    }
}
