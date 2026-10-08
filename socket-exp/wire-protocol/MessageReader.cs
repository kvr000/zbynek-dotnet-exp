using System;
using System.Buffers;
using System.Text;

namespace WireProtocol;

/// <summary>Parses one complete message from <paramref name="reader"/>, see <see cref="WireReader.ReadMessageAsync{T}"/>.</summary>
public delegate T MessageParser<T>(ref MessageReader reader);

/// <summary>Parses one complete message from <paramref name="reader"/>, with caller-supplied state.</summary>
public delegate T MessageParser<TState, T>(ref MessageReader reader, TState state);

/// <summary>
/// Synchronous reader over the bytes buffered so far, for parsing a whole message in one pass. Every read either
/// returns the value or throws <see cref="IncompleteMessageError"/> when the buffer ends first; the async wrapper
/// (<see cref="WireReader.ReadMessageAsync{T}"/>) then waits for more bytes and runs the parser again from the start.
/// Malformed data throws <see cref="ProtocolException"/>.
/// </summary>
/// <remarks>
/// Optimised for the common case of the whole message being buffered already: parsers are plain straight-line code,
/// and the exception (several microseconds) is only paid when a message is split across network reads.
/// Parsers must therefore be free of side effects, since a run may be abandoned and repeated.
/// </remarks>
public ref struct MessageReader
{
    private SequenceReader<byte> reader;

    public MessageReader(ReadOnlySequence<byte> buffer, ByteOrder byteOrder = ByteOrder.LittleEndian)
    {
        reader = new SequenceReader<byte>(buffer);
        ByteOrder = byteOrder;
    }

    /// <summary>Byte order of fixed-size numbers; may be switched mid-message.</summary>
    public ByteOrder ByteOrder { get; set; }

    /// <summary>Bytes read so far.</summary>
    public readonly long Consumed => reader.Consumed;

    /// <summary>Bytes buffered but not read yet (more may still arrive).</summary>
    public readonly long Remaining => reader.Remaining;

    public readonly SequencePosition Position => reader.Position;

    /// <summary>Throws <see cref="IncompleteMessageError"/> unless <paramref name="count"/> more bytes are buffered.
    /// Use it to fail early, e.g. before allocating an array for a declared number of elements.</summary>
    public readonly void Require(long count)
    {
        if (reader.Remaining < count)
        {
            throw IncompleteMessageError.Instance;
        }
    }

    // ---- fixed size -----------------------------------------------------------------------------------------------

    public byte ReadByte() => reader.TryRead(out byte value) ? value : throw IncompleteMessageError.Instance;

    public sbyte ReadSByte() => (sbyte)ReadByte();

    /// <summary>One byte: 0 is false, 1 is true, anything else is a <see cref="ProtocolException"/>.</summary>
    public bool ReadBoolean() => ReadByte() switch
    {
        0 => false,
        1 => true,
        byte b => throw new ProtocolException($"invalid boolean 0x{b:X2}"),
    };

    public short ReadInt16()
    {
        short value;
        if (!(ByteOrder == ByteOrder.LittleEndian ? reader.TryReadLittleEndian(out value) : reader.TryReadBigEndian(out value)))
        {
            throw IncompleteMessageError.Instance;
        }
        return value;
    }

    public ushort ReadUInt16() => (ushort)ReadInt16();

    public int ReadInt32()
    {
        int value;
        if (!(ByteOrder == ByteOrder.LittleEndian ? reader.TryReadLittleEndian(out value) : reader.TryReadBigEndian(out value)))
        {
            throw IncompleteMessageError.Instance;
        }
        return value;
    }

    public uint ReadUInt32() => (uint)ReadInt32();

    public long ReadInt64()
    {
        long value;
        if (!(ByteOrder == ByteOrder.LittleEndian ? reader.TryReadLittleEndian(out value) : reader.TryReadBigEndian(out value)))
        {
            throw IncompleteMessageError.Instance;
        }
        return value;
    }

    public ulong ReadUInt64() => (ulong)ReadInt64();

    public float ReadSingle() => BitConverter.Int32BitsToSingle(ReadInt32());

    public double ReadDouble() => BitConverter.Int64BitsToDouble(ReadInt64());

    // ---- varints --------------------------------------------------------------------------------------------------

    /// <summary>Unsigned 7-bit encoded int, see <see cref="VarInt"/>.</summary>
    public uint ReadVarUInt32() => VarInt.TryRead(ref reader, 32, out ulong value) ? (uint)value : throw IncompleteMessageError.Instance;

    /// <summary>Unsigned 7-bit encoded long, see <see cref="VarInt"/>.</summary>
    public ulong ReadVarUInt64() => VarInt.TryRead(ref reader, 64, out ulong value) ? value : throw IncompleteMessageError.Instance;

    /// <summary>Signed (ZigZag) 7-bit encoded int, see <see cref="VarInt"/>.</summary>
    public int ReadVarInt32() => VarInt.ZigZagDecode(ReadVarUInt32());

    /// <summary>Signed (ZigZag) 7-bit encoded long, see <see cref="VarInt"/>.</summary>
    public long ReadVarInt64() => VarInt.ZigZagDecode(ReadVarUInt64());

    // ---- byte blocks ----------------------------------------------------------------------------------------------

    /// <summary>Exactly <paramref name="count"/> bytes, copied.</summary>
    public byte[] ReadBytes(int count)
    {
        ArgumentOutOfRangeException.ThrowIfNegative(count);
        Require(count);
        if (count == 0)
        {
            return Array.Empty<byte>();
        }
        byte[] bytes = new byte[count];
        reader.TryCopyTo(bytes);
        reader.Advance(count);
        return bytes;
    }

    /// <summary>Fills <paramref name="destination"/>.</summary>
    public void ReadBytes(scoped Span<byte> destination)
    {
        Require(destination.Length);
        reader.TryCopyTo(destination);
        reader.Advance(destination.Length);
    }

    /// <summary>
    /// Exactly <paramref name="count"/> bytes without copying. Valid only until the parser returns: the memory
    /// belongs to the pipe and is released afterwards.
    /// </summary>
    public ReadOnlySequence<byte> ReadSlice(long count)
    {
        Require(count);
        ReadOnlySequence<byte> slice = reader.UnreadSequence.Slice(0, count);
        reader.Advance(count);
        return slice;
    }

    public void Skip(long count)
    {
        Require(count);
        reader.Advance(count);
    }

    /// <summary>Varint length, checked against <paramref name="maxLength"/>.</summary>
    public int ReadLength(int maxLength, string what = "length")
    {
        uint length = ReadVarUInt32();
        if (length > maxLength)
        {
            throw new ProtocolException($"{what} too long: {length} > {maxLength}");
        }
        return (int)length;
    }

    /// <summary>Varint length (at most <paramref name="maxLength"/>) followed by that many bytes.</summary>
    public byte[] ReadLengthPrefixedBytes(int maxLength, string what = "byte string") => ReadBytes(ReadLength(maxLength, what));

    /// <summary>Varint byte length (at most <paramref name="maxLength"/>) followed by that many bytes of text,
    /// <see cref="WireReader.StrictUtf8"/> by default.</summary>
    public string ReadLengthPrefixedString(int maxLength, string what = "string", Encoding? encoding = null)
    {
        int length = ReadLength(maxLength, what);
        Require(length);
        encoding ??= WireReader.StrictUtf8;
        try
        {
            string value = encoding.GetString(reader.UnreadSequence.Slice(0, length));
            reader.Advance(length);
            return value;
        }
        catch (DecoderFallbackException e)
        {
            throw new ProtocolException($"{what} is not valid {encoding.WebName}", e);
        }
    }
}

/// <summary>
/// Not a failure: the buffered bytes end before the message does. Thrown by <see cref="MessageReader"/> and caught
/// by <see cref="WireReader.ReadMessageAsync{T}"/>, which waits for more data and parses the message again.
/// Parsers let it propagate. A single shared instance is thrown, it carries no state.
/// </summary>
public sealed class IncompleteMessageError : Exception
{
    internal static readonly IncompleteMessageError Instance = new();

    private IncompleteMessageError() : base("incomplete message")
    {
    }
}
