using System;
using System.Buffers;
using System.IO.Pipelines;
using System.Runtime.CompilerServices;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace WireProtocol;

/// <summary>Decodes a value from the start of <paramref name="reader"/>; false if more bytes are needed.</summary>
public delegate bool SequenceDecoder<TState, T>(ref SequenceReader<byte> reader, TState state, out T value);

/// <summary>
/// Pull-style protocol reader over a <see cref="PipeReader"/>: each read waits until enough bytes are buffered,
/// decodes them and consumes them, so protocols are written as plain sequential code.
/// </summary>
/// <remarks>
/// <para>Errors: data that cannot be decoded, or a connection closed in the middle of a value, throw
/// <see cref="ProtocolException"/>. Use <see cref="AtEndAsync"/> at message boundaries to detect a clean close.</para>
/// <para>Performance: when the bytes are already buffered every read completes synchronously without allocating;
/// the cost is a few bookkeeping calls per field. For hot paths, <see cref="DecodeAsync{TState,T}"/> parses a
/// whole message synchronously from the buffer in a single read.</para>
/// <para>Fixed-size reads (<see cref="ReadAsync{T}"/>, <see cref="DecodeAsync{TState,T}"/>) keep the whole value
/// buffered until it is complete, so the value must be smaller than the pipe's pause threshold (Kestrel: 1 MB by
/// default). <see cref="ReadExactlyAsync"/>, <see cref="ReadBytesAsync"/> and <see cref="SkipAsync"/> consume
/// incrementally and have no such limit. Not thread-safe: one reader per connection, one read at a time.</para>
/// </remarks>
public sealed class WireReader
{
    /// <summary>UTF-8 that rejects invalid byte sequences instead of replacing them.</summary>
    public static readonly Encoding StrictUtf8 = new UTF8Encoding(encoderShouldEmitUTF8Identifier: false, throwOnInvalidBytes: true);

    private readonly PipeReader input;
    private readonly CancellationToken defaultCancellation;

    /// <param name="input">Source of the data.</param>
    /// <param name="byteOrder">Byte order of fixed-size numbers.</param>
    /// <param name="cancellationToken">Used by every read that is not given its own token.</param>
    public WireReader(PipeReader input, ByteOrder byteOrder = ByteOrder.LittleEndian, CancellationToken cancellationToken = default)
    {
        this.input = input;
        ByteOrder = byteOrder;
        defaultCancellation = cancellationToken;
    }

    public ByteOrder ByteOrder { get; set; }

    public PipeReader Input => input;

    // ---- core ---------------------------------------------------------------------------------------------------

    /// <summary>
    /// Waits until <paramref name="decoder"/> succeeds on the buffered bytes and consumes what it read. The decoder
    /// is re-run from the same start whenever more bytes arrive, so it must not have side effects.
    /// </summary>
    [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder<>))]
    public async ValueTask<T> DecodeAsync<TState, T>(SequenceDecoder<TState, T> decoder, TState state,
                                                    CancellationToken cancellationToken = default)
    {
        CancellationToken ct = Token(cancellationToken);
        while (true)
        {
            ReadResult result = await input.ReadAsync(ct);
            ReadOnlySequence<byte> buffer = result.Buffer;
            bool done;
            T value;
            SequencePosition consumed;
            try
            {
                done = TryDecode(buffer, decoder, state, out value, out consumed);
            }
            catch
            {
                input.AdvanceTo(buffer.Start);   // keep the reader usable for the caller's cleanup
                throw;
            }
            if (done)
            {
                input.AdvanceTo(consumed);
                return value;
            }
            input.AdvanceTo(buffer.Start, buffer.End);   // consumed nothing, examined all: wait for more data
            ThrowIfEnded(result);
        }
    }

    /// <summary>Waits for <paramref name="size"/> bytes, decodes them with <paramref name="decode"/> and consumes them.</summary>
    /// <remarks><paramref name="decode"/> must not keep the sequence: its memory is released when this returns.</remarks>
    public ValueTask<T> ReadAsync<TState, T>(int size, TState state, Func<ReadOnlySequence<byte>, TState, T> decode,
                                            CancellationToken cancellationToken = default)
    {
        ArgumentOutOfRangeException.ThrowIfNegative(size);
        return DecodeAsync(
            static (ref SequenceReader<byte> reader, (int Size, TState State, Func<ReadOnlySequence<byte>, TState, T> Decode) s, out T value) =>
            {
                if (reader.Remaining < s.Size)
                {
                    value = default!;
                    return false;
                }
                value = s.Decode(reader.UnreadSequence.Slice(0, s.Size), s.State);
                reader.Advance(s.Size);
                return true;
            },
            (size, state, decode), cancellationToken);
    }

    /// <inheritdoc cref="ReadAsync{TState,T}"/>
    public ValueTask<T> ReadAsync<T>(int size, Func<ReadOnlySequence<byte>, T> decode, CancellationToken cancellationToken = default)
        => ReadAsync(size, decode, static (data, d) => d(data), cancellationToken);

    /// <summary>Waits until at least one byte is buffered or the peer closed the connection; consumes nothing.</summary>
    /// <returns>True if the connection was closed with no unread data, i.e. cleanly at a message boundary.</returns>
    [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder<>))]
    public async ValueTask<bool> AtEndAsync(CancellationToken cancellationToken = default)
    {
        CancellationToken ct = Token(cancellationToken);
        while (true)
        {
            ReadResult result = await input.ReadAsync(ct);
            ReadOnlySequence<byte> buffer = result.Buffer;
            if (!buffer.IsEmpty || result.IsCompleted)
            {
                input.AdvanceTo(buffer.Start);   // the data stays for the next read
                return buffer.IsEmpty;
            }
            input.AdvanceTo(buffer.Start, buffer.End);
            if (result.IsCanceled)
            {
                throw new OperationCanceledException("read was canceled");
            }
        }
    }

    // ---- fixed size -----------------------------------------------------------------------------------------------

    public ValueTask<byte> ReadByteAsync(CancellationToken cancellationToken = default)
        => ReadAsync(sizeof(byte), static data => FirstByte(data), cancellationToken);

    public ValueTask<sbyte> ReadSByteAsync(CancellationToken cancellationToken = default)
        => ReadAsync(sizeof(sbyte), static data => (sbyte)FirstByte(data), cancellationToken);

    /// <summary>One byte: 0 is false, 1 is true, anything else is a <see cref="ProtocolException"/>.</summary>
    public ValueTask<bool> ReadBooleanAsync(CancellationToken cancellationToken = default)
        => ReadAsync(sizeof(byte), static data => FirstByte(data) switch
        {
            0 => false,
            1 => true,
            byte b => throw new ProtocolException($"invalid boolean 0x{b:X2}"),
        }, cancellationToken);

    public ValueTask<short> ReadInt16Async(CancellationToken cancellationToken = default)
        => ReadAsync(sizeof(short), ByteOrder, static (data, order) => Int16(data, order), cancellationToken);

    public ValueTask<ushort> ReadUInt16Async(CancellationToken cancellationToken = default)
        => ReadAsync(sizeof(ushort), ByteOrder, static (data, order) => (ushort)Int16(data, order), cancellationToken);

    public ValueTask<int> ReadInt32Async(CancellationToken cancellationToken = default)
        => ReadAsync(sizeof(int), ByteOrder, static (data, order) => Int32(data, order), cancellationToken);

    public ValueTask<uint> ReadUInt32Async(CancellationToken cancellationToken = default)
        => ReadAsync(sizeof(uint), ByteOrder, static (data, order) => (uint)Int32(data, order), cancellationToken);

    public ValueTask<long> ReadInt64Async(CancellationToken cancellationToken = default)
        => ReadAsync(sizeof(long), ByteOrder, static (data, order) => Int64(data, order), cancellationToken);

    public ValueTask<ulong> ReadUInt64Async(CancellationToken cancellationToken = default)
        => ReadAsync(sizeof(ulong), ByteOrder, static (data, order) => (ulong)Int64(data, order), cancellationToken);

    public ValueTask<float> ReadSingleAsync(CancellationToken cancellationToken = default)
        => ReadAsync(sizeof(float), ByteOrder, static (data, order) => BitConverter.Int32BitsToSingle(Int32(data, order)), cancellationToken);

    public ValueTask<double> ReadDoubleAsync(CancellationToken cancellationToken = default)
        => ReadAsync(sizeof(double), ByteOrder, static (data, order) => BitConverter.Int64BitsToDouble(Int64(data, order)), cancellationToken);

    // ---- varints --------------------------------------------------------------------------------------------------

    /// <summary>Unsigned 7-bit encoded int, see <see cref="VarInt"/>.</summary>
    public ValueTask<uint> ReadVarUInt32Async(CancellationToken cancellationToken = default)
        => DecodeAsync(static (ref SequenceReader<byte> reader, int bits, out uint value) =>
        {
            bool ok = VarInt.TryRead(ref reader, bits, out ulong v);
            value = (uint)v;
            return ok;
        }, 32, cancellationToken);

    /// <summary>Unsigned 7-bit encoded long, see <see cref="VarInt"/>.</summary>
    public ValueTask<ulong> ReadVarUInt64Async(CancellationToken cancellationToken = default)
        => DecodeAsync(static (ref SequenceReader<byte> reader, int bits, out ulong value) =>
            VarInt.TryRead(ref reader, bits, out value), 64, cancellationToken);

    /// <summary>Signed (ZigZag) 7-bit encoded int, see <see cref="VarInt"/>.</summary>
    public ValueTask<int> ReadVarInt32Async(CancellationToken cancellationToken = default)
        => DecodeAsync(static (ref SequenceReader<byte> reader, int bits, out int value) =>
        {
            bool ok = VarInt.TryRead(ref reader, bits, out ulong v);
            value = VarInt.ZigZagDecode((uint)v);
            return ok;
        }, 32, cancellationToken);

    /// <summary>Signed (ZigZag) 7-bit encoded long, see <see cref="VarInt"/>.</summary>
    public ValueTask<long> ReadVarInt64Async(CancellationToken cancellationToken = default)
        => DecodeAsync(static (ref SequenceReader<byte> reader, int bits, out long value) =>
        {
            bool ok = VarInt.TryRead(ref reader, bits, out ulong v);
            value = VarInt.ZigZagDecode(v);
            return ok;
        }, 64, cancellationToken);

    // ---- byte blocks ----------------------------------------------------------------------------------------------

    /// <summary>Fills <paramref name="destination"/>, consuming bytes as they arrive.</summary>
    [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder))]
    public async ValueTask ReadExactlyAsync(Memory<byte> destination, CancellationToken cancellationToken = default)
    {
        CancellationToken ct = Token(cancellationToken);
        while (!destination.IsEmpty)
        {
            ReadResult result = await input.ReadAsync(ct);
            ReadOnlySequence<byte> chunk = result.Buffer.Slice(0, Math.Min(result.Buffer.Length, destination.Length));
            chunk.CopyTo(destination.Span);
            destination = destination[(int)chunk.Length..];
            input.AdvanceTo(chunk.End);
            if (!destination.IsEmpty)
            {
                ThrowIfEnded(result);
            }
        }
    }

    /// <summary>Discards <paramref name="count"/> bytes, consuming them as they arrive.</summary>
    [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder))]
    public async ValueTask SkipAsync(long count, CancellationToken cancellationToken = default)
    {
        ArgumentOutOfRangeException.ThrowIfNegative(count);
        CancellationToken ct = Token(cancellationToken);
        while (count > 0)
        {
            ReadResult result = await input.ReadAsync(ct);
            ReadOnlySequence<byte> chunk = result.Buffer.Slice(0, Math.Min(result.Buffer.Length, count));
            count -= chunk.Length;
            input.AdvanceTo(chunk.End);
            if (count > 0)
            {
                ThrowIfEnded(result);
            }
        }
    }

    /// <summary>Exactly <paramref name="count"/> bytes.</summary>
    [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder<>))]
    public async ValueTask<byte[]> ReadBytesAsync(int count, CancellationToken cancellationToken = default)
    {
        ArgumentOutOfRangeException.ThrowIfNegative(count);
        if (count == 0)
        {
            return Array.Empty<byte>();
        }
        byte[] bytes = new byte[count];
        await ReadExactlyAsync(bytes, cancellationToken);
        return bytes;
    }

    /// <summary>Varint length (at most <paramref name="maxLength"/>) followed by that many bytes.</summary>
    /// <param name="maxLength">Limit on the length the peer may declare; a longer one is a <see cref="ProtocolException"/>.</param>
    /// <param name="what">Name of the field for error messages.</param>
    /// <param name="cancellationToken">Overrides the reader's default token.</param>
    [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder<>))]
    public async ValueTask<byte[]> ReadLengthPrefixedBytesAsync(int maxLength, string what = "byte string",
                                                                CancellationToken cancellationToken = default)
    {
        int length = await ReadLengthAsync(maxLength, what, cancellationToken);
        return await ReadBytesAsync(length, cancellationToken);
    }

    /// <summary>Varint byte length (at most <paramref name="maxLength"/>) followed by that many bytes of text.</summary>
    /// <param name="maxLength">Limit on the length in bytes the peer may declare.</param>
    /// <param name="what">Name of the field for error messages.</param>
    /// <param name="encoding">Defaults to <see cref="StrictUtf8"/>; invalid bytes are a <see cref="ProtocolException"/>.</param>
    /// <param name="cancellationToken">Overrides the reader's default token.</param>
    [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder<>))]
    public async ValueTask<string> ReadLengthPrefixedStringAsync(int maxLength, string what = "string", Encoding? encoding = null,
                                                                 CancellationToken cancellationToken = default)
    {
        byte[] bytes = await ReadLengthPrefixedBytesAsync(maxLength, what, cancellationToken);
        try
        {
            return (encoding ?? StrictUtf8).GetString(bytes);
        }
        catch (DecoderFallbackException e)
        {
            throw new ProtocolException($"{what} is not valid {(encoding ?? StrictUtf8).WebName}", e);
        }
    }

    /// <summary>Varint length, checked against <paramref name="maxLength"/>.</summary>
    [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder<>))]
    public async ValueTask<int> ReadLengthAsync(int maxLength, string what = "length", CancellationToken cancellationToken = default)
    {
        uint length = await ReadVarUInt32Async(cancellationToken);
        if (length > maxLength)
        {
            throw new ProtocolException($"{what} too long: {length} > {maxLength}");
        }
        return (int)length;
    }

    // ---- helpers --------------------------------------------------------------------------------------------------

    private CancellationToken Token(CancellationToken cancellationToken)
        => cancellationToken.CanBeCanceled ? cancellationToken : defaultCancellation;

    // Separate from the async method: SequenceReader is a ref struct.
    private static bool TryDecode<TState, T>(ReadOnlySequence<byte> buffer, SequenceDecoder<TState, T> decoder, TState state,
                                             out T value, out SequencePosition consumed)
    {
        var reader = new SequenceReader<byte>(buffer);
        bool done = decoder(ref reader, state, out value);
        consumed = reader.Position;
        return done;
    }

    private static void ThrowIfEnded(ReadResult result)
    {
        if (result.IsCanceled)
        {
            throw new OperationCanceledException("read was canceled");
        }
        if (result.IsCompleted)
        {
            throw new ProtocolException("connection closed mid-message");
        }
    }

    private static byte FirstByte(ReadOnlySequence<byte> data)
    {
        var reader = new SequenceReader<byte>(data);
        reader.TryRead(out byte value);
        return value;
    }

    private static short Int16(ReadOnlySequence<byte> data, ByteOrder order)
    {
        var reader = new SequenceReader<byte>(data);
        short value;
        _ = order == ByteOrder.LittleEndian ? reader.TryReadLittleEndian(out value) : reader.TryReadBigEndian(out value);
        return value;
    }

    private static int Int32(ReadOnlySequence<byte> data, ByteOrder order)
    {
        var reader = new SequenceReader<byte>(data);
        int value;
        _ = order == ByteOrder.LittleEndian ? reader.TryReadLittleEndian(out value) : reader.TryReadBigEndian(out value);
        return value;
    }

    private static long Int64(ReadOnlySequence<byte> data, ByteOrder order)
    {
        var reader = new SequenceReader<byte>(data);
        long value;
        _ = order == ByteOrder.LittleEndian ? reader.TryReadLittleEndian(out value) : reader.TryReadBigEndian(out value);
        return value;
    }
}
