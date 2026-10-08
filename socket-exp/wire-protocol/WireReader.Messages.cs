using System;
using System.Buffers;
using System.IO.Pipelines;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;

namespace WireProtocol;

public sealed partial class WireReader
{
    /// <summary>
    /// Largest message <see cref="ReadMessageAsync{T}"/> waits for; a message still incomplete with this many bytes
    /// buffered is a <see cref="ProtocolException"/>. Keep it at most the pipe's pause threshold (Kestrel: 1 MB by
    /// default), otherwise the pipe stops filling and the read waits until cancelled.
    /// </summary>
    public long MaxMessageSize { get; set; } = 1024 * 1024;

    /// <summary>
    /// Reads one complete message: runs <paramref name="parser"/> over the buffered bytes and, if it throws
    /// <see cref="IncompleteMessageError"/>, waits for more bytes and runs it again from the start.
    /// </summary>
    /// <exception cref="ProtocolException">Malformed message, or the connection closed before it was complete.</exception>
    public ValueTask<T> ReadMessageAsync<T>(MessageParser<T> parser, CancellationToken cancellationToken = default)
        => ReadMessageAsync(static (ref MessageReader reader, MessageParser<T> p) => p(ref reader), parser, cancellationToken);

    /// <inheritdoc cref="ReadMessageAsync{T}"/>
    [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder<>))]
    public async ValueTask<T> ReadMessageAsync<TState, T>(MessageParser<TState, T> parser, TState state,
                                                         CancellationToken cancellationToken = default)
    {
        (_, T value) = await ReadMessageCoreAsync(parser, state, allowEnd: false, cancellationToken);
        return value;
    }

    /// <summary>
    /// Like <see cref="ReadMessageAsync{T}"/>, but returns <c>default</c> (null for classes) if the peer closed the
    /// connection cleanly before the first byte of the message, i.e. at a message boundary.
    /// </summary>
    public ValueTask<T?> ReadMessageOrDefaultAsync<T>(MessageParser<T> parser, CancellationToken cancellationToken = default)
        => ReadMessageOrDefaultAsync(static (ref MessageReader reader, MessageParser<T> p) => p(ref reader), parser, cancellationToken);

    /// <inheritdoc cref="ReadMessageOrDefaultAsync{T}"/>
    [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder<>))]
    public async ValueTask<T?> ReadMessageOrDefaultAsync<TState, T>(MessageParser<TState, T> parser, TState state,
                                                                   CancellationToken cancellationToken = default)
    {
        (bool ended, T value) = await ReadMessageCoreAsync(parser, state, allowEnd: true, cancellationToken);
        return ended ? default : value;
    }

    [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder<>))]
    private async ValueTask<(bool Ended, T Value)> ReadMessageCoreAsync<TState, T>(MessageParser<TState, T> parser, TState state,
                                                                                 bool allowEnd, CancellationToken cancellationToken)
    {
        CancellationToken ct = Token(cancellationToken);
        while (true)
        {
            ReadResult result = await input.ReadAsync(ct);
            ReadOnlySequence<byte> buffer = result.Buffer;
            if (buffer.IsEmpty && result.IsCompleted && allowEnd)
            {
                input.AdvanceTo(buffer.End);
                return (true, default!);
            }
            if (!buffer.IsEmpty)
            {
                bool complete;
                T value;
                SequencePosition consumed;
                try
                {
                    complete = TryParseMessage(buffer, parser, state, ByteOrder, out value, out consumed);
                }
                catch
                {
                    input.AdvanceTo(buffer.Start);   // keep the reader usable for the caller's cleanup
                    throw;
                }
                if (complete)
                {
                    input.AdvanceTo(consumed);
                    return (false, value);
                }
                if (buffer.Length >= MaxMessageSize)
                {
                    input.AdvanceTo(buffer.Start);
                    throw new ProtocolException($"message exceeds {MaxMessageSize} bytes");
                }
            }
            input.AdvanceTo(buffer.Start, buffer.End);   // consumed nothing, examined all: wait for more data
            ThrowIfEnded(result);
        }
    }

    // Synchronous, so MessageReader (a ref struct) can live here; the only place IncompleteMessageError is caught.
    private static bool TryParseMessage<TState, T>(ReadOnlySequence<byte> buffer, MessageParser<TState, T> parser, TState state,
                                                   ByteOrder byteOrder, out T value, out SequencePosition consumed)
    {
        var reader = new MessageReader(buffer, byteOrder);
        try
        {
            value = parser(ref reader, state);
        }
        catch (IncompleteMessageError)
        {
            value = default!;
            consumed = default;
            return false;
        }
        consumed = reader.Position;
        return true;
    }
}
