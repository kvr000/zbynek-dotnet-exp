using System;
using System.Buffers;
using System.IO.Pipelines;
using System.Threading.Tasks;
using Xunit;

namespace WireProtocol.Tests;

public class MessageReaderTests
{
    private sealed record Sample(string Name, long Time, short[] Values, byte[] Signature);

    private static Sample ParseSample(ref MessageReader r)
    {
        string name = r.ReadLengthPrefixedString(32, "name");
        long time = r.ReadInt64();
        int count = r.ReadLength(100, "count");
        r.Require(count * sizeof(short));
        var values = new short[count];
        for (int i = 0; i < count; i++)
        {
            values[i] = r.ReadInt16();
        }
        byte[] signature = r.ReadLengthPrefixedBytes(64, "signature");
        return new Sample(name, time, values, signature);
    }

    private static byte[] EncodeSample(string name, long time, short[] values, byte[] signature, ByteOrder order = ByteOrder.LittleEndian)
    {
        var buffer = new ArrayBufferWriter<byte>();
        var w = new WireWriter(buffer, order);
        w.WriteLengthPrefixedString(name);
        w.WriteInt64(time);
        w.WriteVarUInt32((uint)values.Length);
        foreach (short v in values)
        {
            w.WriteInt16(v);
        }
        w.WriteLengthPrefixedBytes(signature);
        return buffer.WrittenSpan.ToArray();
    }

    private static WireReader Reader(byte[] data, int chunk, PipeOptions? options = null)
    {
        var pipe = new Pipe(options ?? PipeOptions.Default);
        _ = Task.Run(async () =>
        {
            int step = chunk <= 0 ? Math.Max(data.Length, 1) : chunk;
            for (int i = 0; i < data.Length; i += step)
            {
                await pipe.Writer.WriteAsync(data.AsMemory(i, Math.Min(step, data.Length - i)));
            }
            await pipe.Writer.CompleteAsync();
        });
        return new WireReader(pipe.Reader);
    }

    [Theory]
    [InlineData(0)]
    [InlineData(1)]
    [InlineData(5)]
    public async Task ParsesMessagesBackToBackHoweverSplit(int chunk)
    {
        byte[] one = EncodeSample("sensor-001", 1791479386472, new short[] { 215, -40, 1000 }, new byte[32]);
        byte[] two = EncodeSample("x", -1, Array.Empty<short>(), new byte[] { 7 });
        WireReader r = Reader([.. one, .. two], chunk);

        Sample a = await r.ReadMessageAsync(ParseSample);
        Assert.Equal("sensor-001", a.Name);
        Assert.Equal(1791479386472, a.Time);
        Assert.Equal(new short[] { 215, -40, 1000 }, a.Values);
        Assert.Equal(32, a.Signature.Length);

        Sample? b = await r.ReadMessageOrDefaultAsync(ParseSample);
        Assert.Equal("x", b!.Name);
        Assert.Equal(new byte[] { 7 }, b.Signature);

        Assert.Null(await r.ReadMessageOrDefaultAsync(ParseSample));   // clean close at the boundary
    }

    [Theory]
    [InlineData(0)]
    [InlineData(1)]
    public async Task ThrowsWhenClosedMidMessage(int chunk)
    {
        byte[] data = EncodeSample("sensor-001", 1, new short[] { 1, 2 }, new byte[32]);
        WireReader r = Reader(data[..^3], chunk);
        await Assert.ThrowsAsync<ProtocolException>(async () => await r.ReadMessageOrDefaultAsync(ParseSample));
    }

    [Fact]
    public async Task ReadMessageThrowsOnCleanCloseWhenMessageRequired()
    {
        await Assert.ThrowsAsync<ProtocolException>(async () => await Reader(Array.Empty<byte>(), 0).ReadMessageAsync(ParseSample));
    }

    [Fact]
    public async Task MalformedDataIsProtocolException()
    {
        byte[] data = EncodeSample("sensor-001", 1, new short[101], new byte[1]);   // count above limit
        ProtocolException e = await Assert.ThrowsAsync<ProtocolException>(async () => await Reader(data, 0).ReadMessageAsync(ParseSample));
        Assert.Contains("count", e.Message);
    }

    [Fact]
    public async Task RejectsMessageLargerThanLimit()
    {
        WireReader r = Reader(new byte[] { 0x80, 0x80, 0x80, 0x80 }, 1);   // varint never finishes
        r.MaxMessageSize = 3;
        await Assert.ThrowsAsync<ProtocolException>(async () => await r.ReadMessageAsync(static (ref MessageReader m) => m.ReadVarUInt64()));
    }

    [Fact]
    public async Task PassesStateAndByteOrder()
    {
        byte[] data = EncodeSample("be", 0x0102030405060708, new short[] { 0x1234 }, Array.Empty<byte>(), ByteOrder.BigEndian);
        WireReader r = Reader(data, 3);
        r.ByteOrder = ByteOrder.BigEndian;
        (Sample s, string tag) = await r.ReadMessageAsync(static (ref MessageReader m, string t) => (ParseSample(ref m), t), "state");
        Assert.Equal(0x0102030405060708, s.Time);
        Assert.Equal(new short[] { 0x1234 }, s.Values);
        Assert.Equal("state", tag);
    }

    [Fact]
    public async Task MixesWithFieldReads()
    {
        var buffer = new ArrayBufferWriter<byte>();
        var w = new WireWriter(buffer);
        w.WriteByte(0x42);
        w.WriteBytes(EncodeSample("mix", 5, new short[] { 9 }, new byte[] { 1, 2 }));
        w.WriteInt32(-7);
        WireReader r = Reader(buffer.WrittenSpan.ToArray(), 2);
        Assert.Equal(0x42, await r.ReadByteAsync());
        Assert.Equal("mix", (await r.ReadMessageAsync(ParseSample)).Name);
        Assert.Equal(-7, await r.ReadInt32Async());
        Assert.True(await r.AtEndAsync());
    }

    [Fact]
    public async Task SliceAndSkipWork()
    {
        WireReader r = Reader(new byte[] { 1, 2, 3, 4, 5, 6 }, 1);
        int sum = await r.ReadMessageAsync(static (ref MessageReader m) =>
        {
            m.Skip(1);
            ReadOnlySequence<byte> slice = m.ReadSlice(3);
            Span<byte> rest = stackalloc byte[2];
            m.ReadBytes(rest);
            return slice.FirstSpan[0] + (int)slice.Length + rest[1];
        });
        Assert.Equal(2 + 3 + 6, sum);
    }
}
