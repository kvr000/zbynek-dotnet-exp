using System;
using System.Buffers;
using System.IO.Pipelines;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Xunit;

namespace WireProtocol.Tests;

public class WireProtocolTests
{
    /// <summary>Feeds <paramref name="data"/> into a pipe, all at once or one byte per flush, then completes it.</summary>
    private static WireReader Reader(byte[] data, bool trickle, ByteOrder order = ByteOrder.LittleEndian, PipeOptions? options = null)
    {
        var pipe = new Pipe(options ?? PipeOptions.Default);
        _ = Task.Run(async () =>
        {
            int step = trickle ? 1 : Math.Max(data.Length, 1);
            for (int i = 0; i < data.Length; i += step)
            {
                await pipe.Writer.WriteAsync(data.AsMemory(i, Math.Min(step, data.Length - i)));
            }
            await pipe.Writer.CompleteAsync();
        });
        return new WireReader(pipe.Reader, order);
    }

    private static byte[] Encode(Action<WireWriter> write, ByteOrder order = ByteOrder.LittleEndian)
    {
        var buffer = new ArrayBufferWriter<byte>();
        write(new WireWriter(buffer, order));
        return buffer.WrittenSpan.ToArray();
    }

    [Theory]
    [InlineData(false, ByteOrder.LittleEndian)]
    [InlineData(true, ByteOrder.LittleEndian)]
    [InlineData(false, ByteOrder.BigEndian)]
    [InlineData(true, ByteOrder.BigEndian)]
    public async Task RoundTripsAllTypes(bool trickle, ByteOrder order)
    {
        byte[] data = Encode(w =>
        {
            w.WriteByte(0xAB);
            w.WriteSByte(-5);
            w.WriteBoolean(true);
            w.WriteInt16(-12345);
            w.WriteUInt16(54321);
            w.WriteInt32(int.MinValue);
            w.WriteUInt32(uint.MaxValue);
            w.WriteInt64(long.MinValue + 7);
            w.WriteUInt64(ulong.MaxValue - 7);
            w.WriteSingle(3.25f);
            w.WriteDouble(-1e300);
            w.WriteVarUInt32(uint.MaxValue);
            w.WriteVarUInt64(ulong.MaxValue);
            w.WriteVarInt32(int.MinValue);
            w.WriteVarInt64(-1);
            w.WriteBytes(new byte[] { 1, 2, 3 });
            w.WriteLengthPrefixedBytes(new byte[] { 9, 8 });
            w.WriteLengthPrefixedString("teplota °C");
        }, order);

        WireReader r = Reader(data, trickle, order);
        Assert.Equal(0xAB, await r.ReadByteAsync());
        Assert.Equal(-5, await r.ReadSByteAsync());
        Assert.True(await r.ReadBooleanAsync());
        Assert.Equal(-12345, await r.ReadInt16Async());
        Assert.Equal(54321, await r.ReadUInt16Async());
        Assert.Equal(int.MinValue, await r.ReadInt32Async());
        Assert.Equal(uint.MaxValue, await r.ReadUInt32Async());
        Assert.Equal(long.MinValue + 7, await r.ReadInt64Async());
        Assert.Equal(ulong.MaxValue - 7, await r.ReadUInt64Async());
        Assert.Equal(3.25f, await r.ReadSingleAsync());
        Assert.Equal(-1e300, await r.ReadDoubleAsync());
        Assert.Equal(uint.MaxValue, await r.ReadVarUInt32Async());
        Assert.Equal(ulong.MaxValue, await r.ReadVarUInt64Async());
        Assert.Equal(int.MinValue, await r.ReadVarInt32Async());
        Assert.Equal(-1L, await r.ReadVarInt64Async());
        Assert.Equal(new byte[] { 1, 2, 3 }, await r.ReadBytesAsync(3));
        Assert.Equal(new byte[] { 9, 8 }, await r.ReadLengthPrefixedBytesAsync(10));
        Assert.Equal("teplota °C", await r.ReadLengthPrefixedStringAsync(100));
        Assert.True(await r.AtEndAsync());
    }

    [Fact]
    public void EncodesKnownBytes()
    {
        Assert.Equal(new byte[] { 0x34, 0x12 }, Encode(w => w.WriteInt16(0x1234)));
        Assert.Equal(new byte[] { 0x12, 0x34 }, Encode(w => w.WriteInt16(0x1234), ByteOrder.BigEndian));
        Assert.Equal(new byte[] { 0xAC, 0x02 }, Encode(w => w.WriteVarUInt32(300)));
        Assert.Equal(new byte[] { 0x7F }, Encode(w => w.WriteVarUInt32(127)));
        Assert.Equal(new byte[] { 0x80, 0x01 }, Encode(w => w.WriteVarUInt32(128)));
        Assert.Equal(new byte[] { 0x01 }, Encode(w => w.WriteVarInt32(-1)));
        Assert.Equal(new byte[] { 0x02 }, Encode(w => w.WriteVarInt32(1)));
        Assert.Equal(new byte[] { 0x03, (byte)'a', (byte)'b', (byte)'c' }, Encode(w => w.WriteLengthPrefixedString("abc")));
    }

    [Theory]
    [InlineData(new byte[] { 0xFF, 0xFF, 0xFF, 0xFF, 0x1F })]         // 5th byte has more than 4 bits
    [InlineData(new byte[] { 0x80, 0x80, 0x80, 0x80, 0x80, 0x00 })]   // 6 bytes
    public async Task RejectsVarUInt32Overflow(byte[] data)
    {
        await Assert.ThrowsAsync<ProtocolException>(async () => await Reader(data, false).ReadVarUInt32Async());
    }

    [Fact]
    public async Task RejectsVarUInt64Overflow()
    {
        byte[] data = Enumerable.Repeat((byte)0xFF, 9).Append((byte)0x02).ToArray();   // 10th byte may only be 0 or 1
        await Assert.ThrowsAsync<ProtocolException>(async () => await Reader(data, false).ReadVarUInt64Async());
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task ThrowsWhenClosedMidValue(bool trickle)
    {
        await Assert.ThrowsAsync<ProtocolException>(async () => await Reader(new byte[] { 1, 2, 3 }, trickle).ReadInt64Async());
        await Assert.ThrowsAsync<ProtocolException>(async () => await Reader(new byte[] { 0x80 }, trickle).ReadVarUInt32Async());
        await Assert.ThrowsAsync<ProtocolException>(async () => await Reader(new byte[] { 5, 1, 2 }, trickle).ReadLengthPrefixedBytesAsync(10));
        await Assert.ThrowsAsync<ProtocolException>(async () => await Reader(Array.Empty<byte>(), trickle).ReadByteAsync());
    }

    [Fact]
    public async Task AtEndDoesNotConsume()
    {
        WireReader r = Reader(new byte[] { 42 }, false);
        Assert.False(await r.AtEndAsync());
        Assert.False(await r.AtEndAsync());
        Assert.Equal(42, await r.ReadByteAsync());
        Assert.True(await r.AtEndAsync());
    }

    [Fact]
    public async Task EnforcesLengthLimitBeforeReadingData()
    {
        byte[] data = Encode(w => w.WriteVarUInt32(1_000_000));   // declares 1 MB, sends nothing
        ProtocolException e = await Assert.ThrowsAsync<ProtocolException>(
            async () => await Reader(data, false).ReadLengthPrefixedStringAsync(128, "deviceId"));
        Assert.Contains("deviceId", e.Message);
    }

    [Fact]
    public async Task RejectsInvalidUtf8()
    {
        byte[] data = { 2, 0xC3, 0x28 };
        await Assert.ThrowsAsync<ProtocolException>(async () => await Reader(data, false).ReadLengthPrefixedStringAsync(10));
    }

    [Fact]
    public async Task RejectsInvalidBoolean()
    {
        await Assert.ThrowsAsync<ProtocolException>(async () => await Reader(new byte[] { 2 }, false).ReadBooleanAsync());
    }

    [Fact]
    public async Task ReadsBlocksLargerThanPipeThreshold()
    {
        byte[] data = Enumerable.Range(0, 100_000).Select(i => (byte)i).ToArray();
        var options = new PipeOptions(pauseWriterThreshold: 4096, resumeWriterThreshold: 2048, minimumSegmentSize: 512);
        WireReader r = Reader(data.Prepend((byte)0).ToArray(), false, options: options);
        await r.SkipAsync(1);
        Assert.Equal(data, await r.ReadBytesAsync(data.Length));
        Assert.True(await r.AtEndAsync());
    }

    [Fact]
    public async Task DecodesWholeMessageSynchronously()
    {
        byte[] data = Encode(w =>
        {
            w.WriteByte((byte)'T');
            w.WriteVarUInt32(2);
            w.WriteInt16(215);
            w.WriteInt16(-40);
        });
        WireReader r = Reader(data, true);
        short[] values = await r.DecodeAsync(static (ref SequenceReader<byte> reader, int max, out short[] value) =>
        {
            value = Array.Empty<short>();
            if (!reader.TryRead(out byte type) || !VarInt.TryRead(ref reader, 32, out ulong count))
            {
                return false;
            }
            if (type != (byte)'T' || count > (ulong)max)
            {
                throw new ProtocolException("bad message");
            }
            if (reader.Remaining < (long)count * sizeof(short))
            {
                return false;
            }
            value = new short[count];
            for (int i = 0; i < value.Length; i++)
            {
                reader.TryReadLittleEndian(out value[i]);
            }
            return true;
        }, 100);
        Assert.Equal(new short[] { 215, -40 }, values);
        Assert.True(await r.AtEndAsync());
    }

    [Fact]
    public async Task HonoursCancellation()
    {
        var pipe = new Pipe();
        using var cts = new CancellationTokenSource(TimeSpan.FromMilliseconds(50));
        var r = new WireReader(pipe.Reader, cancellationToken: cts.Token);
        await Assert.ThrowsAnyAsync<OperationCanceledException>(async () => await r.ReadInt32Async());
    }

    [Fact]
    public async Task PerCallTokenOverridesDefault()
    {
        var pipe = new Pipe();
        var r = new WireReader(pipe.Reader);
        using var cts = new CancellationTokenSource(TimeSpan.FromMilliseconds(50));
        await Assert.ThrowsAnyAsync<OperationCanceledException>(async () => await r.ReadByteAsync(cts.Token));
    }

    [Fact]
    public void ZigZagMapsSmallMagnitudesToSmallNumbers()
    {
        Assert.Equal(0u, VarInt.ZigZagEncode(0));
        Assert.Equal(1u, VarInt.ZigZagEncode(-1));
        Assert.Equal(2u, VarInt.ZigZagEncode(1));
        Assert.Equal(uint.MaxValue, VarInt.ZigZagEncode(int.MinValue));
        Assert.Equal(long.MinValue, VarInt.ZigZagDecode(VarInt.ZigZagEncode(long.MinValue)));
        Assert.Equal(long.MaxValue, VarInt.ZigZagDecode(VarInt.ZigZagEncode(long.MaxValue)));
    }
}
