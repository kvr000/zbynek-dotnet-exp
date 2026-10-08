using System;
using System.Buffers;
using System.Buffers.Binary;
using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;
using System.IO.Pipelines;
using System.Runtime.InteropServices;
using System.Security.Cryptography;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Connections;
using Microsoft.AspNetCore.Connections.Features;
using Microsoft.AspNetCore.Hosting;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

// IoT TCP server. Wire format (all integers little-endian, "bytes" = 7-bit varint length + raw bytes,
// "string" = bytes holding UTF-8):
//
//   Login
//     1. device -> server:  string deviceId
//     2. server -> device:  int64 serverTimeMillis, bytes serverSig
//     3. device -> server:  int64 deviceTimeMillis, bytes deviceSig
//   Temperature data (repeated after login)
//     device -> server:     'T', varint count, count * { int64 timeMillis, int16 temperature }, bytes sig
//
// Signatures are HMAC-SHA256 with the per-device key, chained so each covers the whole history of the session:
//     serverSig = HMAC(K, 'S' || string deviceId || int64 serverTime)
//     deviceSig = HMAC(K, 'D' || serverSig || int64 deviceTime)
//     dataSig_n = HMAC(K, prevSig || 'T' || varint count || samples)      prevSig = deviceSig, then dataSig_n-1
// serverTime is unique per server process, so it doubles as the login challenge: a recorded deviceSig cannot be
// replayed, and chaining prevents replaying, reordering or dropping data messages within a session.
internal static class Program
{
    private const int Port = 5000;

    public static async Task<int> Main(string[] args)
    {
        WebApplicationBuilder builder = WebApplication.CreateSlimBuilder(args);
        builder.Services.Configure<HostOptions>(o => o.ShutdownTimeout = TimeSpan.FromSeconds(20));
        builder.Services.Configure<ConsoleLifetimeOptions>(o => o.SuppressStatusMessages = true);
        builder.Logging.AddFilter("Microsoft.Hosting.Lifetime", LogLevel.Warning);   // hide "Now listening on ..."
        builder.Services.AddSingleton<DeviceKeyStore>();
        builder.Services.AddSingleton<TemperatureSink>();
        builder.WebHost.ConfigureKestrel(o =>
        {
            o.Limits.MaxConcurrentConnections = 10_000;
            o.ListenAnyIP(Port, listen => listen.UseConnectionHandler<IotHandler>());
        });

        await using WebApplication app = builder.Build();

        // WebApplication runs on the Generic Host, whose ConsoleLifetime already handles SIGTERM/SIGINT/SIGQUIT:
        // ctx.Cancel = true + StopApplication() → Kestrel stops accepting, signals ConnectionClosedRequested
        // on every open connection and waits for handlers up to ShutdownTimeout, then aborts the rest.
        // These registrations only record the exit code; every handler for a signal is invoked.
        using var term = PosixSignalRegistration.Create(PosixSignal.SIGTERM, _ => Environment.ExitCode = 143);
        using var intr = PosixSignalRegistration.Create(PosixSignal.SIGINT,  _ => Environment.ExitCode = 130);

        app.Lifetime.ApplicationStarted.Register(() => Console.Error.WriteLine($"Listening on port {Port}..."));
        app.Lifetime.ApplicationStopping.Register(() => Console.Error.WriteLine("Shutting down..."));
        app.Lifetime.ApplicationStopped.Register(() => Console.Error.WriteLine("All connections closed."));

        await app.RunAsync();   // returns after Kestrel and all hosted services stopped

        return Environment.ExitCode;   // 0 = normal stop, 130/143 = stopped by SIGINT/SIGTERM
    }
}

/// <summary>Handles one device connection: login handshake, then a stream of signed temperature messages.</summary>
internal sealed class IotHandler : ConnectionHandler
{
    private const int MaxDeviceIdLength = 128;
    private const int MaxSignatureLength = 64;
    private const int MaxSamplesPerMessage = 4096;
    private const int SampleSize = sizeof(long) + sizeof(short);
    private static readonly TimeSpan LoginTimeout = TimeSpan.FromSeconds(30);
    private static readonly TimeSpan IdleTimeout = TimeSpan.FromMinutes(10);
    private static readonly UTF8Encoding StrictUtf8 = new(encoderShouldEmitUTF8Identifier: false, throwOnInvalidBytes: true);
    private static long lastServerTime;

    private readonly DeviceKeyStore keys;
    private readonly TemperatureSink sink;
    private readonly ILogger<IotHandler> log;

    public IotHandler(DeviceKeyStore keys, TemperatureSink sink, ILogger<IotHandler> log)
    {
        this.keys = keys;
        this.sink = sink;
        this.log = log;
    }

    public override async Task OnConnectedAsync(ConnectionContext connection)
    {
        // Signalled when the server starts a graceful shutdown.
        CancellationToken closing = connection.Features.Get<IConnectionLifetimeNotificationFeature>()
                                        ?.ConnectionClosedRequested ?? CancellationToken.None;
        // Same token plus a timer: login deadline first, then re-armed as idle timeout before every message.
        using var timeout = CancellationTokenSource.CreateLinkedTokenSource(closing);
        timeout.CancelAfter(LoginTimeout);
        PipeReader input = connection.Transport.Input;
        PipeWriter output = connection.Transport.Output;
        string peer = connection.RemoteEndPoint?.ToString() ?? connection.ConnectionId;
        log.LogInformation("{Peer} connected", peer);

        DeviceSession? session = null;
        try
        {
            session = await LoginAsync(input, output, peer, timeout.Token);
            if (session != null)
            {
                await ReceiveDataAsync(session, input, timeout);
            }
        }
        catch (OperationCanceledException) when (closing.IsCancellationRequested)
        {
            // server shutting down
        }
        catch (OperationCanceledException) when (timeout.IsCancellationRequested)
        {
            log.LogWarning("{Peer} {Device}: timed out", peer, session?.DeviceId ?? "-");
        }
        catch (ProtocolException e)
        {
            log.LogWarning("{Peer} {Device}: protocol error: {Error}", peer, session?.DeviceId ?? "-", e.Message);
        }
        catch (ConnectionResetException)
        {
            // peer reset the connection
        }
        finally
        {
            session?.Dispose();
        }
        log.LogInformation("{Peer} {Device} disconnected", peer, session?.DeviceId ?? "-");
    }

    private async Task<DeviceSession?> LoginAsync(PipeReader input, PipeWriter output, string peer, CancellationToken ct)
    {
        // 1. device id
        string? deviceId = await ReadMessageAsync<string>(input, ParseDeviceId, ct);
        if (deviceId == null)
        {
            return null;
        }
        if (!keys.TryGetKey(deviceId, out byte[]? key))
        {
            log.LogWarning("{Peer}: unknown device {Device}", peer, deviceId);
            return null;
        }
        var session = new DeviceSession(deviceId, key);
        try
        {
            // 2. server time + signature
            long serverTime = NextServerTime();
            byte[] serverSig = session.SignServerChallenge(serverTime);
            WriteLogin(output, serverTime, serverSig);
            await output.FlushAsync(ct);

            // 3. device time + signature
            LoginResponse? response = await ReadMessageAsync<LoginResponse>(input, ParseLoginResponse, ct);
            if (response == null)
            {
                throw new ProtocolException("connection closed during login");
            }
            if (!session.VerifyDeviceResponse(serverSig, response.DeviceTime, response.Signature))
            {
                throw new ProtocolException("invalid login signature");
            }
            session.ClockOffsetMillis = response.DeviceTime - serverTime;
            log.LogInformation("{Peer}: device {Device} logged in, clock offset {Offset} ms",
                peer, deviceId, session.ClockOffsetMillis);
            return session;
        }
        catch
        {
            session.Dispose();
            throw;
        }
    }

    private async Task ReceiveDataAsync(DeviceSession session, PipeReader input, CancellationTokenSource timeout)
    {
        while (true)
        {
            timeout.CancelAfter(IdleTimeout);
            TemperatureMessage? message = await ReadMessageAsync<TemperatureMessage>(input, ParseTemperature, timeout.Token);
            if (message == null)
            {
                return;   // device closed the connection between messages
            }
            if (!session.VerifyData(message.Samples, message.Signature))
            {
                throw new ProtocolException("invalid data signature");
            }
            sink.Record(session.DeviceId, session.ClockOffsetMillis, message.Samples);
        }
    }

    /// <summary>Unique, strictly increasing millisecond timestamp, used as the login challenge.</summary>
    private static long NextServerTime()
    {
        long now = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
        while (true)
        {
            long prev = Volatile.Read(ref lastServerTime);
            long next = Math.Max(now, prev + 1);
            if (Interlocked.CompareExchange(ref lastServerTime, next, prev) == prev)
            {
                return next;
            }
        }
    }

    private static void WriteLogin(PipeWriter output, long serverTime, byte[] signature)
    {
        Span<byte> span = output.GetSpan(sizeof(long) + 5 + signature.Length);
        BinaryPrimitives.WriteInt64LittleEndian(span, serverTime);
        int n = sizeof(long);
        n += Wire.WriteVarUInt(span[n..], (uint)signature.Length);
        signature.CopyTo(span[n..]);
        output.Advance(n + signature.Length);
    }

    // ---- framing ------------------------------------------------------------------------------------------------

    private delegate T Parser<T>(ref SequenceReader<byte> reader);

    /// <summary>Reads one complete message; returns null if the peer closed the connection before its first byte.</summary>
    private static async ValueTask<T?> ReadMessageAsync<T>(PipeReader input, Parser<T> parser, CancellationToken ct)
        where T : class
    {
        while (true)
        {
            ReadResult result = await input.ReadAsync(ct);   // pooled buffers, no per-connection allocation
            ReadOnlySequence<byte> buffer = result.Buffer;
            try
            {
                T value = Parse(buffer, parser, out SequencePosition consumed);
                input.AdvanceTo(consumed);                   // whole message parsed: consume exactly it
                return value;
            }
            catch (IncompleteMessageError)
            {
                // not all bytes of the message are buffered yet
            }
            if (result.IsCompleted)
            {
                input.AdvanceTo(buffer.End);
                return buffer.IsEmpty ? null : throw new ProtocolException("connection closed mid-message");
            }
            input.AdvanceTo(buffer.Start, buffer.End);       // nothing consumed, all examined: wait for more data
        }
    }

    // Separate from the async method: SequenceReader is a ref struct.
    private static T Parse<T>(ReadOnlySequence<byte> buffer, Parser<T> parser, out SequencePosition consumed)
    {
        var reader = new SequenceReader<byte>(buffer);
        T value = parser(ref reader);
        consumed = reader.Position;
        return value;
    }

    private static string ParseDeviceId(ref SequenceReader<byte> reader)
    {
        byte[] bytes = Wire.ReadBytes(ref reader, MaxDeviceIdLength, "deviceId");
        try
        {
            return StrictUtf8.GetString(bytes);
        }
        catch (DecoderFallbackException)
        {
            throw new ProtocolException("deviceId is not valid UTF-8");
        }
    }

    private static LoginResponse ParseLoginResponse(ref SequenceReader<byte> reader)
    {
        long deviceTime = Wire.ReadInt64(ref reader);
        byte[] signature = Wire.ReadBytes(ref reader, MaxSignatureLength, "signature");
        return new LoginResponse(deviceTime, signature);
    }

    private static TemperatureMessage ParseTemperature(ref SequenceReader<byte> reader)
    {
        byte type = Wire.ReadByte(ref reader);
        if (type != (byte)'T')
        {
            throw new ProtocolException($"unknown message type 0x{type:X2}");
        }
        uint count = Wire.ReadVarUInt(ref reader);
        if (count > MaxSamplesPerMessage)
        {
            throw new ProtocolException($"too many samples: {count}");
        }
        Wire.Require(ref reader, count * SampleSize);   // check before allocating
        var samples = new TemperatureSample[count];
        for (int i = 0; i < samples.Length; i++)
        {
            samples[i] = new TemperatureSample(Wire.ReadInt64(ref reader), Wire.ReadInt16(ref reader));
        }
        byte[] signature = Wire.ReadBytes(ref reader, MaxSignatureLength, "signature");
        return new TemperatureMessage(samples, signature);
    }

    private sealed record LoginResponse(long DeviceTime, byte[] Signature);

    private sealed record TemperatureMessage(TemperatureSample[] Samples, byte[] Signature);
}

internal readonly record struct TemperatureSample(long TimeMillis, short Temperature);

/// <summary>Per-connection authenticated state: device key and the signature chain.</summary>
internal sealed class DeviceSession : IDisposable
{
    private readonly IncrementalHash mac;
    private byte[] lastSignature = Array.Empty<byte>();

    public DeviceSession(string deviceId, byte[] key)
    {
        DeviceId = deviceId;
        mac = IncrementalHash.CreateHMAC(HashAlgorithmName.SHA256, key);
    }

    public string DeviceId { get; }

    /// <summary>Device clock minus server clock, measured at login.</summary>
    public long ClockOffsetMillis { get; set; }

    public byte[] SignServerChallenge(long serverTime)
    {
        Span<byte> buf = stackalloc byte[5];
        mac.AppendData("S"u8);
        byte[] id = Encoding.UTF8.GetBytes(DeviceId);
        mac.AppendData(buf[..Wire.WriteVarUInt(buf, (uint)id.Length)]);
        mac.AppendData(id);
        AppendInt64(serverTime);
        return mac.GetHashAndReset();
    }

    public bool VerifyDeviceResponse(byte[] serverSig, long deviceTime, byte[] signature)
    {
        mac.AppendData("D"u8);
        mac.AppendData(serverSig);
        AppendInt64(deviceTime);
        return Accept(signature);
    }

    public bool VerifyData(TemperatureSample[] samples, byte[] signature)
    {
        Span<byte> buf = stackalloc byte[sizeof(long) + sizeof(short)];
        mac.AppendData(lastSignature);
        mac.AppendData("T"u8);
        mac.AppendData(buf[..Wire.WriteVarUInt(buf, (uint)samples.Length)]);
        foreach (TemperatureSample s in samples)
        {
            BinaryPrimitives.WriteInt64LittleEndian(buf, s.TimeMillis);
            BinaryPrimitives.WriteInt16LittleEndian(buf[sizeof(long)..], s.Temperature);
            mac.AppendData(buf);
        }
        return Accept(signature);
    }

    private void AppendInt64(long value)
    {
        Span<byte> buf = stackalloc byte[sizeof(long)];
        BinaryPrimitives.WriteInt64LittleEndian(buf, value);
        mac.AppendData(buf);
    }

    /// <summary>Finishes the HMAC, compares in constant time and on success advances the chain.</summary>
    private bool Accept(byte[] signature)
    {
        byte[] expected = mac.GetHashAndReset();
        if (!CryptographicOperations.FixedTimeEquals(expected, signature))
        {
            return false;
        }
        lastSignature = expected;
        return true;
    }

    public void Dispose() => mac.Dispose();
}

/// <summary>Device keys from configuration: "Devices": { "&lt;deviceId&gt;": "&lt;base64 HMAC key&gt;" }.</summary>
internal sealed class DeviceKeyStore
{
    private readonly Dictionary<string, byte[]> keys = new(StringComparer.Ordinal);

    public DeviceKeyStore(IConfiguration config)
    {
        foreach (IConfigurationSection device in config.GetSection("Devices").GetChildren())
        {
            if (device.Value != null)
            {
                keys[device.Key] = Convert.FromBase64String(device.Value);
            }
        }
    }

    public bool TryGetKey(string deviceId, [MaybeNullWhen(false)] out byte[] key) => keys.TryGetValue(deviceId, out key);
}

/// <summary>Destination of verified samples. Replace logging with storage as needed.</summary>
internal sealed class TemperatureSink
{
    private readonly ILogger<TemperatureSink> log;

    public TemperatureSink(ILogger<TemperatureSink> log) => this.log = log;

    public void Record(string deviceId, long clockOffsetMillis, TemperatureSample[] samples)
    {
        foreach (TemperatureSample s in samples)
        {
            // device timestamp shifted to server clock
            DateTimeOffset time = DateTimeOffset.FromUnixTimeMilliseconds(s.TimeMillis - clockOffsetMillis);
            log.LogInformation("{Device} {Time:O} temperature {Temperature}", deviceId, time, s.Temperature);
        }
    }
}

/// <summary>
/// Readers over buffered input. Each throws <see cref="IncompleteMessageError"/> when the buffer ends before the
/// value does, so parsers are written as straight-line code; the caller retries once more bytes arrive.
/// </summary>
internal static class Wire
{
    public static void Require(ref SequenceReader<byte> reader, long length)
    {
        if (reader.Remaining < length)
        {
            throw IncompleteMessageError.Instance;
        }
    }

    public static byte ReadByte(ref SequenceReader<byte> reader)
    {
        return reader.TryRead(out byte value) ? value : throw IncompleteMessageError.Instance;
    }

    public static short ReadInt16(ref SequenceReader<byte> reader)
    {
        return reader.TryReadLittleEndian(out short value) ? value : throw IncompleteMessageError.Instance;
    }

    public static long ReadInt64(ref SequenceReader<byte> reader)
    {
        return reader.TryReadLittleEndian(out long value) ? value : throw IncompleteMessageError.Instance;
    }

    /// <summary>7-bit encoded unsigned int (LEB128, as BinaryWriter.Write7BitEncodedInt).</summary>
    public static uint ReadVarUInt(ref SequenceReader<byte> reader)
    {
        uint value = 0;
        for (int shift = 0; shift < 35; shift += 7)
        {
            byte b = ReadByte(ref reader);
            if (shift == 28 && b > 0x0F)
            {
                break;
            }
            value |= (uint)(b & 0x7F) << shift;
            if ((b & 0x80) == 0)
            {
                return value;
            }
        }
        throw new ProtocolException("varint overflow");
    }

    public static byte[] ReadBytes(ref SequenceReader<byte> reader, int maxLength, string what)
    {
        uint length = ReadVarUInt(ref reader);
        if (length > maxLength)
        {
            throw new ProtocolException($"{what} too long: {length}");
        }
        Require(ref reader, length);
        byte[] bytes = new byte[length];
        reader.TryCopyTo(bytes);
        reader.Advance(length);
        return bytes;
    }

    public static int WriteVarUInt(Span<byte> destination, uint value)
    {
        int i = 0;
        while (value >= 0x80)
        {
            destination[i++] = (byte)(value | 0x80);
            value >>= 7;
        }
        destination[i++] = (byte)value;
        return i;
    }
}

/// <summary>
/// Not a failure: the buffered input ends before the message does. Thrown by <see cref="Wire"/> readers and caught
/// only by the framing loop, which then waits for more data and re-parses the message from its first byte.
/// One shared instance: it carries no state and is never seen outside the framing loop.
/// </summary>
internal sealed class IncompleteMessageError : Exception
{
    public static readonly IncompleteMessageError Instance = new();

    private IncompleteMessageError() : base("incomplete message")
    {
    }
}

internal sealed class ProtocolException : Exception
{
    public ProtocolException(string message) : base(message)
    {
    }
}
