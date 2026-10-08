using System;
using System.Buffers;
using System.Buffers.Binary;
using System.Collections.Generic;
using System.Diagnostics;
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
using WireProtocol;

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

        // Startup time: from OS process creation (runtime load, JIT, config, DI, socket bind) until Kestrel listens.
        // --exitAfterStart=true stops right away, so tools like hyperfine can time repeated starts.
        bool exitAfterStart = app.Configuration.GetValue<bool>("exitAfterStart");
        app.Lifetime.ApplicationStarted.Register(() =>
        {
            TimeSpan startup = DateTime.Now - Process.GetCurrentProcess().StartTime;
            Console.Error.WriteLine($"Listening on port {Port}... (startup {startup.TotalMilliseconds:F0} ms)");
            if (exitAfterStart)
            {
                app.Lifetime.StopApplication();
            }
        });
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
        var input = new WireReader(connection.Transport.Input, ByteOrder.LittleEndian, timeout.Token);
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
        log.LogInformation("{Peer} {Device} disconnected", peer, session?.DeviceId ?? "-");
    }

    private async Task<DeviceSession?> LoginAsync(WireReader input, PipeWriter output, string peer, CancellationToken ct)
    {
        // 1. device id
        string? deviceId = await input.ReadMessageOrDefaultAsync(ParseDeviceId);
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

        // 2. server time + signature
        long serverTime = NextServerTime();
        byte[] serverSig = session.SignServerChallenge(serverTime);
        WriteLogin(output, serverTime, serverSig);
        await output.FlushAsync(ct);

        // 3. device time + signature
        LoginResponse response = await input.ReadMessageAsync(ParseLoginResponse);
        if (!session.VerifyDeviceResponse(serverSig, response.DeviceTime, response.Signature))
        {
            throw new ProtocolException("invalid login signature");
        }
        session.ClockOffsetMillis = response.DeviceTime - serverTime;
        log.LogInformation("{Peer}: device {Device} logged in, clock offset {Offset} ms",
            peer, deviceId, session.ClockOffsetMillis);
        return session;
    }

    private async Task ReceiveDataAsync(DeviceSession session, WireReader input, CancellationTokenSource timeout)
    {
        while (true)
        {
            timeout.CancelAfter(IdleTimeout);
            DataMessage? message = await input.ReadMessageOrDefaultAsync(ParseDataMessage);
            switch (message)
            {
                case null:
                    return;   // device closed the connection between messages
                case TemperatureMessage temperature:
                    if (!session.VerifyData(temperature.Samples, temperature.Signature))
                    {
                        throw new ProtocolException("invalid data signature");
                    }
                    sink.Record(session.DeviceId, session.ClockOffsetMillis, temperature.Samples);
                    break;
                default:
                    throw new InvalidOperationException($"unhandled message {message.GetType().Name}");
            }
        }
    }

    // ---- message parsers: straight-line code over the buffered bytes, see MessageReader -------------------------

    private static string ParseDeviceId(ref MessageReader reader)
        => reader.ReadLengthPrefixedString(MaxDeviceIdLength, "deviceId");

    private static LoginResponse ParseLoginResponse(ref MessageReader reader)
        => new(reader.ReadInt64(), reader.ReadLengthPrefixedBytes(MaxSignatureLength, "signature"));

    /// <summary>Message type byte followed by the type's payload.</summary>
    private static DataMessage ParseDataMessage(ref MessageReader reader)
    {
        byte type = reader.ReadByte();
        return type switch
        {
            (byte)'T' => ParseTemperaturePayload(ref reader),
            _ => throw new ProtocolException($"unsupported message type 0x{type:X2}"),
        };
    }

    private static TemperatureMessage ParseTemperaturePayload(ref MessageReader reader)
    {
        int count = reader.ReadLength(MaxSamplesPerMessage, "sample count");
        reader.Require(count * SampleSize);   // all samples buffered before allocating
        var samples = new TemperatureSample[count];
        for (int i = 0; i < samples.Length; i++)
        {
            samples[i] = new TemperatureSample(reader.ReadInt64(), reader.ReadInt16());
        }
        byte[] signature = reader.ReadLengthPrefixedBytes(MaxSignatureLength, "signature");
        return new TemperatureMessage(samples, signature);
    }

    private sealed record LoginResponse(long DeviceTime, byte[] Signature);

    /// <summary>Base of the messages a device sends after login, one subclass per message type.</summary>
    private abstract record DataMessage;

    /// <summary>'T': temperature samples.</summary>
    private sealed record TemperatureMessage(TemperatureSample[] Samples, byte[] Signature) : DataMessage;

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
        var writer = new WireWriter(output);
        writer.WriteInt64(serverTime);
        writer.WriteLengthPrefixedBytes(signature);
    }
}

internal readonly record struct TemperatureSample(long TimeMillis, short Temperature);

/// <summary>Per-connection authenticated state: device key and the signature chain.</summary>
internal sealed class DeviceSession
{
    private readonly byte[] key;
    private readonly ArrayBufferWriter<byte> signed = new();
    private readonly WireWriter writer;
    private byte[] lastSignature = Array.Empty<byte>();

    public DeviceSession(string deviceId, byte[] key)
    {
        DeviceId = deviceId;
        this.key = key;
        writer = new WireWriter(signed);
    }

    public string DeviceId { get; }

    /// <summary>Device clock minus server clock, measured at login.</summary>
    public long ClockOffsetMillis { get; set; }

    public byte[] SignServerChallenge(long serverTime)
    {
        signed.ResetWrittenCount();
        writer.WriteByte((byte)'S');
        writer.WriteLengthPrefixedString(DeviceId);
        writer.WriteInt64(serverTime);
        return HMACSHA256.HashData(key, signed.WrittenSpan);
    }

    public bool VerifyDeviceResponse(byte[] serverSig, long deviceTime, byte[] signature)
    {
        signed.ResetWrittenCount();
        writer.WriteByte((byte)'D');
        writer.WriteBytes(serverSig);
        writer.WriteInt64(deviceTime);
        return Accept(signature);
    }

    public bool VerifyData(TemperatureSample[] samples, byte[] signature)
    {
        signed.ResetWrittenCount();
        writer.WriteBytes(lastSignature);
        writer.WriteByte((byte)'T');
        writer.WriteVarUInt32((uint)samples.Length);
        foreach (TemperatureSample s in samples)
        {
            writer.WriteInt64(s.TimeMillis);
            writer.WriteInt16(s.Temperature);
        }
        return Accept(signature);
    }

    /// <summary>HMACs the signed bytes, compares in constant time and on success advances the chain.</summary>
    private bool Accept(byte[] signature)
    {
        byte[] expected = HMACSHA256.HashData(key, signed.WrittenSpan);
        if (!CryptographicOperations.FixedTimeEquals(expected, signature))
        {
            return false;
        }
        lastSignature = expected;
        return true;
    }
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
