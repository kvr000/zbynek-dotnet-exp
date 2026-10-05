using System;
using System.Buffers;
using System.Collections.Concurrent;
using System.IO;
using System.Net;
using System.Net.Sockets;
using System.Runtime.InteropServices;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;

internal static class Program
{
    public static async Task<int> Main(string[] args)
    {
        HostApplicationBuilder builder = Host.CreateApplicationBuilder(args);
        builder.Services.Configure<HostOptions>(o => o.ShutdownTimeout = TimeSpan.FromSeconds(20));
        builder.Services.Configure<ConsoleLifetimeOptions>(o => o.SuppressStatusMessages = true);
        builder.Services.AddHostedService<EchoServer>();

        using IHost host = builder.Build();

        // The host's ConsoleLifetime already handles SIGTERM/SIGINT/SIGQUIT:
        // ctx.Cancel = true + StopApplication() → cancels stoppingToken.
        // These registrations only record the exit code; every handler for a signal is invoked.
        using var term = PosixSignalRegistration.Create(PosixSignal.SIGTERM, _ => Environment.ExitCode = 143);
        using var intr = PosixSignalRegistration.Create(PosixSignal.SIGINT,  _ => Environment.ExitCode = 130);

        await host.RunAsync();   // returns after all hosted services stopped (or ShutdownTimeout elapsed)

        return Environment.ExitCode;
    }
}

/// <summary>TCP echo server: sends back whatever each client sends.</summary>
internal sealed class EchoServer : BackgroundService
{
    private const int Port = 5000;
    private const int BufferSize = 4096;

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        using var listener = new TcpListener(IPAddress.Any, Port);
        listener.Start();
        Console.Error.WriteLine($"Listening on port {Port}...");

        // Open connections, so shutdown can wait for them to finish.
        var connections = new ConcurrentDictionary<Task, byte>();
        try
        {
            while (true)
            {
                TcpClient client = await listener.AcceptTcpClientAsync(stoppingToken);   // cancellable accept
                Task connection = HandleClientAsync(client, stoppingToken);              // same token for every connection
                connections.TryAdd(connection, 0);
                _ = connection.ContinueWith(t => connections.TryRemove(t, out _), TaskScheduler.Default);
            }
        }
        catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
        {
            Console.Error.WriteLine("Shutting down...");
        }
        finally
        {
            listener.Stop();                         // 1. stop accepting new connections
            await Task.WhenAll(connections.Keys);    // 2. wait for open connections (they see the cancelled token)
            Console.Error.WriteLine("All connections closed.");
        }
    }

    private static async Task HandleClientAsync(TcpClient client, CancellationToken token)
    {
        EndPoint? remote = client.Client.RemoteEndPoint;
        Console.Error.WriteLine($"{remote} connected");
        try
        {
            using (client)
            {
                NetworkStream stream = client.GetStream();
                while (true)
                {
                    // Zero-byte read: waits until data arrives without holding any buffer,
                    // so idle connections cost no buffer memory.
                    await stream.ReadAsync(Memory<byte>.Empty, token);

                    byte[] buffer = ArrayPool<byte>.Shared.Rent(BufferSize);   // only while active
                    try
                    {
                        int read = await stream.ReadAsync(buffer, token);      // data is ready: completes at once
                        if (read == 0)
                            break;                                             // client closed
                        await stream.WriteAsync(buffer.AsMemory(0, read), token);
                    }
                    finally
                    {
                        ArrayPool<byte>.Shared.Return(buffer);
                    }
                }
            }
        }
        catch (OperationCanceledException) when (token.IsCancellationRequested)
        {
            // server shutting down: connection is closed by `using`
        }
        catch (Exception ex) when (ex is IOException or SocketException)
        {
            Console.Error.WriteLine($"{remote} error: {ex.Message}");
        }
        Console.Error.WriteLine($"{remote} disconnected");
    }
}
