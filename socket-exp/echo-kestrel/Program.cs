using System;
using System.Buffers;
using System.IO.Pipelines;
using System.Runtime.InteropServices;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Connections;
using Microsoft.AspNetCore.Connections.Features;
using Microsoft.AspNetCore.Hosting;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

internal static class Program
{
    private const int Port = 5000;

    public static async Task<int> Main(string[] args)
    {
        WebApplicationBuilder builder = WebApplication.CreateSlimBuilder(args);
        builder.Services.Configure<HostOptions>(o => o.ShutdownTimeout = TimeSpan.FromSeconds(20));
        builder.Services.Configure<ConsoleLifetimeOptions>(o => o.SuppressStatusMessages = true);
        builder.Logging.AddFilter("Microsoft.Hosting.Lifetime", LogLevel.Warning);   // hide "Now listening on ..."
        builder.WebHost.ConfigureKestrel(o =>
        {
            o.Limits.MaxConcurrentConnections = 10_000;
            o.ListenAnyIP(Port, listen => listen.UseConnectionHandler<EchoHandler>());
        });

        await using WebApplication app = builder.Build();

        // WebApplication runs on the Generic Host, whose ConsoleLifetime already handles SIGTERM/SIGINT/SIGQUIT:
        // ctx.Cancel = true + StopApplication() → Kestrel stops accepting, signals ConnectionClosedRequested
        // on every open connection and waits for handlers up to ShutdownTimeout, then aborts the rest.
        // These registrations only record the exit code; every handler for a signal is invoked.
        using var term = PosixSignalRegistration.Create(PosixSignal.SIGTERM, _ => Environment.ExitCode = 143);
        using var intr = PosixSignalRegistration.Create(PosixSignal.SIGINT,  _ => Environment.ExitCode = 130);

        // Optional hooks into the shutdown sequence.
        app.Lifetime.ApplicationStarted.Register(() => Console.Error.WriteLine($"Listening on port {Port}..."));
        app.Lifetime.ApplicationStopping.Register(() => Console.Error.WriteLine("Shutting down..."));
        app.Lifetime.ApplicationStopped.Register(() => Console.Error.WriteLine("All connections closed."));

        await app.RunAsync();   // returns after Kestrel and all hosted services stopped

        // cleanup that must happen before exit goes here (flush, report status, ...)
        return Environment.ExitCode;   // 0 = normal stop, 130/143 = stopped by SIGINT/SIGTERM
    }
}

/// <summary>TCP echo handler: sends back whatever the client sends.</summary>
internal sealed class EchoHandler : ConnectionHandler
{
    public override async Task OnConnectedAsync(ConnectionContext connection)
    {
        // Signalled when the server starts a graceful shutdown.
        CancellationToken closing = connection.Features.Get<IConnectionLifetimeNotificationFeature>()
                                        ?.ConnectionClosedRequested ?? CancellationToken.None;
        PipeReader input = connection.Transport.Input;
        PipeWriter output = connection.Transport.Output;
        Console.Error.WriteLine($"{connection.RemoteEndPoint} connected");
        try
        {
            while (true)
            {
                ReadResult result = await input.ReadAsync(closing);     // pooled buffers, no per-connection allocation
                ReadOnlySequence<byte> buffer = result.Buffer;
                foreach (ReadOnlyMemory<byte> segment in buffer)
                {
                    output.Write(segment.Span);
                }
                input.AdvanceTo(buffer.End);
                await output.FlushAsync(closing);                      // backpressure: waits if the client reads slowly
                if (result.IsCompleted)
                {
                    break;                                             // client closed
                }
            }
        }
        catch (OperationCanceledException) when (closing.IsCancellationRequested)
        {
            // server shutting down
        }
        Console.Error.WriteLine($"{connection.RemoteEndPoint} disconnected");
    }
}
