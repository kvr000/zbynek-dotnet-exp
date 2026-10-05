using System;
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
        builder.Services.AddHostedService<Worker>();

        using IHost host = builder.Build();

        // The host's ConsoleLifetime already handles SIGTERM/SIGINT/SIGQUIT:
        // ctx.Cancel = true + StopApplication() → cancels stoppingToken.
        // These registrations only record the exit code; every handler for a signal is invoked.
        using var term = PosixSignalRegistration.Create(PosixSignal.SIGTERM, _ => Environment.ExitCode = 143);
        using var intr = PosixSignalRegistration.Create(PosixSignal.SIGINT,  _ => Environment.ExitCode = 130);

        await host.RunAsync();   // returns after all hosted services stopped (or ShutdownTimeout elapsed)

        // cleanup that must happen before exit goes here (flush, report status, ...)
        return Environment.ExitCode;
    }
}

internal sealed class Worker : BackgroundService
{
    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        Console.Error.WriteLine("Running until terminated...");
        try
        {
            while (true)
            {
                stoppingToken.ThrowIfCancellationRequested();
                // ... do a unit of work, passing `stoppingToken` to every async call ...
                await Task.Delay(TimeSpan.FromSeconds(1), stoppingToken);   // cancellable wait
            }
        }
        catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
        {
            Console.Error.WriteLine("Shutting down...");
        }
    }
}
