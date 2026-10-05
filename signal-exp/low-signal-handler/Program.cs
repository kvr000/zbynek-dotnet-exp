using System;
using System.Runtime.InteropServices;
using System.Threading;
using System.Threading.Tasks;

internal static class Program
{
    public static async Task<int> Main(string[] args)
    {
        using var cts = new CancellationTokenSource();
        int exitCode = 0;

        void OnSignal(PosixSignalContext ctx)
        {
            if (cts.IsCancellationRequested)
                return;                     // 2nd signal: keep default action → immediate termination

            ctx.Cancel = true;              // 1st signal: suppress termination, shut down cooperatively
            Volatile.Write(ref exitCode, ctx.Signal switch
            {
                PosixSignal.SIGINT  => 130,
                PosixSignal.SIGTERM => 143,
                _                   => 1,
            });
            Console.Error.WriteLine($"Received {ctx.Signal}, shutting down...");
            cts.Cancel();
        }

        // Declared after cts → disposed before cts, so a late signal never hits a disposed CTS.
        using var term = PosixSignalRegistration.Create(PosixSignal.SIGTERM, OnSignal);
        using var intr = PosixSignalRegistration.Create(PosixSignal.SIGINT,  OnSignal);

        try
        {
            Console.Error.WriteLine("Running until terminated...");
            await RunAsync(cts.Token);
        }
        catch (OperationCanceledException) when (cts.IsCancellationRequested)
        {
            // expected on shutdown
        }

        // cleanup that must happen before exit goes here (flush, report status, ...)
        return Volatile.Read(ref exitCode);
    }

    private static async Task RunAsync(CancellationToken token)
    {
        while (true)
        {
            token.ThrowIfCancellationRequested();
            // ... do a unit of work, passing `token` to every async call ...
            await Task.Delay(TimeSpan.FromSeconds(1), token);   // cancellable wait
        }
    }
}
