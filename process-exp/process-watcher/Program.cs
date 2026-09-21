using System.Diagnostics;
using System.Runtime.InteropServices;
using Newtonsoft.Json;

internal static class Program
{
    public static async Task<int> Main(string[] args)
    {
        var options = CommandLine.Parse(args);
        if (options.Help)
        {
            CommandLine.PrintHelp();
            return 0;
        }

        if (options.Doc)
        {
            Console.WriteLine(File.ReadAllText(Path.Combine(AppContext.BaseDirectory, "README.md")));
            return 0;
        }

        if (options.Spec is null)
        {
            Console.Error.WriteLine("--spec must be specified");
            return 2;
        }

        var loader = new SpecificationLoader();
        var specification = loader.Load(options.Spec, options.Properties);
        var watcher = new ProcessWatcher();

        using var term = PosixSignalRegistration.Create(PosixSignal.SIGTERM, _ => watcher.Cancel());
        using var interrupt = PosixSignalRegistration.Create(PosixSignal.SIGINT, _ => watcher.Cancel());
        using var hup = PosixSignalRegistration.Create(PosixSignal.SIGHUP, _ =>
        {
            try
            {
                var reloaded = loader.Load(options.Spec, options.Properties);
                watcher.Reload(reloaded);
                Console.Error.WriteLine("Got HUP signal; specification reloaded.");
            }
            catch (Exception ex)
            {
                Console.Error.WriteLine($"Failed to reload specification: {ex.Message}");
            }
        });

        await watcher.RunAsync(specification);
        return 0;
    }
}

internal sealed record CommandLineOptions(string? Spec, string? Properties, bool Help, bool Doc);

internal static class CommandLine
{
    public static CommandLineOptions Parse(string[] args)
    {
        string? spec = null;
        string? properties = null;
        var help = false;
        var doc = false;

        for (var i = 0; i < args.Length; i++)
        {
            switch (args[i])
            {
                case "-h":
                case "--help":
                    help = true;
                    break;
                case "--doc":
                    doc = true;
                    break;
                case "-s":
                case "--spec":
                    spec = NeedValue(args, ref i, args[i]);
                    break;
                case "-p":
                case "--properties":
                    properties = NeedValue(args, ref i, args[i]);
                    break;
                default:
                    throw new ArgumentException($"Unknown option: {args[i]}");
            }
        }

        return new CommandLineOptions(spec, properties, help, doc);
    }

    private static string NeedValue(string[] args, ref int index, string option)
    {
        if (++index >= args.Length)
            throw new ArgumentException($"Missing value for {option}");
        return args[index];
    }

    public static void PrintHelp()
    {
        Console.WriteLine("Usage: process-watcher options...");
        Console.WriteLine("ProcessRunner - runs and controls processes");
        Console.WriteLine();
        Console.WriteLine("Options:");
        Console.WriteLine("-s,--spec          definition file of processes");
        Console.WriteLine("-p,--properties    definition file of tasks");
        Console.WriteLine("--doc              prints concise documentation");
        Console.WriteLine("-h,--help          show this help");
    }
}

internal sealed class SpecificationLoader
{
    private static readonly JsonSerializerSettings JsonSettings = new()
    {
        MissingMemberHandling = MissingMemberHandling.Ignore,
        DateParseHandling = DateParseHandling.None,
        FloatParseHandling = FloatParseHandling.Double,
        Formatting = Formatting.Indented,
        CheckAdditionalContent = true,
    };

    public Specification Load(string specPath, string? propertiesPath)
    {
        var text = File.ReadAllText(specPath);
        var specification = JsonConvert.DeserializeObject<Specification>(text, JsonSettings)
            ?? throw new InvalidOperationException("Specification is empty.");

        if (propertiesPath is not null)
        {
            var properties = new Dictionary<string, string>(StringComparer.Ordinal);
            foreach (var line in File.ReadLines(propertiesPath))
            {
                var trimmed = line.Trim();
                if (trimmed.Length == 0 || trimmed.StartsWith('#') || trimmed.StartsWith('!'))
                    continue;
                var separator = trimmed.IndexOfAny(['=', ':']);
                if (separator < 0)
                    continue;
                properties[trimmed[..separator].Trim()] = trimmed[(separator + 1)..].Trim();
            }
            specification.Properties = properties;
        }

        return specification;
    }
}

internal sealed class Specification
{
    [JsonProperty("processes")]
    public Dictionary<string, ProcessDefinition> Processes { get; set; } = new();

    [JsonProperty("properties")]
    public Dictionary<string, string> Properties { get; set; } = new();
}

internal sealed class ProcessDefinition
{
    [JsonProperty("command")]
    public List<string>? Command { get; set; }

    [JsonProperty("shellCommand")]
    public string? ShellCommand { get; set; }

    [JsonProperty("startTimeMs")]
    public long StartTimeMs { get; set; } = 4_000;

    [JsonProperty("restartDelayMs")]
    public long RestartDelayMs { get; set; } = 10_000;

    [JsonProperty("terminateTimeMs")]
    public long TerminateTimeMs { get; set; } = 10_000;

    [JsonProperty("disabled")]
    public bool Disabled { get; set; }

    [JsonProperty("disableProperty")]
    public string? DisableProperty { get; set; }

    [JsonProperty("disableOs")]
    public HashSet<string>? DisableOs { get; set; }

    [JsonProperty("disableFile")]
    public string? DisableFile { get; set; }

    [JsonProperty("dependencies")]
    public List<string> Dependencies { get; set; } = [];
}

internal sealed class ProcessWatcher
{
    private readonly object sync = new();
    private readonly Dictionary<string, ProcessState> processes = new(StringComparer.Ordinal);
    private readonly ProcessExecutor executor = new();
    private readonly CancellationTokenSource shutdown = new();
    private readonly TaskCompletionSource exit = new(TaskCreationOptions.RunContinuationsAsynchronously);
    private Specification? specification;
    private bool cancelling;
    private bool reviewing;
    private bool reviewPending;
    private bool reloadPending;

    public async Task RunAsync(Specification initial)
    {
        Validate(initial);
        lock (sync)
        {
            specification = initial;
            foreach (var pair in initial.Processes)
                processes[pair.Key] = CreateState(pair.Key, pair.Value, initial.Properties);
            reviewPending = true;
        }

        await ReviewLoopAsync(shutdown.Token);
        await exit.Task;
    }

    public void Cancel()
    {
        lock (sync)
        {
            if (cancelling)
                return;
            Console.Error.WriteLine("Got termination signal.");
            cancelling = true;
            foreach (var state in processes.Values)
            {
                if (!state.IsDesiredInactive)
                    state.Desired = DesiredState.Stopped;
            }
            reviewPending = true;
        }
    }

    public void Reload(Specification newSpecification)
    {
        try
        {
            Validate(newSpecification);
            lock (sync)
            {
                if (cancelling)
                    throw new InvalidOperationException("Terminate in progress");

                specification = newSpecification;
                foreach (var state in processes.Values.ToArray())
                {
                    if (!newSpecification.Processes.TryGetValue(state.Id, out var definition))
                    {
                        state.Desired = DesiredState.Remove;
                    }
                    else if (!Equals(state.Process, definition))
                    {
                        state.Process = definition;
                        state.Desired = IsDisabled(definition, newSpecification.Properties)
                            ? DesiredState.Disabled
                            : state.Handle is null ? DesiredState.Running : DesiredState.Restart;
                    }
                    else
                    {
                        state.Desired = IsDisabled(definition, newSpecification.Properties)
                            ? DesiredState.Disabled
                            : DesiredState.Running;
                    }
                }

                foreach (var pair in newSpecification.Processes)
                    if (!processes.ContainsKey(pair.Key))
                        processes[pair.Key] = CreateState(pair.Key, pair.Value, newSpecification.Properties);

                reloadPending = true;
                reviewPending = true;
            }
        }
        catch (Exception ex)
        {
            Console.Error.WriteLine($"Reload failed: {ex.Message}");
        }
    }

    private async Task ReviewLoopAsync(CancellationToken cancellationToken)
    {
        while (!cancellationToken.IsCancellationRequested)
        {
            TimeSpan? nextReview = null;

            lock (sync)
            {
                if (!reviewPending)
                {
                    // There may be a delayed restart.  Process exit callbacks will wake the loop.
                    nextReview = TimeSpan.FromMilliseconds(250);
                }
                else
                {
                    reviewPending = false;
                    reviewing = true;
                    try
                    {
                        nextReview = ReviewLocked();
                    }
                    finally
                    {
                        reviewing = false;
                    }
                }
            }

            if (exit.Task.IsCompleted)
                break;

            try
            {
                await Task.Delay(nextReview ?? TimeSpan.FromMilliseconds(250), cancellationToken);
            }
            catch (OperationCanceledException)
            {
                break;
            }
        }
    }

    private TimeSpan? ReviewLocked()
    {
        var now = Environment.TickCount64;
        long? earliest = null;

        foreach (var state in processes.Values.ToArray())
        {
            switch (state.Desired)
            {
                case DesiredState.Running:
                    if (!cancelling && state.Handle is null)
                    {
                        if (!state.Process.Dependencies.All(DependencyIsUp))
                            continue;

                        var delay = state.LastStart + state.Process.RestartDelayMs - now;
                        if (delay > 0)
                        {
                            earliest = earliest is null ? delay : Math.Min(earliest.Value, delay);
                            continue;
                        }

                        StartLocked(state);
                    }
                    break;

                case DesiredState.Disabled:
                    if (state.Handle is null)
                    {
                        state.Status = ProcessStatus.Disabled;
                        break;
                    }
                    goto case DesiredState.Stopped;

                case DesiredState.Stopped:
                case DesiredState.Remove:
                case DesiredState.Restart:
                    if (state.Handle is not null && state.Status != ProcessStatus.Terminating)
                        TerminateLocked(state);
                    break;
            }
        }

        if (cancelling && processes.Values.All(s => s.IsDown))
            exit.TrySetResult();

        // A reload is considered complete once every currently tracked process is up/down
        // enough for the next review.  The original implementation waits until all are up.
        if (reloadPending && processes.Values.All(s => s.IsUp))
            reloadPending = false;

        return earliest.HasValue ? TimeSpan.FromMilliseconds(Math.Max(1, earliest.Value)) : null;
    }

    private bool DependencyIsUp(string dependency)
    {
        return !processes.TryGetValue(dependency, out var state) || state.IsUp;
    }

    private void StartLocked(ProcessState state)
    {
        ProcessHandle handle;
        try
        {
            handle = state.Process.Command is not null
                ? executor.Execute(state.Process.Command)
                : executor.ExecuteShell(state.Process.ShellCommand!);
        }
        catch (Exception ex)
        {
            Console.Error.WriteLine($"Failed to start process={state.Id}: {ex.Message}");
            state.Status = ProcessStatus.Stopped;
            state.Handle = null;
            return;
        }

        state.Handle = handle;
        state.Status = ProcessStatus.Starting;
        state.LastStart = Environment.TickCount64;
        Console.Error.WriteLine($"Process started: process={state.Id}");

        _ = WatchProcessAsync(state, handle);
    }

    private async Task WatchProcessAsync(ProcessState state, ProcessHandle handle)
    {
        int? exitCode = null;
        Exception? error = null;
        try
        {
            exitCode = await handle.WaitForExitAsync();
        }
        catch (Exception ex)
        {
            error = ex;
        }

        lock (sync)
        {
            if (!ReferenceEquals(state.Handle, handle))
                return;

            state.Status = ProcessStatus.Stopped;
            state.Handle = null;
            Console.Error.WriteLine(error is null
                ? $"Process exited: process={state.Id} exit={exitCode}"
                : $"Process exited: process={state.Id}: {error}");

            switch (state.Desired)
            {
                case DesiredState.Restart:
                    state.Desired = DesiredState.Running;
                    break;
                case DesiredState.Remove:
                    processes.Remove(state.Id);
                    break;
            }

            reviewPending = true;
        }
    }

    private void TerminateLocked(ProcessState state)
    {
        var handle = state.Handle!;
        state.Status = ProcessStatus.Terminating;
        handle.Terminate();
        _ = KillAfterTimeoutAsync(state, handle, state.Process.TerminateTimeMs);
    }

    private async Task KillAfterTimeoutAsync(ProcessState state, ProcessHandle handle, long timeoutMs)
    {
        try { await Task.Delay(TimeSpan.FromMilliseconds(timeoutMs)); }
        catch { return; }

        lock (sync)
        {
            if (ReferenceEquals(state.Handle, handle))
                handle.Kill();
        }
    }

    private ProcessState CreateState(string id, ProcessDefinition process, IReadOnlyDictionary<string, string> properties)
    {
        return new ProcessState
        {
            Id = id,
            Process = process,
            Desired = IsDisabled(process, properties) ? DesiredState.Disabled : DesiredState.Running,
            Status = ProcessStatus.Stopped,
        };
    }

    private static bool IsDisabled(ProcessDefinition process, IReadOnlyDictionary<string, string> properties)
    {
        if (process.Disabled)
            return true;
        if (process.DisableProperty is not null &&
            properties.TryGetValue(process.DisableProperty, out var value) &&
            bool.TryParse(value, out var enabled) && enabled)
            return true;
        if (process.DisableFile is not null && File.Exists(process.DisableFile))
            return true;
        if (process.DisableOs is not null && process.DisableOs.Contains(Environment.OSVersion.Platform.ToString(), StringComparer.OrdinalIgnoreCase))
            return true;
        if (process.DisableOs is not null && process.DisableOs.Contains(RuntimeInformation.OSDescription, StringComparer.OrdinalIgnoreCase))
            return true;
        return false;
    }

    private static void Validate(Specification specification)
    {
        foreach (var pair in specification.Processes)
        {
            var process = pair.Value;
            if ((process.Command is null) == (process.ShellCommand is null))
                throw new ArgumentException($"Process '{pair.Key}': exactly one of command and shellCommand must be specified");
            if (process.StartTimeMs < 0)
                throw new ArgumentException($"Process '{pair.Key}': startTimeMs must be non-negative");
            if (process.RestartDelayMs < 0)
                throw new ArgumentException($"Process '{pair.Key}': restartDelayMs must be non-negative");
            if (process.TerminateTimeMs < 0)
                throw new ArgumentException($"Process '{pair.Key}': terminateTimeMs must be non-negative");
        }
    }

    private sealed class ProcessState
    {
        public required string Id { get; init; }
        public required ProcessDefinition Process { get; set; }
        public DesiredState Desired { get; set; }
        public ProcessStatus Status { get; set; }
        public ProcessHandle? Handle { get; set; }
        public long LastStart { get; set; }
        public bool IsUp => Status is ProcessStatus.Starting or ProcessStatus.Running or ProcessStatus.Disabled;
        public bool IsDown => Status is ProcessStatus.Stopped or ProcessStatus.Disabled;
        public bool IsDesiredInactive => Desired is DesiredState.Remove or DesiredState.Disabled;
    }

    private enum DesiredState { Running, Disabled, Stopped, Restart, Remove }
    private enum ProcessStatus { Stopped, Starting, Running, Disabled, Terminating }
}

internal sealed class ProcessExecutor
{
    public ProcessHandle Execute(IReadOnlyList<string> command)
    {
        if (command.Count == 0)
            throw new ArgumentException("command must not be empty");

        var startInfo = new ProcessStartInfo
        {
            FileName = command[0],
            UseShellExecute = false,
            RedirectStandardOutput = false,
            RedirectStandardError = false,
        };
        foreach (var argument in command.Skip(1))
            startInfo.ArgumentList.Add(argument);

        return Start(startInfo);
    }

    public ProcessHandle ExecuteShell(string command)
    {
        if (OperatingSystem.IsWindows())
            return Start(new ProcessStartInfo("cmd.exe", $"/c {command}") { UseShellExecute = false });
        return Execute(["/bin/sh", "-c", command]);
    }

    private static ProcessHandle Start(ProcessStartInfo startInfo)
    {
        var process = new System.Diagnostics.Process { StartInfo = startInfo, EnableRaisingEvents = true };
        if (!process.Start())
            throw new InvalidOperationException("Process.Start returned false.");
        return new ProcessHandle(process);
    }
}

internal sealed class ProcessHandle
{
    private readonly System.Diagnostics.Process process;

    public ProcessHandle(System.Diagnostics.Process process) => this.process = process;

    public Task<int> WaitForExitAsync() => process.WaitForExitAsync();

    public void Terminate()
    {
        if (process.HasExited)
            return;
        if (OperatingSystem.IsLinux() || OperatingSystem.IsMacOS())
            UnixSignals.Send(process.Id, UnixSignals.SIGTERM);
        else
            process.Kill();
    }

    public void Kill()
    {
        if (!process.HasExited)
            process.Kill();
    }
}

internal static class UnixSignals
{
    public const int SIGTERM = 15;

    [DllImport("libc", SetLastError = true)]
    private static extern int kill(int pid, int signal);

    public static void Send(int pid, int signal)
    {
        if (kill(pid, signal) != 0)
            throw new InvalidOperationException($"kill({pid}, {signal}) failed: errno={Marshal.GetLastWin32Error()}");
    }
}
