using System;
using System.Collections.Generic;
using Windows.Devices.Bluetooth.Advertisement;

class Program
{
    // Store the MAC address alongside the best name we have found for it
    private static readonly Dictionary<ulong, string> DiscoveredDevices = new Dictionary<ulong, string>();
    private static readonly object LockObject = new object();

    static void Main(string[] args)
    {
        BluetoothLEAdvertisementWatcher watcher = new BluetoothLEAdvertisementWatcher();

        // FIX 1: Change scanning mode to Active to request the names from devices
        watcher.ScanningMode = BluetoothLEScanningMode.Active;

        watcher.Received += OnAdvertisementReceived;

        Console.WriteLine("Scanning for BLE devices actively... Press any key to stop.");
        watcher.Start();

        Console.ReadKey();
        watcher.Stop();
    }

    private static void OnAdvertisementReceived(BluetoothLEAdvertisementWatcher sender, BluetoothLEAdvertisementReceivedEventArgs args)
    {
        string currentName = string.IsNullOrEmpty(args.Advertisement.LocalName) ? "Unknown Device" : args.Advertisement.LocalName;
        bool shouldPrint = false;

        lock (LockObject)
        {
            // Check if we have seen this MAC address before
            if (DiscoveredDevices.TryGetValue(args.BluetoothAddress, out string? lastKnownName))
            {
                if (currentName != lastKnownName && currentName != "Unknown Device")
                {
                    DiscoveredDevices[args.BluetoothAddress] = currentName;
                    shouldPrint = true;
                }
            }
            else
            {
                // Completely new device address discovered
                DiscoveredDevices.Add(args.BluetoothAddress, currentName);
                shouldPrint = true;
            }
        }

        if (shouldPrint)
        {
            Console.WriteLine($"Found: {currentName} [{args.BluetoothAddress:X}]");
        }
    }
}
