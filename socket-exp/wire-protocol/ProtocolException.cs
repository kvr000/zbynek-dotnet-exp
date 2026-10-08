using System;

namespace WireProtocol;

/// <summary>The peer sent malformed data or closed the connection in the middle of a value.</summary>
public sealed class ProtocolException : Exception
{
    public ProtocolException(string message) : base(message)
    {
    }

    public ProtocolException(string message, Exception innerException) : base(message, innerException)
    {
    }
}

/// <summary>Byte order of fixed-size integers and floating point values.</summary>
public enum ByteOrder
{
    LittleEndian,
    BigEndian,
}
