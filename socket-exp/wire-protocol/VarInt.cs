using System;
using System.Buffers;

namespace WireProtocol;

/// <summary>
/// 7-bit encoded integers (LEB128, as <see cref="System.IO.BinaryWriter.Write7BitEncodedInt64"/>): 7 bits per byte,
/// least significant group first, high bit set on every byte but the last. Signed values use ZigZag encoding
/// (as protobuf <c>sint32</c>/<c>sint64</c>) so small negative numbers stay short.
/// </summary>
public static class VarInt
{
    public const int MaxLength32 = 5;
    public const int MaxLength64 = 10;

    public static int GetLength(ulong value)
    {
        int length = 1;
        while (value >= 0x80)
        {
            value >>= 7;
            length++;
        }
        return length;
    }

    /// <summary>Writes <paramref name="value"/>; <paramref name="destination"/> needs up to <see cref="MaxLength64"/> bytes.</summary>
    public static int Write(Span<byte> destination, ulong value)
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

    /// <summary>
    /// Reads a value of at most <paramref name="bits"/> (32 or 64) bits. Returns false if the buffer ends first
    /// (the reader is then left partially advanced); throws <see cref="ProtocolException"/> if the value does not fit.
    /// </summary>
    public static bool TryRead(ref SequenceReader<byte> reader, int bits, out ulong value)
    {
        value = 0;
        for (int shift = 0; ; shift += 7)
        {
            if (!reader.TryRead(out byte b))
            {
                return false;
            }
            if (shift + 7 > bits && (b & 0x7F) >> (bits - shift) != 0)
            {
                throw new ProtocolException($"varint exceeds {bits} bits");
            }
            value |= (ulong)(b & 0x7F) << shift;
            if ((b & 0x80) == 0)
            {
                return true;
            }
            if (shift + 7 >= bits)
            {
                throw new ProtocolException($"varint exceeds {bits} bits");
            }
        }
    }

    public static uint ZigZagEncode(int value) => (uint)((value << 1) ^ (value >> 31));

    public static ulong ZigZagEncode(long value) => (ulong)((value << 1) ^ (value >> 63));

    public static int ZigZagDecode(uint value) => (int)(value >> 1) ^ -(int)(value & 1);

    public static long ZigZagDecode(ulong value) => (long)(value >> 1) ^ -(long)(value & 1);
}
