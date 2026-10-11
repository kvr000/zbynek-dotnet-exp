package com.github.kvr000.java.exp.iot.netty;

import com.github.kvr000.java.exp.iot.netty.wire.MessageReader;
import com.github.kvr000.java.exp.iot.netty.wire.ProtocolException;


/**
 * Protocol messages and their parsers: straight-line code that assumes the whole message is buffered, run by
 * {@link IotDecoder}, see {@link MessageReader}.
 *
 * <pre>
 * Login
 *   device -> server:     'L', string deviceId, int64 deviceTimeMillis, bytes deviceSig
 *   server -> device:     'l', string deviceId, int64 serverTimeMillis, byte success (0/1), bytes serverSig
 * Data (repeated after a successful login; the device waits for the login response first)
 *   device -> server:     byte type, payload
 *   'T' temperature:      varint count, count * { int64 timeMillis, int16 temperature }, bytes sig
 * </pre>
 * Integers are little-endian, "bytes" is a varint length followed by the bytes, "string" is bytes holding UTF-8.
 */
public final class Messages
{
	public static final int MAX_DEVICE_ID_LENGTH = 128;
	public static final int MAX_SIGNATURE_LENGTH = 64;
	public static final int MAX_SAMPLES_PER_MESSAGE = 4096;
	public static final int SAMPLE_SIZE = Long.BYTES + Short.BYTES;

	private Messages()
	{
	}

	/** 'L': the first message of a connection. */
	public record LoginMessage(String deviceId, long deviceTime, byte[] signature)
	{
	}

	/** Messages a device sends after login, one record per message type. */
	public sealed interface DataMessage permits TemperatureMessage
	{
	}

	/** 'T': temperature samples. */
	public record TemperatureMessage(TemperatureSample[] samples, byte[] signature) implements DataMessage
	{
	}

	public record TemperatureSample(long timeMillis, short temperature)
	{
	}

	public static LoginMessage parseLoginMessage(MessageReader reader)
	{
		byte type = reader.readByte();
		if (type != 'L') {
			throw new ProtocolException(String.format("expected login message, got type 0x%02X", type));
		}
		return new LoginMessage(
			reader.readLengthPrefixedString(MAX_DEVICE_ID_LENGTH, "deviceId"),
			reader.readLong(),
			reader.readLengthPrefixedBytes(MAX_SIGNATURE_LENGTH, "signature")
		);
	}

	/** Message type byte followed by the type's payload. */
	public static DataMessage parseDataMessage(MessageReader reader)
	{
		byte type = reader.readByte();
		return switch (type) {
			case 'T' -> parseTemperaturePayload(reader);
			default -> throw new ProtocolException(String.format("unsupported message type 0x%02X", type));
		};
	}

	static TemperatureMessage parseTemperaturePayload(MessageReader reader)
	{
		int count = reader.readLength(MAX_SAMPLES_PER_MESSAGE, "sample count");
		TemperatureSample[] samples = new TemperatureSample[count];
		for (int i = 0; i < count; i++) {
			samples[i] = new TemperatureSample(reader.readLong(), reader.readShort());
		}
		byte[] signature = reader.readLengthPrefixedBytes(MAX_SIGNATURE_LENGTH, "signature");
		return new TemperatureMessage(samples, signature);
	}
}
