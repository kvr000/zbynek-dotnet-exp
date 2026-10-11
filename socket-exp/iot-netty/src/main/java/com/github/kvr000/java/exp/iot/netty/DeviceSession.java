package com.github.kvr000.java.exp.iot.netty;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import com.github.kvr000.java.exp.iot.netty.wire.WireWriter;

import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;
import java.security.GeneralSecurityException;
import java.security.MessageDigest;


/**
 * Per-connection authenticated state: device key and the signature chain.
 *
 * Signatures are HMAC-SHA256 with the per-device key over the message's own bytes, chained so each covers the
 * whole history of the session:
 * <pre>
 *     deviceSig = HMAC(K, 'L' || string deviceId || int64 deviceTime)
 *     serverSig = HMAC(K, deviceSig || 'l' || string deviceId || int64 serverTime || byte success)
 *     dataSig_n = HMAC(K, prevSig || 'T' || varint count || samples)      prevSig = serverSig, then dataSig_n-1
 * </pre>
 * serverTime is unique per server process, so a replayed login gets a new serverSig and recorded data signatures no
 * longer verify.
 */
final class DeviceSession implements AutoCloseable
{
	private final String deviceId;

	private final Mac mac;

	private final ByteBuf signed = Unpooled.buffer(256);

	private final WireWriter writer = new WireWriter(signed);

	private byte[] lastSignature = new byte[0];

	/** Device clock minus server clock, measured at login. */
	long clockOffsetMillis;

	DeviceSession(String deviceId, byte[] key)
	{
		this.deviceId = deviceId;
		try {
			mac = Mac.getInstance("HmacSHA256");
			mac.init(new SecretKeySpec(key, "HmacSHA256"));
		}
		catch (GeneralSecurityException ex) {
			throw new IllegalStateException("HmacSHA256 not available", ex);
		}
	}

	String deviceId()
	{
		return deviceId;
	}

	boolean verifyLogin(long deviceTime, byte[] signature)
	{
		signed.clear();
		writer.writeByte('L').writeLengthPrefixedString(deviceId).writeLong(deviceTime);
		return accept(signature);
	}

	/** Signs the login response, chained to the received device signature; starts the data chain. */
	byte[] signLoginResponse(byte[] deviceSig, long serverTime, boolean success)
	{
		signed.clear();
		writer.writeBytes(deviceSig).writeByte('l').writeLengthPrefixedString(deviceId).writeLong(serverTime)
			.writeBoolean(success);
		lastSignature = hmac();
		return lastSignature;
	}

	boolean verifyTemperature(Messages.TemperatureMessage message)
	{
		signed.clear();
		writer.writeBytes(lastSignature).writeByte('T').writeVarUInt32(message.samples().length);
		for (Messages.TemperatureSample s : message.samples()) {
			writer.writeLong(s.timeMillis()).writeShort(s.temperature());
		}
		return accept(message.signature());
	}

	private byte[] hmac()
	{
		mac.update(signed.nioBuffer());
		return mac.doFinal();
	}

	/** HMACs the signed bytes, compares in constant time and on success advances the chain. */
	private boolean accept(byte[] signature)
	{
		byte[] expected = hmac();
		if (!MessageDigest.isEqual(expected, signature)) {
			return false;
		}
		lastSignature = expected;
		return true;
	}

	@Override
	public void close()
	{
		signed.release();
	}
}
