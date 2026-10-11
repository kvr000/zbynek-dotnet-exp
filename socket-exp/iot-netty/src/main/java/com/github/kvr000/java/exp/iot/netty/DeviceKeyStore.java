package com.github.kvr000.java.exp.iot.netty;

import java.io.IOException;
import java.io.Reader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Base64;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;


/**
 * Device keys from a properties file: {@code deviceId=base64 HMAC key}.
 */
final class DeviceKeyStore
{
	private final Map<String, byte[]> keys;

	private DeviceKeyStore(Map<String, byte[]> keys)
	{
		this.keys = keys;
	}

	static DeviceKeyStore load(Path path) throws IOException
	{
		Properties properties = new Properties();
		try (Reader reader = Files.newBufferedReader(path, StandardCharsets.UTF_8)) {
			properties.load(reader);
		}
		Map<String, byte[]> keys = new HashMap<>();
		for (String deviceId : properties.stringPropertyNames()) {
			keys.put(deviceId, Base64.getDecoder().decode(properties.getProperty(deviceId).trim()));
		}
		return new DeviceKeyStore(Map.copyOf(keys));
	}

	/** Key of the device, or null if unknown. */
	byte[] find(String deviceId)
	{
		return keys.get(deviceId);
	}
}
