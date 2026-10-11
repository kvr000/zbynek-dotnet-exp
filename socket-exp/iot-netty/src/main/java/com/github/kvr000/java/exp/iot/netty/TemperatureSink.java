package com.github.kvr000.java.exp.iot.netty;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.time.Instant;


/**
 * Destination of verified samples. Replace logging with storage as needed.
 */
final class TemperatureSink
{
	private static final Logger log = LogManager.getLogger(TemperatureSink.class);

	void record(String deviceId, long clockOffsetMillis, Messages.TemperatureSample[] samples)
	{
		for (Messages.TemperatureSample s : samples) {
			// device timestamp shifted to server clock
			log.info("{} {} temperature {}", deviceId, Instant.ofEpochMilli(s.timeMillis() - clockOffsetMillis), s.temperature());
		}
	}
}
