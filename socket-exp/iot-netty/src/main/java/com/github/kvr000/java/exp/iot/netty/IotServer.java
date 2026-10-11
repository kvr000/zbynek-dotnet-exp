package com.github.kvr000.java.exp.iot.netty;

import io.netty.channel.ChannelInitializer;
import io.netty.channel.group.ChannelGroup;
import io.netty.channel.group.DefaultChannelGroup;
import io.netty.channel.socket.DuplexChannel;
import io.netty.handler.timeout.IdleStateHandler;
import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.GlobalEventExecutor;
import net.dryuf.netty.address.AddressSpec;
import net.dryuf.netty.core.NettyEngine;
import net.dryuf.netty.core.NettyServer;
import net.dryuf.netty.util.NettyFutures;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.logging.log4j.LogManager;

import java.lang.management.ManagementFactory;
import java.nio.channels.UnsupportedAddressTypeException;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletionException;
import java.util.concurrent.TimeUnit;


/**
 * IoT TCP server for temperature devices, Netty counterpart of iot-kestrel.
 *
 * Options ({@code --name=value}): {@code port} (5000), {@code devices} (devices.properties),
 * {@code exitAfterStart} (false; stops right after startup, for timing it).
 * Exit code: 0 on normal stop, 130/143 when stopped by SIGINT/SIGTERM (set by the JVM).
 */
public final class IotServer
{
	private static final long IDLE_TIMEOUT_SECONDS = 600;
	private static final long SHUTDOWN_TIMEOUT_SECONDS = 10;

	@SuppressWarnings("unchecked")
	public static void main(String[] args) throws Exception
	{
		Map<String, String> options = parseOptions(args);
		int port = Integer.parseInt(options.getOrDefault("port", "5000"));
		DeviceKeyStore keys = DeviceKeyStore.load(Path.of(options.getOrDefault("devices", "devices.properties")));
		TemperatureSink sink = new TemperatureSink();
		ChannelGroup connections = new DefaultChannelGroup(GlobalEventExecutor.INSTANCE);

		NettyEngine engine = new NettyEngine();   // epoll on Linux, kqueue on macOS, NIO elsewhere
		NettyServer server = listenAnyIp(engine, port, new ChannelInitializer<DuplexChannel>()
		{
			@Override
			protected void initChannel(DuplexChannel ch)
			{
				connections.add(ch);
				ch.pipeline().addLast(
					new IdleStateHandler(IDLE_TIMEOUT_SECONDS, 0, 0, TimeUnit.SECONDS),
					new IotDecoder(),
					new IotHandler(keys, sink)
				);
			}
		});

		// Ctrl+C / SIGTERM: stop accepting, close all connections, stop the event loops, then the logging.
		Runtime.getRuntime().addShutdownHook(new Thread(() -> {
			System.err.println("Shutting down...");
			server.close();
			connections.close().awaitUninterruptibly(SHUTDOWN_TIMEOUT_SECONDS, TimeUnit.SECONDS);
			Future<?> workers = engine.getWorkerGroup().shutdownGracefully(0, SHUTDOWN_TIMEOUT_SECONDS, TimeUnit.SECONDS);
			Future<?> boss = engine.getBossGroup().shutdownGracefully(0, SHUTDOWN_TIMEOUT_SECONDS, TimeUnit.SECONDS);
			NettyFutures.join((Future<Void>) workers, (Future<Void>) boss).join();
			System.err.println("All connections closed.");
			LogManager.shutdown();
		}, "shutdown"));

		// Startup time: from JVM start (class loading, Netty, bind) until listening. JVM uptime rather than
		// ProcessHandle.startInstant(), which has only ~1 s precision on Linux.
		long startupMillis = ManagementFactory.getRuntimeMXBean().getUptime();
		System.err.println("Listening on port " + port + "... (startup " + startupMillis + " ms)");

		if (Boolean.parseBoolean(options.getOrDefault("exitAfterStart", "false"))) {
			System.exit(0);
		}
		server.closedFuture().join();
	}

	/** Listens on all addresses, IPv6 + IPv4 where available, otherwise IPv4 only (like Kestrel's ListenAnyIP). */
	private static NettyServer listenAnyIp(NettyEngine engine, int port, ChannelInitializer<DuplexChannel> initializer)
	{
		try {
			return new NettyServer(engine.listen(anyAddress(AddressSpec.PROTO_TCP, port), initializer).join());
		}
		catch (CompletionException ex) {
			if (ExceptionUtils.indexOfThrowable(ex, UnsupportedAddressTypeException.class) < 0) {
				throw ex;
			}
			return new NettyServer(engine.listen(anyAddress(AddressSpec.PROTO_TCP4, port), initializer).join());
		}
	}

	private static AddressSpec anyAddress(String proto, int port)
	{
		return AddressSpec.builder().proto(proto).host(AddressSpec.ANY_HOST).port(port).build();
	}

	/** {@code --name=value} pairs; {@code --name} alone means true. */
	private static Map<String, String> parseOptions(String[] args)
	{
		Map<String, String> options = new HashMap<>();
		for (String arg : args) {
			if (!arg.startsWith("--")) {
				throw new IllegalArgumentException("unexpected argument: " + arg);
			}
			int eq = arg.indexOf('=');
			if (eq < 0) {
				options.put(arg.substring(2), "true");
			}
			else {
				options.put(arg.substring(2, eq), arg.substring(eq + 1));
			}
		}
		return options;
	}
}
