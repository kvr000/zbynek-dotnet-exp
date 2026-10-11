package com.github.kvr000.java.exp.iot.netty;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.handler.codec.DecoderException;
import io.netty.channel.socket.ChannelInputShutdownEvent;
import io.netty.handler.timeout.IdleStateEvent;
import com.github.kvr000.java.exp.iot.netty.wire.ProtocolException;
import com.github.kvr000.java.exp.iot.netty.wire.WireWriter;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;


/**
 * Handles one device connection: login handshake, then a stream of signed data messages.
 *
 * Receives complete messages from {@link IotDecoder}, which guarantees their order (login, then data). Everything runs on the channel's event loop, so no synchronization is needed.
 */
final class IotHandler extends ChannelInboundHandlerAdapter
{
	private static final Logger log = LogManager.getLogger(IotHandler.class);

	static final long LOGIN_TIMEOUT_SECONDS = 30;

	/** Last login challenge: unique, strictly increasing millisecond timestamp across all connections. */
	private static final AtomicLong lastServerTime = new AtomicLong();

	private final DeviceKeyStore keys;

	private final TemperatureSink sink;

	private IotDecoder decoder;

	private String peer;

	private ScheduledFuture<?> loginTimeout;

	private DeviceSession session;

	private boolean loggedIn;

	IotHandler(DeviceKeyStore keys, TemperatureSink sink)
	{
		this.keys = keys;
		this.sink = sink;
	}

	@Override
	public void handlerAdded(ChannelHandlerContext ctx)
	{
		decoder = ctx.pipeline().get(IotDecoder.class);
	}

	@Override
	public void channelActive(ChannelHandlerContext ctx)
	{
		peer = String.valueOf(ctx.channel().remoteAddress());
		log.info("{} connected", peer);
		loginTimeout = ctx.executor().schedule(() -> {
			log.warn("{} {}: login timed out", peer, deviceId());
			close(ctx);
		}, LOGIN_TIMEOUT_SECONDS, TimeUnit.SECONDS);
		// NettyEngine creates connections with auto-read off (for explicit flow control); this server just reads.
		ctx.channel().config().setAutoRead(true);
	}

	@Override
	public void channelRead(ChannelHandlerContext ctx, Object msg)
	{
		if (!ctx.channel().isActive()) {
			return;   // closing: messages decoded from the same read after the close
		}
		switch (msg) {
			case Messages.LoginMessage login -> onLogin(ctx, login);
			case Messages.DataMessage message -> onDataMessage(message);
			default -> throw new IllegalStateException("unexpected message: " + msg.getClass().getName());
		}
	}

	private void onLogin(ChannelHandlerContext ctx, Messages.LoginMessage login)
	{
		long serverTime = nextServerTime();
		byte[] key = keys.find(login.deviceId());
		if (key == null) {
			log.warn("{}: unknown device {}", peer, login.deviceId());
			rejectLogin(ctx, writeLoginResponse(ctx, login.deviceId(), serverTime, false, new byte[0]));
			return;
		}
		session = new DeviceSession(login.deviceId(), key);
		boolean success = session.verifyLogin(login.deviceTime(), login.signature());
		byte[] serverSig = session.signLoginResponse(login.signature(), serverTime, success);
		ByteBuf response = writeLoginResponse(ctx, login.deviceId(), serverTime, success, serverSig);
		if (!success) {
			log.warn("{} {}: protocol error: invalid login signature", peer, deviceId());
			rejectLogin(ctx, response);
			return;
		}
		ctx.writeAndFlush(response);
		session.clockOffsetMillis = login.deviceTime() - serverTime;
		loggedIn = true;
		loginTimeout.cancel(false);
		log.info("{}: device {} logged in, clock offset {} ms", peer, session.deviceId(), session.clockOffsetMillis);
	}

	private static ByteBuf writeLoginResponse(ChannelHandlerContext ctx, String deviceId, long serverTime, boolean success,
						  byte[] signature)
	{
		ByteBuf response = ctx.alloc().buffer();
		new WireWriter(response).writeByte('l').writeLengthPrefixedString(deviceId).writeLong(serverTime)
			.writeBoolean(success).writeLengthPrefixedBytes(signature);
		return response;
	}

	/** Sends the negative login response, then closes. */
	private void rejectLogin(ChannelHandlerContext ctx, ByteBuf response)
	{
		decoder.fail();   // discard anything still buffered
		ctx.writeAndFlush(response).addListener(ChannelFutureListener.CLOSE);
	}

	private void onDataMessage(Messages.DataMessage message)
	{
		if (!loggedIn) {   // IotDecoder only yields data after a login message, but the login may have failed
			throw new ProtocolException("data message without successful login");
		}
		switch (message) {   // exhaustive: DataMessage is sealed, a new type without a case does not compile
			case Messages.TemperatureMessage temperature -> {
				if (!session.verifyTemperature(temperature)) {
					throw new ProtocolException("invalid data signature");
				}
				sink.record(session.deviceId(), session.clockOffsetMillis, temperature.samples());
			}
		}
	}

	@Override
	public void userEventTriggered(ChannelHandlerContext ctx, Object evt)
	{
		if (evt instanceof IdleStateEvent) {
			log.warn("{} {}: timed out", peer, deviceId());
			close(ctx);
		}
		else if (evt instanceof ChannelInputShutdownEvent) {
			close(ctx);   // device closed its side (half-closure): nothing more to receive or send
		}
		else {
			ctx.fireUserEventTriggered(evt);
		}
	}

	@Override
	public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause)
	{
		if (cause instanceof DecoderException && cause.getCause() != null) {
			cause = cause.getCause();   // exceptions from IotDecoder arrive wrapped
		}
		if (cause instanceof ProtocolException) {
			log.warn("{} {}: protocol error: {}", peer, deviceId(), cause.getMessage());
		}
		else if (cause instanceof IOException) {
			// peer reset the connection
		}
		else {
			log.warn("{} {}: {}", peer, deviceId(), cause.toString());
		}
		close(ctx);
	}

	@Override
	public void channelInactive(ChannelHandlerContext ctx)
	{
		if (loginTimeout != null) {
			loginTimeout.cancel(false);
		}
		if (session != null) {
			session.close();
		}
		log.info("{} {} disconnected", peer, deviceId());
	}

	private void close(ChannelHandlerContext ctx)
	{
		decoder.fail();   // discard anything still buffered
		ctx.close();
	}

	private String deviceId()
	{
		return session != null ? session.deviceId() : "-";
	}

	private static long nextServerTime()
	{
		long now = System.currentTimeMillis();
		return lastServerTime.updateAndGet(prev -> Math.max(now, prev + 1));
	}
}
