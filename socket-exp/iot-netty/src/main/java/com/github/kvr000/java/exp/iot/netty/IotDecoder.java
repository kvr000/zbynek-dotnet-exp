package com.github.kvr000.java.exp.iot.netty;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.ReplayingDecoder;
import com.github.kvr000.java.exp.iot.netty.wire.MessageReader;
import com.github.kvr000.java.exp.iot.netty.wire.ProtocolException;

import java.util.List;


/**
 * Splits the device's byte stream into messages and enforces their order: one login message, then data messages
 * only.
 *
 * The phase is the decoder's own state, advanced with {@code checkpoint()} after each complete message, so the order
 * holds regardless of what (or how asynchronously) the next handler does with the messages. Parsers read as if the
 * whole message were buffered; when it is not, the buffer throws Netty's replay signal and {@link ReplayingDecoder}
 * re-runs {@link #decode} from the last checkpoint once more bytes arrive. Several messages in one read are decoded
 * in one go.
 */
final class IotDecoder extends ReplayingDecoder<IotDecoder.Phase>
{
	enum Phase
	{
		LOGIN,
		DATA,
		/** Protocol error seen: everything else is discarded, the handler is closing the connection. */
		FAILED,
	}

	IotDecoder()
	{
		super(Phase.LOGIN);
	}

	@Override
	protected void decode(ChannelHandlerContext ctx, ByteBuf in, List<Object> out)
	{
		MessageReader reader = new MessageReader(in);
		try {
			switch (state()) {
				case LOGIN -> {
					out.add(Messages.parseLoginMessage(reader));
					checkpoint(Phase.DATA);
				}
				case DATA -> {
					out.add(Messages.parseDataMessage(reader));
					checkpoint();
				}
				case FAILED -> in.skipBytes(actualReadableBytes());
			}
		}
		catch (RuntimeException ex) {   // not the replay signal, which is an Error
			fail();
			throw ex;
		}
	}

	/** Connection closed (or half-closed): bytes left over are an incomplete message. */
	@Override
	protected void decodeLast(ChannelHandlerContext ctx, ByteBuf in, List<Object> out)
	{
		if (state() != Phase.FAILED && actualReadableBytes() > 0) {
			fail();
			throw new ProtocolException("connection closed mid-message");
		}
	}

	/** Stops decoding, e.g. when the connection is being closed; further input is discarded. */
	void fail()
	{
		state(Phase.FAILED);
	}
}
