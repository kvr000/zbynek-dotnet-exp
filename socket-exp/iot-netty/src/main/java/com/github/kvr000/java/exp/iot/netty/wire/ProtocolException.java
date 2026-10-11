package com.github.kvr000.java.exp.iot.netty.wire;


/**
 * The peer sent malformed data or closed the connection in the middle of a message.
 */
public class ProtocolException extends RuntimeException
{
	private static final long serialVersionUID = 1L;

	public ProtocolException(String message)
	{
		super(message);
	}

	public ProtocolException(String message, Throwable cause)
	{
		super(message, cause);
	}
}
