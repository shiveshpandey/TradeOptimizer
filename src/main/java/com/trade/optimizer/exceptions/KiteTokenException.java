package com.trade.optimizer.exceptions;

/**
 * Denotes session is expired.
 */
public class KiteTokenException extends KiteException {

	private static final long serialVersionUID = 1L;

	public KiteTokenException(String message, int code) {
		super(message, code);
	}
}
