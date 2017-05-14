package com.trade.optimizer.exceptions;

/**
 * Exceptions raised when invalid data is returned from kite trade.
 */

public class KiteDataException extends KiteException {

	private static final long serialVersionUID = 1L;

	public KiteDataException(String message, int code) {
		super(message, code);
	}
}
