package com.trade.optimizer.exceptions;

/**
 * Represents all order placement and manipulation errors. Default code is 500.
 */

public class KiteOrderException extends KiteException {

	private static final long serialVersionUID = 1L;

	public KiteOrderException(String message, int code) {
		super(message, code);
	}
}
