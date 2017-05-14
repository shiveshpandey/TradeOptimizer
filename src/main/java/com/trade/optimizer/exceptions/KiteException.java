package com.trade.optimizer.exceptions;

/**
 * This is the base exception class which has a publicly accessible message and
 * code that is received from Kite Connect api.
 */

public class KiteException extends Throwable {

	private static final long serialVersionUID = 1L;
	public String message;
	public int code;

	public KiteException(String message) {
		this.message = message;
	}

	public KiteException(String message, int code) {
		this.message = message;
		this.code = code;
	}
}
