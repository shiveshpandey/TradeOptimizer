package com.trade.optimizer.exceptions;

/**
 * This class is a wrapper for exception thrown when user's phone network is
 * off.
 */

public class KiteNoNetworkException extends KiteException {

	private static final long serialVersionUID = 1L;

	public KiteNoNetworkException(String message, int code) {
		super(message, code);
	}

	public KiteNoNetworkException(String message) {
		super(message);
	}
}
