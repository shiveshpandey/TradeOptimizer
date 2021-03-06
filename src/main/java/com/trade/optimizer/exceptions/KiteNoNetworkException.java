package com.trade.optimizer.exceptions;

/**
 * This class is a wrapper for exception thrown
 * when user's phone network is off.
 */

public class KiteNoNetworkException extends KiteException {

    public KiteNoNetworkException(String message, int code) {
        super(message, code);
    }

    public KiteNoNetworkException(String message) {
        super(message);
    }
}
