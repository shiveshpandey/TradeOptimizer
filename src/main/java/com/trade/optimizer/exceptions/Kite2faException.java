package com.trade.optimizer.exceptions;

/**
 * Raised when there is a two FA related error. Default code is 403.
 */
public class Kite2faException extends KiteException {

    private static final long serialVersionUID = 1L;

    public Kite2faException(String message, int code) {
        super(message, code);
    }
}
