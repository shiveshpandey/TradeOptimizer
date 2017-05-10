package com.trade.optimizer.exceptions;

/**
 * Deals with all kinds of parse and encoding errors.
 */

public class KiteParseException extends KiteException {

    private static final long serialVersionUID = 1L;

    public KiteParseException(String message, int code) {
        super(message, code);
    }
}
