package com.trade.optimizer.exceptions;

/**
 * This deals with user exceptions, like user is not allowed to trade in a segment, or invalid
 * user-name/password.
 *
 * It extends from the base exception.
 */

public class KiteUserException extends KiteException {

    private static final long serialVersionUID = 1L;

    public KiteUserException(String message, int code) {
        super(message, code);
    }
}
