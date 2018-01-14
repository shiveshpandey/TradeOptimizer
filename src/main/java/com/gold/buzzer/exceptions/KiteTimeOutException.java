package com.gold.buzzer.exceptions;

/**
 * Wrapper around all timeout exceptions
 */

public class KiteTimeOutException extends KiteException {
    public KiteTimeOutException(String message, int code) {
        super(message, code);
    }
}
