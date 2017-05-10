package com.trade.optimizer.exceptions;

/**
 * Represents permission denied exceptions for certain calls. Default code is 403
 */
public class KitePermissionException extends KiteException {

    private static final long serialVersionUID = 1L;

    public KitePermissionException(String message, int code) {
        super(message, code);
    }
}
