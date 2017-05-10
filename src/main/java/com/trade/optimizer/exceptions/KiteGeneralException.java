package com.trade.optimizer.exceptions;

/**
 * An unclassified, general error. Default code is 500
 */
public class KiteGeneralException extends KiteException {

    private static final long serialVersionUID = 1L;

    public KiteGeneralException(String message, int code) {
        super(message, code);
    }
}
