package com.trade.optimizer.exceptions;

/**
 * Represents a network issue between Kite and the backend Order Management System (OMS). Default
 * code is 503.
 */

public class KiteNetworkException extends KiteException {

    private static final long serialVersionUID = 1L;

    public KiteNetworkException(String message, int code) {
        super(message, code);
    }
}
