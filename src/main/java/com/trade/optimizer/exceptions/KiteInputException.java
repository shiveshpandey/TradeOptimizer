package com.trade.optimizer.exceptions;

/**
 * Represents user input errors such as missing and invalid parameters. Default code is 400.
 */
public class KiteInputException extends KiteException {

    private static final long serialVersionUID = 1L;

    public KiteInputException(String message, int code) {
        super(message, code);
    }
}
