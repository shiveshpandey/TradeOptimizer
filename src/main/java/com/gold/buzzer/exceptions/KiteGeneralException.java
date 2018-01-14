package com.gold.buzzer.exceptions;

/**
 * An unclassified, general error. Default code is 500
 */
public class KiteGeneralException extends KiteException {
    // initialize and call the base class
    public KiteGeneralException(String message, int code){
        super(message, code);
    }
}
