package com.trade.optimizer.kiteconnect.http;

/**
 * A callback whenever there is a token expiry
 */
public interface SessionExpiryHook {

    public void sessionExpired();
}
