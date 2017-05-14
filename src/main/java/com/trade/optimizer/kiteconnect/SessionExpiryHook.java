package com.trade.optimizer.kiteconnect;

/**
 * A callback whenever there is a token expiry
 */
public interface SessionExpiryHook {

	public void sessionExpired();
}
