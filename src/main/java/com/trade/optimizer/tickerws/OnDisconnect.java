package com.trade.optimizer.tickerws;

/**
 * Callback to listen to com.rainmatter.ticker websocket disconnected event.
 */
public interface OnDisconnect {
    void onDisconnected();
}
