package com.trade.optimizer.tickerws;

import java.util.ArrayList;

import com.trade.optimizer.models.Tick;

/**
 * Callback to listen to com.rainmatter.ticker websocket on tick arrival event.
 */

/** OnTick interface is called once ticks arrive.*/
public interface OnTick {
    void onTick(ArrayList<Tick> ticks);
}
