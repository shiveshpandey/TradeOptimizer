package com.trade.optimizer.tickerws;

import java.util.ArrayList;

import com.trade.optimizer.models.Tick;

/**
 * Callback to listen to ticker web-socket on tick arrival event.
 */

public interface OnTick {
    void onTick(ArrayList<Tick> ticks);
}
