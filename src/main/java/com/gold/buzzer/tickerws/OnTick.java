package com.gold.buzzer.tickerws;

import java.util.ArrayList;

import com.gold.buzzer.models.Tick;

/**
 * Callback to listen to com.rainmatter.ticker websocket on tick arrival event.
 */

/** OnTick interface is called once ticks arrive.*/
public interface OnTick {
    void onTick(ArrayList<Tick> ticks);
}
