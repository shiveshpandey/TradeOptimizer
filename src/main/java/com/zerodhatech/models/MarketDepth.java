package com.zerodhatech.models;

import java.util.List;

import com.google.gson.annotations.SerializedName;

/**
 * A wrapper for market depth data.
 */
public class MarketDepth {
	@SerializedName("buy")
	public List<Depth> buy;
	@SerializedName("sell")
	public List<Depth> sell;
}
