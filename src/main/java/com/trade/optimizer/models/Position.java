package com.trade.optimizer.models;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.annotations.SerializedName;
import org.json.JSONException;
import org.json.JSONObject;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * A wrapper for position.
 */
public class Position {

    @SerializedName("product")
    public String product;
    @SerializedName("symbol_code")
    public String symbolCode;
    @SerializedName("name")
    public String name;
    @SerializedName("exchange")
    public String exchange;
    @SerializedName("sell_value")
    public Double sellValue;
    @SerializedName("last_price")
    public Double lastPrice;
    @SerializedName("unrealised")
    public Double unrealised;
    @SerializedName("buy_price")
    public Double buyPrice;
    @SerializedName("sell_price")
    public Double sellPrice;
    @SerializedName("m2m")
    public Double m2m;
    @SerializedName("tradingsymbol")
    public String tradingSymbol;
    @SerializedName("quantity")
    public int netQuantity;
    @SerializedName("sell_quantity")
    public int sellQuantity;
    @SerializedName("realised")
    public Double realised;
    @SerializedName("buy_quantity")
    public int buyQuantity;
    @SerializedName("net_value")
    public Double netValue;
    @SerializedName("buy_value")
    public Double buyValue;
    @SerializedName("multiplier")
    public Double multiplier;
    @SerializedName("instrument_token")
    public String instrumentToken;
    @SerializedName("close_price")
    public Double closePrice;
    @SerializedName("pnl")
    public Double pnl;
    @SerializedName("overnight_quantity")
    public int overnightQuantity;
}

