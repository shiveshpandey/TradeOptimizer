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
 * A wrapper for holdings.
 */
public class Holding {

    @SerializedName("product")
    public String product;
    @SerializedName("last_price")
    public String lastPrice;
    @SerializedName("price")
    public String price;
    @SerializedName("tradingsymbol")
    public String tradingSymbol;
    @SerializedName("t1_quantity")
    public String t1Quantity;
    @SerializedName("collateral_quantity")
    public String collateralQuantity;
    @SerializedName("collateral_type")
    public String collateraltype;
    @SerializedName("account_id")
    public String accountId;
    @SerializedName("isin")
    public String isin;
    @SerializedName("pnl")
    public String pnl;
    @SerializedName("quantity")
    public String quantity;
    @SerializedName("realised_quantity")
    public String realisedQuantity;
    /*@SerializedName("net_value")
    public String netValue;*/
    @SerializedName("average_price")
    public String averagePrice;
    @SerializedName("exchange")
    public String exchange;
    @SerializedName("instrument_token")
    public String instrumentToken;
}
