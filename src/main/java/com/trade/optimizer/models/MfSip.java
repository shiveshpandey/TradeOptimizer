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
 * A wrapper for Mutualfunds sip.
 */
public class MfSip {
    @SerializedName("dividend_type")
    public String dividendType;
    @SerializedName("pending_instalments")
    public int pendingInstalments;
    @SerializedName("created")
    public String created;
    @SerializedName("last_instalment")
    public String lastInstalment;
    @SerializedName("transaction_type")
    public String transactionType;
    @SerializedName("frequency")
    public String frequency;
    @SerializedName("instalment_date")
    public int instalmentDate;
    @SerializedName("fund")
    public String fund;
    @SerializedName("sip_id")
    public String sipId;
    @SerializedName("tradingsymbol")
    public String tradingsymbol;
    @SerializedName("tag")
    public String tag;
    @SerializedName("instalment_amount")
    public int instalmentAmount;
    @SerializedName("instalments")
    public int instalments;
    @SerializedName("status")
    public String status;
    @SerializedName("order_id")
    public String orderId;

}
