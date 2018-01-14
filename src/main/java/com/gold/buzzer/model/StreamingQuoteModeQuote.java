package com.gold.buzzer.model;

public class StreamingQuoteModeQuote extends StreamingQuoteModeLtp {
	public Long lastTradedQty;
	public Double avgTradedPrice;
	public Long vol;
	public Long buyQty;
	public Long sellQty;
	public Double openPrice;
	public Double highPrice;
	public Double lowPrice;
	public Double closePrice;

	/**
	 * Constructor
	 * 
	 * @param time
	 * @param instrumentToken
	 * @param ltp
	 * @param lastTradedQty
	 * @param avgTradedPrice
	 * @param vol
	 * @param buyQty
	 * @param sellQty
	 * @param openPrice
	 * @param highPrice
	 * @param lowPrice
	 * @param closePrice
	 */
	public StreamingQuoteModeQuote(String time, String instrumentToken, Double ltp, Long lastTradedQty,
			Double avgTradedPrice, Long vol, Long buyQty, Long sellQty, Double openPrice, Double highPrice,
			Double lowPrice, Double closePrice) {
		super(time, instrumentToken, ltp);
		this.lastTradedQty = lastTradedQty;
		this.avgTradedPrice = avgTradedPrice;
		this.vol = vol;
		this.buyQty = buyQty;
		this.sellQty = sellQty;
		this.openPrice = openPrice;
		this.highPrice = highPrice;
		this.lowPrice = lowPrice;
		this.closePrice = closePrice;
	}

	public Long getLastTradedQty() {
		return lastTradedQty;
	}

	public void setLastTradedQty(Long lastTradedQty) {
		this.lastTradedQty = lastTradedQty;
	}

	public Double getAvgTradedPrice() {
		return avgTradedPrice;
	}

	public void setAvgTradedPrice(Double avgTradedPrice) {
		this.avgTradedPrice = avgTradedPrice;
	}

	public Long getVol() {
		return vol;
	}

	public void setVol(Long vol) {
		this.vol = vol;
	}

	public Long getBuyQty() {
		return buyQty;
	}

	public void setBuyQty(Long buyQty) {
		this.buyQty = buyQty;
	}

	public Long getSellQty() {
		return sellQty;
	}

	public void setSellQty(Long sellQty) {
		this.sellQty = sellQty;
	}

	public Double getOpenPrice() {
		return openPrice;
	}

	public void setOpenPrice(Double openPrice) {
		this.openPrice = openPrice;
	}

	public Double getHighPrice() {
		return highPrice;
	}

	public void setHighPrice(Double highPrice) {
		this.highPrice = highPrice;
	}

	public Double getLowPrice() {
		return lowPrice;
	}

	public void setLowPrice(Double lowPrice) {
		this.lowPrice = lowPrice;
	}

	public Double getClosePrice() {
		return closePrice;
	}

	public void setClosePrice(Double closePrice) {
		this.closePrice = closePrice;
	}

	@Override
	public String toString() {
		return "StreamingQuoteModeQuote [lastTradedQty=" + lastTradedQty + ", avgTradedPrice=" + avgTradedPrice
				+ ", vol=" + vol + ", buyQty=" + buyQty + ", sellQty=" + sellQty + ", openPrice=" + openPrice
				+ ", highPrice=" + highPrice + ", lowPrice=" + lowPrice + ", closePrice=" + closePrice + ", ltp=" + ltp
				+ ", time=" + time + ", instrumentToken=" + instrumentToken + "]";
	}
}
