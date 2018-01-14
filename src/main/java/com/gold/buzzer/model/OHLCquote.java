package com.gold.buzzer.model;

public class OHLCquote {
	private Double openPrice;
	private Double highPrice;
	private Double lowPrice;
	private Double closePrice;
	private Long vol;

	/**
	 * Constructor
	 * 
	 * @param openPrice
	 * @param highPrice
	 * @param lowPrice
	 * @param closePrice
	 * @param vol
	 */
	public OHLCquote(Double openPrice, Double highPrice, Double lowPrice, Double closePrice, Long vol) {
		super();
		this.openPrice = openPrice;
		this.highPrice = highPrice;
		this.lowPrice = lowPrice;
		this.closePrice = closePrice;
		this.vol = vol;
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

	public Long getVol() {
		return vol;
	}

	public void setVol(Long vol) {
		this.vol = vol;
	}

	@Override
	public String toString() {
		return "OHLCquote [openPrice=" + openPrice + ", highPrice=" + highPrice + ", lowPrice=" + lowPrice
				+ ", closePrice=" + closePrice + ", vol=" + vol + "]";
	}
}
