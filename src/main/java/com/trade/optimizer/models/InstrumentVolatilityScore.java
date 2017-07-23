package com.trade.optimizer.models;

public class InstrumentVolatilityScore {
	private Double dailyVolatility;
	private Double annualVolatility;
	private Double currentVolatility;
	private String instrumentName;
	private String tradable;

	public String getTradable() {
		return tradable;
	}

	public void setTradable(String tradable) {
		this.tradable = tradable;
	}

	public Double getDailyVolatility() {
		return dailyVolatility;
	}

	public void setDailyVolatility(Double dailyVolatility) {
		this.dailyVolatility = dailyVolatility;
	}

	public Double getAnnualVolatility() {
		return annualVolatility;
	}

	public void setAnnualVolatility(Double annualVolatility) {
		this.annualVolatility = annualVolatility;
	}

	public Double getCurrentVolatility() {
		return currentVolatility;
	}

	public void setCurrentVolatility(Double currentVolatility) {
		this.currentVolatility = currentVolatility;
	}

	public String getInstrumentName() {
		return instrumentName;
	}

	public void setInstrumentName(String instrumentName) {
		this.instrumentName = instrumentName;
	}

}
