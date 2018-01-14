package com.gold.buzzer.models;

import com.gold.buzzer.utils.StreamingConfig;

public class InstrumentVolatilityScore {
	private Double dailyVolatility;
	private Double annualVolatility;
	private Double currentVolatility;
	private String instrumentName;
	private String tradable;
	private double price;
	private int lotSize;

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

	public double getPrice() {
		return price;
	}

	public void setPrice(double price) {
		this.price = price;
	}

	public int getLotSize() {
		int breakage = (int) (StreamingConfig.averagePerScriptInvestment / this.price);
		if (breakage >= 5000)
			this.lotSize = 5000;
		else if (breakage >= 2000)
			this.lotSize = 2000;
		else if (breakage < 2000 && breakage >= 1000)
			this.lotSize = 1000;
		else if (breakage < 1000 && breakage >= 700)
			this.lotSize = 750;
		else if (breakage < 700 && breakage >= 500)
			this.lotSize = 500;
		else if (breakage < 500 && breakage >= 300)
			this.lotSize = 300;
		else if (breakage < 300 && breakage >= 200)
			this.lotSize = 250;
		else if (breakage < 200 && breakage >= 150)
			this.lotSize = 150;
		else if (breakage < 150 && breakage >= 100)
			this.lotSize = 100;
		else if (breakage < 100 && breakage >= 60)
			this.lotSize = 75;
		else if (breakage < 60 && breakage >= 50)
			this.lotSize = 50;
		else if (breakage < 50 && breakage >= 25)
			this.lotSize = 25;
		else if (breakage < 25 && breakage >= 10)
			this.lotSize = 10;
		else if (breakage < 10 && breakage >= 5)
			this.lotSize = 5;
		else if (breakage < 5 && breakage >= 2)
			this.lotSize = 2;
		else if (breakage < 2 && breakage >= 1)
			this.lotSize = 1;
		else
			this.lotSize = 0;
		return this.lotSize;
	}

	public void setLotSize(int lotSize) {
		this.lotSize = lotSize;
	}

}
