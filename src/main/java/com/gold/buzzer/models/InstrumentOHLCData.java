package com.gold.buzzer.models;

public class InstrumentOHLCData {
	private double open;
	private double high;
	private double low;
	private double close;
	private double highMinusLow;
	private String dt;
	private String instrumentName;
	private String series;
	private double slowEma;
	private double fastEma;
	private double slowSma;
	private double fastSma;
	private double slowEmaLow;
	private double fastEmaLow;
	private double slowSmaLow;
	private double fastSmaLow;
	
	public double getOpen() {
		return open;
	}

	public void setOpen(double open) {
		this.open = open;
	}

	public double getHigh() {
		return high;
	}

	public void setHigh(double high) {
		this.high = high;
	}

	public double getLow() {
		return low;
	}

	public void setLow(double low) {
		this.low = low;
	}

	public double getClose() {
		return close;
	}

	public void setClose(double close) {
		this.close = close;
	}

	public String getInstrumentName() {
		return instrumentName;
	}

	public void setInstrumentName(String instrumentName) {
		this.instrumentName = instrumentName;
	}

	public double getHighMinusLow() {
		return highMinusLow;
	}

	public void setHighMinusLow(double highMinusLow) {
		this.highMinusLow = highMinusLow;
	}

	public String getDt() {
		return dt;
	}

	public void setDt(String dt) {
		this.dt = dt;
	}

	public String getSeries() {
		return series;
	}

	public void setSeries(String series) {
		this.series = series;
	}

	public double getSlowEma() {
		return slowEma;
	}

	public void setSlowEma(double slowEma) {
		this.slowEma = slowEma;
	}

	public double getFastEma() {
		return fastEma;
	}

	public void setFastEma(double fastEma) {
		this.fastEma = fastEma;
	}

	public double getSlowSma() {
		return slowSma;
	}

	public void setSlowSma(double slowSma) {
		this.slowSma = slowSma;
	}

	public double getFastSma() {
		return fastSma;
	}

	public void setFastSma(double fastSma) {
		this.fastSma = fastSma;
	}

	public double getSlowEmaLow() {
		return slowEmaLow;
	}

	public void setSlowEmaLow(double slowEmaLow) {
		this.slowEmaLow = slowEmaLow;
	}

	public double getFastEmaLow() {
		return fastEmaLow;
	}

	public void setFastEmaLow(double fastEmaLow) {
		this.fastEmaLow = fastEmaLow;
	}

	public double getSlowSmaLow() {
		return slowSmaLow;
	}

	public void setSlowSmaLow(double slowSmaLow) {
		this.slowSmaLow = slowSmaLow;
	}

	public double getFastSmaLow() {
		return fastSmaLow;
	}

	public void setFastSmaLow(double fastSmaLow) {
		this.fastSmaLow = fastSmaLow;
	}

}