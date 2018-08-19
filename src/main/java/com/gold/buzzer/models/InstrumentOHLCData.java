package com.gold.buzzer.models;

public class InstrumentOHLCData {
	private double open;
	private double high;
	private double low;
	private double close;
	private double highMinusLow;
	private String dt;
	private String instrumentName;

	
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

}