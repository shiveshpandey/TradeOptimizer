package com.gold.buzzer.models;

public class GoldBuzzSignal {
	private int signal;
	private String signalLevel;

	public GoldBuzzSignal(int signal, String signalLevel) {
		this.signal = signal;
		this.signalLevel = signalLevel;
	}

	public int getSignal() {
		return signal;
	}

	public void setSignal(int signal) {
		this.signal = signal;
	}

	public String getSignalLevel() {
		return signalLevel;
	}

	public void setSignalLevel(String signalLevel) {
		this.signalLevel = signalLevel;
	}

}
