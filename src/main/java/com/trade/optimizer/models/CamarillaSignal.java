package com.trade.optimizer.models;

public class CamarillaSignal {
	private int signal;
	private String signalLevel;

	public CamarillaSignal(int signal, String signalLevel) {
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
