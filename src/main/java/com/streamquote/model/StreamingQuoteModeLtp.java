package com.streamquote.model;

public class StreamingQuoteModeLtp extends StreamingQuote {
	public Double ltp;

	/**
	 * Constructor
	 * 
	 * @param time
	 * @param instrumentToken
	 * @param ltp
	 */
	public StreamingQuoteModeLtp(String time, String instrumentToken, Double ltp) {
		super(time, instrumentToken);
		this.ltp = ltp;
	}

	public Double getLtp() {
		return ltp;
	}

	public void setLtp(Double ltp) {
		this.ltp = ltp;
	}

	@Override
	public String toString() {
		return "StreamingQuoteModeLtp [ltp=" + ltp + ", time=" + time + ", instrumentToken=" + instrumentToken + "]";
	}
}
