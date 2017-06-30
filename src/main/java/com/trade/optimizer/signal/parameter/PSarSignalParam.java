package com.trade.optimizer.signal.parameter;

import com.streamquote.utils.StreamingConfig;

public class PSarSignalParam {

	Double high = StreamingConfig.MAX_VALUE;
	Double low = StreamingConfig.MAX_VALUE;
	Double pSar = StreamingConfig.MAX_VALUE;
	Double eP = StreamingConfig.MAX_VALUE;
	Double eP_pSar = StreamingConfig.MAX_VALUE;
	static Double accFactor1 = 0.02;
	static Double accFactor2 = 0.2;
	Double accFactor = 0.2;
	Double eP_pSarXaccFactor = StreamingConfig.MAX_VALUE;
	int trend = 1;

	public PSarSignalParam(Double high, Double low) {
		this.high = high;
		this.low = low;
		this.pSar = low;
		this.eP = high;
		this.eP_pSar = high - low;
		this.accFactor = accFactor1;
		this.eP_pSarXaccFactor = this.eP_pSar * accFactor1;
		if (this.eP_pSarXaccFactor > 0)
			this.trend = 2;
		else if (this.eP_pSarXaccFactor < 0)
			this.trend = 0;
	}

	public PSarSignalParam(Double high, Double low, Double pSar, Double eP, Double eP_pSar, Double accFactor,
			Double eP_pSarXaccFactor, int trend) {

		this.high = high;
		this.low = low;

		this.pSar = pSar + eP_pSarXaccFactor;
		if ((trend == 0 && this.pSar < this.high) || (trend == 2 && this.pSar > this.low))
			this.pSar = eP;

		if (this.pSar < this.high)
			this.trend = 2;
		else if (this.pSar > this.low)
			this.trend = 0;
		else
			this.trend = 1;

		if (this.trend == 2 && this.high > eP)
			this.eP = this.high;
		else if (this.trend == 2 && this.high <= eP)
			this.eP = eP;
		else if (this.trend == 0 && this.low < eP)
			this.eP = this.low;
		else if (this.trend == 0 && this.low >= eP)
			this.eP = eP;

		this.eP_pSar = this.eP - this.pSar;

		if (this.trend == trend) {
			if (trend == 2 && eP > this.eP) {
				this.accFactor = accFactor + accFactor1;
			}
			if (trend == 2 && eP <= this.eP) {
				this.accFactor = accFactor;
			}
			if (trend == 0 && eP < this.eP) {
				this.accFactor = accFactor + accFactor1;
			}
			if (trend == 0 && eP >= this.eP) {
				this.accFactor = accFactor;
			}
		} else {
			this.accFactor = accFactor1;
		}
		if (this.accFactor >= accFactor2) {
			this.accFactor = accFactor2;
		}

		this.eP_pSarXaccFactor = this.eP_pSar * this.accFactor;
	}

	public Double getHigh() {
		return high;
	}

	public void setHigh(Double high) {
		this.high = high;
	}

	public Double getLow() {
		return low;
	}

	public void setLow(Double low) {
		this.low = low;
	}

	public Double getpSar() {
		return pSar;
	}

	public void setpSar(Double pSar) {
		this.pSar = pSar;
	}

	public Double geteP() {
		return eP;
	}

	public void seteP(Double eP) {
		this.eP = eP;
	}

	public Double geteP_pSar() {
		return eP_pSar;
	}

	public void seteP_pSar(Double eP_pSar) {
		this.eP_pSar = eP_pSar;
	}

	public Double getAccFactor() {
		return accFactor;
	}

	public void setAccFactor(Double accFactor) {
		this.accFactor = accFactor;
	}

	public Double geteP_pSarXaccFactor() {
		return eP_pSarXaccFactor;
	}

	public void seteP_pSarXaccFactor(Double eP_pSarXaccFactor) {
		this.eP_pSarXaccFactor = eP_pSarXaccFactor;
	}

	public int getTrend() {
		return trend;
	}

	public void setTrend(int trend) {
		this.trend = trend;
	}
}