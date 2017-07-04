package com.trade.optimizer.signal.parameter;

import java.util.List;

import com.streamquote.utils.StreamingConfig;

public class MACDSignalParam {

	Double close = StreamingConfig.MAX_VALUE;
	Double fastEma = StreamingConfig.MAX_VALUE;
	Double slowEma = StreamingConfig.MAX_VALUE;
	Double difference = StreamingConfig.MAX_VALUE;
	static int fastEmaPeriods = 9;
	static int slowEmaPeriods = 14;
	static int signalEmaPeriods = 6;
	static double fastEmaAccFactor = (double) 2 / (fastEmaPeriods + 1);
	static double slowEmaAccFactor = (double) 2 / (slowEmaPeriods + 1);
	static double signalEmaAccFactor = (double) 2 / (signalEmaPeriods + 1);
	Double signal = StreamingConfig.MAX_VALUE;

	public MACDSignalParam(List<MACDSignalParam> rsiSignalParamList, Double close) {
		if (rsiSignalParamList.size() == fastEmaPeriods - 1) {
			this.fastEma = close;
			for (int j = 0; j < fastEmaPeriods - 1; j++) {
				this.fastEma = this.fastEma + rsiSignalParamList.get(j).close;
			}
			this.fastEma = this.fastEma / (fastEmaPeriods);
		} else if (rsiSignalParamList.size() >= fastEmaPeriods) {
			this.fastEma = ((close - rsiSignalParamList.get(0).fastEma) * fastEmaAccFactor)
					+ rsiSignalParamList.get(0).fastEma;
		}
		if (rsiSignalParamList.size() == slowEmaPeriods - 1) {
			this.slowEma = close;
			for (int j = 0; j < slowEmaPeriods - 1; j++) {
				this.slowEma = this.slowEma + rsiSignalParamList.get(j).close;
			}
			this.slowEma = this.slowEma / (slowEmaPeriods);
			this.difference = this.fastEma - this.slowEma;

		} else if (rsiSignalParamList.size() >= slowEmaPeriods) {
			this.slowEma = ((close - rsiSignalParamList.get(0).slowEma) * slowEmaAccFactor)
					+ rsiSignalParamList.get(0).slowEma;
			this.difference = this.fastEma - this.slowEma;
		}
		if (rsiSignalParamList.size() == (slowEmaPeriods + signalEmaPeriods - 2)) {
			this.signal = this.difference;
			for (int j = 0; j < signalEmaPeriods - 1; j++) {
				this.signal = this.signal + rsiSignalParamList.get(j).difference;
			}
			this.signal = this.signal / (signalEmaPeriods);
		} else if (rsiSignalParamList.size() >= (slowEmaPeriods + signalEmaPeriods - 1)) {
			this.signal = ((this.difference - rsiSignalParamList.get(0).signal) * signalEmaAccFactor)
					+ rsiSignalParamList.get(0).signal;
		}
	}

	public MACDSignalParam(Double close, Double fastEma, Double slowEma, Double signal) {

		this.fastEma = ((close - fastEma) * fastEmaAccFactor) + fastEma;
		this.slowEma = ((close - slowEma) * slowEmaAccFactor) + slowEma;
		this.difference = this.fastEma - this.slowEma;
		this.signal = ((this.difference - signal) * signalEmaAccFactor) + signal;
	}

	public MACDSignalParam() {
	}

	public Double getClose() {
		return close;
	}

	public void setClose(Double close) {
		this.close = close;
	}

	public Double getFastEma() {
		return fastEma;
	}

	public void setFastEma(Double fastEma) {
		this.fastEma = fastEma;
	}

	public Double getSlowEma() {
		return slowEma;
	}

	public void setSlowEma(Double slowEma) {
		this.slowEma = slowEma;
	}

	public Double getDifference() {
		return difference;
	}

	public void setDifference(Double difference) {
		this.difference = difference;
	}

	public Double getSignal() {
		return signal;
	}

	public void setSignal(Double signal) {
		this.signal = signal;
	}
}