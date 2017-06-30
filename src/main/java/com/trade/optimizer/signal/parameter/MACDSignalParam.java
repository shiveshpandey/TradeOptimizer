package com.trade.optimizer.signal.parameter;

import java.util.List;

import com.streamquote.utils.StreamingConfig;

public class MACDSignalParam {

	Double close = StreamingConfig.MAX_VALUE;
	Double fastEma = StreamingConfig.MAX_VALUE;
	Double slowEma = StreamingConfig.MAX_VALUE;
	Double difference = StreamingConfig.MAX_VALUE;
	static int fastEmaPeriods = 12;
	static int slowEmaPeriods = 26;
	static int signalEmaPeriods = 9;
	static double fastEmaAccFactor = 2 / (fastEmaPeriods + 1);
	static double slowEmaAccFactor = 2 / (slowEmaPeriods + 1);
	static double signalEmaAccFactor = 2 / (signalEmaPeriods + 1);
	Double signal = StreamingConfig.MAX_VALUE;

	public MACDSignalParam(List<MACDSignalParam> rsiSignalParamList) {
		for (int i = rsiSignalParamList.size() - 1; i > 0; i--) {
			if (rsiSignalParamList.size() == fastEmaPeriods) {
				for (int j = fastEmaPeriods - 1; j > 0; j--) {
					this.fastEma = this.fastEma + rsiSignalParamList.get(j).close;
				}
			} else if (rsiSignalParamList.size() > fastEmaPeriods) {
				this.fastEma = ((rsiSignalParamList.get(i).close - rsiSignalParamList.get(i - 1).fastEma)
						* fastEmaAccFactor) + rsiSignalParamList.get(i - 1).fastEma;
			}
			if (rsiSignalParamList.size() == slowEmaPeriods) {
				for (int j = slowEmaPeriods - 1; j > 0; j--) {
					this.slowEma = this.slowEma + rsiSignalParamList.get(j).close;
				}
				this.difference = this.fastEma - this.slowEma;
			} else if (rsiSignalParamList.size() > slowEmaPeriods) {
				this.slowEma = ((rsiSignalParamList.get(i).close - rsiSignalParamList.get(i - 1).slowEma)
						* slowEmaAccFactor) + rsiSignalParamList.get(i - 1).slowEma;
				this.difference = this.fastEma - this.slowEma;
			}
			if (rsiSignalParamList.size() == slowEmaPeriods + signalEmaPeriods) {
				for (int j = signalEmaPeriods - 1; j >= 0; j--) {
					this.signal = this.signal + rsiSignalParamList.get(j).difference;
				}
			} else if (rsiSignalParamList.size() > slowEmaPeriods + signalEmaPeriods) {
				this.signal = ((rsiSignalParamList.get(i).difference - rsiSignalParamList.get(i - 1).signal)
						* signalEmaAccFactor) + rsiSignalParamList.get(i - 1).signal;
			}
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