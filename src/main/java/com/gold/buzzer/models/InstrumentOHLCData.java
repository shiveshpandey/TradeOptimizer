package com.gold.buzzer.models;

import java.util.ArrayList;

public class InstrumentOHLCData {
	private double open;
	private double high;
	private double low;
	private double close;
	private double avg2DayOpen;
	private double avg2Dayhigh;
	private double avg2Daylow;
	private double avg2Dayclose;
	private double avg3Dayopen;
	private double avg3Dayhigh;
	private double avg3Daylow;
	private double avg3Dayclose;
	private double avg5Dayopen;
	private double avg5Dayhigh;
	private double avg5Daylow;
	private double avg5Dayclose;
	private double avg10Dayopen;
	private double avg10Dayhigh;
	private double avg10Daylow;
	private double avg10Dayclose;
	private double avgWaitedOpen;
	private double avgWaitedhigh;
	private double avgWaitedlow;
	private double avgWaitedclose;
	private double waitedHighMinusLow;
	private double highMinusLow;

	private String instrumentName;

	public InstrumentOHLCData(ArrayList<InstrumentOHLCData> arrayList) {
		this.instrumentName = arrayList.get(0).instrumentName;

		int i = 0, j = 0, k = 0, l = 0;
		for (int index = 0; index < arrayList.size(); index++) {
			if (index == 0) {
				this.open = arrayList.get(index).open;
				this.high = arrayList.get(index).high;
				this.low = arrayList.get(index).low;
				this.close = arrayList.get(index).close;
			}
			if (index < 2) {
				i++;
				this.avg2DayOpen = this.avg2DayOpen + arrayList.get(index).open;
				this.avg2Dayhigh = this.avg2Dayhigh + arrayList.get(index).high;
				this.avg2Daylow = this.avg2Daylow + arrayList.get(index).low;
				this.avg2Dayclose = this.avg2Dayclose + arrayList.get(index).close;
			}
			if (index < 3) {
				j++;
				this.avg3Dayopen = this.avg3Dayopen + arrayList.get(index).open;
				this.avg3Dayhigh = this.avg3Dayhigh + arrayList.get(index).high;
				this.avg3Daylow = this.avg3Daylow + arrayList.get(index).low;
				this.avg3Dayclose = this.avg3Dayclose + arrayList.get(index).close;
			}
			if (index < 5) {
				k++;
				this.avg5Dayopen = this.avg5Dayopen + arrayList.get(index).open;
				this.avg5Dayhigh = this.avg5Dayhigh + arrayList.get(index).high;
				this.avg5Daylow = this.avg5Daylow + arrayList.get(index).low;
				this.avg5Dayclose = this.avg5Dayclose + arrayList.get(index).close;
			}
			if (index < 10) {
				l++;
				this.avg10Dayopen = this.avg10Dayopen + arrayList.get(index).open;
				this.avg10Dayhigh = this.avg10Dayhigh + arrayList.get(index).high;
				this.avg10Daylow = this.avg10Daylow + arrayList.get(index).low;
				this.avg10Dayclose = this.avg10Dayclose + arrayList.get(index).close;
			}
		}
		this.avg2DayOpen = this.avg2DayOpen / i;
		this.avg2Dayhigh = this.avg2Dayhigh / i;
		this.avg2Daylow = this.avg2Daylow / i;
		this.avg2Dayclose = this.avg2Dayclose / i;

		this.avg3Dayopen = this.avg3Dayopen / j;
		this.avg3Dayhigh = this.avg3Dayhigh / j;
		this.avg3Daylow = this.avg3Daylow / j;
		this.avg3Dayclose = this.avg3Dayclose / j;

		this.avg5Dayopen = this.avg5Dayopen / k;
		this.avg5Dayhigh = this.avg5Dayhigh / k;
		this.avg5Daylow = this.avg5Daylow / k;
		this.avg5Dayclose = this.avg5Dayclose / k;

		this.avg10Dayopen = this.avg10Dayopen / l;
		this.avg10Dayhigh = this.avg10Dayhigh / l;
		this.avg10Daylow = this.avg10Daylow / l;
		this.avg10Dayclose = this.avg10Dayclose / l;

		this.avgWaitedOpen = this.avgWaitedOpen + this.open + this.avg2DayOpen + this.avg3Dayopen + this.avg5Dayopen
				+ this.avg10Dayopen;
		this.avgWaitedhigh = this.avgWaitedhigh + this.high + this.avg2Dayhigh + this.avg3Dayhigh + this.avg5Dayhigh
				+ this.avg10Dayhigh;
		this.avgWaitedlow = this.avgWaitedlow + this.low + this.avg2Daylow + this.avg3Daylow + this.avg5Daylow
				+ this.avg10Daylow;
		this.avgWaitedclose = this.avgWaitedclose + this.close + this.avg2Dayclose + this.avg3Dayclose
				+ this.avg5Dayclose + this.avg10Dayclose;

		this.avgWaitedOpen = this.avgWaitedOpen / 5;
		this.avgWaitedhigh = this.avgWaitedhigh / 5;
		this.avgWaitedlow = this.avgWaitedlow / 5;
		this.avgWaitedclose = this.avgWaitedclose / 5;

		this.avgWaitedOpen = (this.avgWaitedOpen + this.avg10Dayopen) / 2;
		this.avgWaitedhigh = (this.avgWaitedhigh + this.avg10Dayhigh) / 2;
		this.avgWaitedlow = (this.avgWaitedlow + this.avg10Daylow) / 2;
		this.avgWaitedclose = (this.avgWaitedclose + this.avg10Dayclose) / 2;

		this.waitedHighMinusLow = this.avgWaitedhigh - this.avgWaitedlow;
		this.highMinusLow = this.high - this.low;
	}

	public InstrumentOHLCData() {
	}

	public InstrumentOHLCData(String string, ArrayList<InstrumentOHLCData> arrayList) {
		this.instrumentName = arrayList.get(0).instrumentName;

		int i = 0, j = 0, k = 0, l = 0;
		for (int index = 0; index < arrayList.size(); index++) {
			if (index == 0) {
				this.high = arrayList.get(index).high;
				this.low = arrayList.get(index).low;
				this.close = arrayList.get(index).close;
			}
			if (index < 2) {
				i++;
				this.avg2Dayhigh = this.avg2Dayhigh + arrayList.get(index).high;
				this.avg2Daylow = this.avg2Daylow + arrayList.get(index).low;
				this.avg2Dayclose = this.avg2Dayclose + arrayList.get(index).close;
			}
			if (index < 3) {
				j++;
				this.avg3Dayhigh = this.avg3Dayhigh + arrayList.get(index).high;
				this.avg3Daylow = this.avg3Daylow + arrayList.get(index).low;
				this.avg3Dayclose = this.avg3Dayclose + arrayList.get(index).close;
			}
			if (index < 5) {
				k++;
				this.avg5Dayhigh = this.avg5Dayhigh + arrayList.get(index).high;
				this.avg5Daylow = this.avg5Daylow + arrayList.get(index).low;
				this.avg5Dayclose = this.avg5Dayclose + arrayList.get(index).close;
			}
			if (index < 10) {
				l++;
				this.avg10Dayhigh = this.avg10Dayhigh + arrayList.get(index).high;
				this.avg10Daylow = this.avg10Daylow + arrayList.get(index).low;
				this.avg10Dayclose = this.avg10Dayclose + arrayList.get(index).close;
			}
		}
		this.avg2Dayhigh = this.avg2Dayhigh / i;
		this.avg2Daylow = this.avg2Daylow / i;
		this.avg2Dayclose = this.avg2Dayclose / i;

		this.avg3Dayhigh = this.avg3Dayhigh / j;
		this.avg3Daylow = this.avg3Daylow / j;
		this.avg3Dayclose = this.avg3Dayclose / j;

		this.avg5Dayhigh = this.avg5Dayhigh / k;
		this.avg5Daylow = this.avg5Daylow / k;
		this.avg5Dayclose = this.avg5Dayclose / k;

		this.avg10Dayhigh = this.avg10Dayhigh / l;
		this.avg10Daylow = this.avg10Daylow / l;
		this.avg10Dayclose = this.avg10Dayclose / l;

		this.avgWaitedhigh = this.avgWaitedhigh + this.high + this.avg2Dayhigh + this.avg3Dayhigh + this.avg5Dayhigh
				+ this.avg10Dayhigh;
		this.avgWaitedlow = this.avgWaitedlow + this.low + this.avg2Daylow + this.avg3Daylow + this.avg5Daylow
				+ this.avg10Daylow;
		this.avgWaitedclose = this.avgWaitedclose + this.close + this.avg2Dayclose + this.avg3Dayclose
				+ this.avg5Dayclose + this.avg10Dayclose;

		this.avgWaitedhigh = this.avgWaitedhigh / 5;
		this.avgWaitedlow = this.avgWaitedlow / 5;
		this.avgWaitedclose = this.avgWaitedclose / 5;

		this.avgWaitedhigh = (this.avgWaitedhigh + this.avg10Dayhigh) / 2;
		this.avgWaitedlow = (this.avgWaitedlow + this.avg10Daylow) / 2;
		this.avgWaitedclose = (this.avgWaitedclose + this.avg10Dayclose) / 2;
	}

	public InstrumentOHLCData(int i1, ArrayList<InstrumentOHLCData> arrayList) {
		this.instrumentName = arrayList.get(0).instrumentName;
		this.close = arrayList.get(0).close;

		int i = 0, j = 0, k = 0, l = 0;
		for (int index = 0; index < arrayList.size(); index++) {
			if (index == 0) {
				this.high = arrayList.get(index).high;
			}
			if (index < 2) {
				i++;
				this.avg2Dayhigh = this.avg2Dayhigh + arrayList.get(index).high;
			}
			if (index < 3) {
				j++;
				this.avg3Dayhigh = this.avg3Dayhigh + arrayList.get(index).high;
			}
			if (index < 5) {
				k++;
				this.avg5Dayhigh = this.avg5Dayhigh + arrayList.get(index).high;
			}
			if (index < 10) {
				l++;
				this.avg10Dayhigh = this.avg10Dayhigh + arrayList.get(index).high;
			}
		}
		this.avg2Dayhigh = this.avg2Dayhigh / i;

		this.avg3Dayhigh = this.avg3Dayhigh / j;

		this.avg5Dayhigh = this.avg5Dayhigh / k;

		this.avg10Dayhigh = this.avg10Dayhigh / l;

		this.avgWaitedhigh = this.avgWaitedhigh + this.high + this.avg2Dayhigh + this.avg3Dayhigh + this.avg5Dayhigh
				+ this.avg10Dayhigh;

		this.avgWaitedhigh = this.avgWaitedhigh / 5;
		this.avgWaitedhigh = (this.avgWaitedhigh + this.avg10Dayhigh) / 2;
	}

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

	public double getAvg2DayOpen() {
		return avg2DayOpen;
	}

	public void setAvg2DayOpen(double avg2DayOpen) {
		this.avg2DayOpen = avg2DayOpen;
	}

	public double getAvg2Dayhigh() {
		return avg2Dayhigh;
	}

	public void setAvg2Dayhigh(double avg2Dayhigh) {
		this.avg2Dayhigh = avg2Dayhigh;
	}

	public double getAvg2Daylow() {
		return avg2Daylow;
	}

	public void setAvg2Daylow(double avg2Daylow) {
		this.avg2Daylow = avg2Daylow;
	}

	public double getAvg2Dayclose() {
		return avg2Dayclose;
	}

	public void setAvg2Dayclose(double avg2Dayclose) {
		this.avg2Dayclose = avg2Dayclose;
	}

	public double getAvg3Dayopen() {
		return avg3Dayopen;
	}

	public void setAvg3Dayopen(double avg3Dayopen) {
		this.avg3Dayopen = avg3Dayopen;
	}

	public double getAvg3Dayhigh() {
		return avg3Dayhigh;
	}

	public void setAvg3Dayhigh(double avg3Dayhigh) {
		this.avg3Dayhigh = avg3Dayhigh;
	}

	public double getAvg3Daylow() {
		return avg3Daylow;
	}

	public void setAvg3Daylow(double avg3Daylow) {
		this.avg3Daylow = avg3Daylow;
	}

	public double getAvg3Dayclose() {
		return avg3Dayclose;
	}

	public void setAvg3Dayclose(double avg3Dayclose) {
		this.avg3Dayclose = avg3Dayclose;
	}

	public double getAvg5Dayopen() {
		return avg5Dayopen;
	}

	public void setAvg5Dayopen(double avg5Dayopen) {
		this.avg5Dayopen = avg5Dayopen;
	}

	public double getAvg5Dayhigh() {
		return avg5Dayhigh;
	}

	public void setAvg5Dayhigh(double avg5Dayhigh) {
		this.avg5Dayhigh = avg5Dayhigh;
	}

	public double getAvg5Daylow() {
		return avg5Daylow;
	}

	public void setAvg5Daylow(double avg5Daylow) {
		this.avg5Daylow = avg5Daylow;
	}

	public double getAvg5Dayclose() {
		return avg5Dayclose;
	}

	public void setAvg5Dayclose(double avg5Dayclose) {
		this.avg5Dayclose = avg5Dayclose;
	}

	public double getAvg10Dayopen() {
		return avg10Dayopen;
	}

	public void setAvg10Dayopen(double avg10Dayopen) {
		this.avg10Dayopen = avg10Dayopen;
	}

	public double getAvg10Dayhigh() {
		return avg10Dayhigh;
	}

	public void setAvg10Dayhigh(double avg10Dayhigh) {
		this.avg10Dayhigh = avg10Dayhigh;
	}

	public double getAvg10Daylow() {
		return avg10Daylow;
	}

	public void setAvg10Daylow(double avg10Daylow) {
		this.avg10Daylow = avg10Daylow;
	}

	public double getAvg10Dayclose() {
		return avg10Dayclose;
	}

	public void setAvg10Dayclose(double avg10Dayclose) {
		this.avg10Dayclose = avg10Dayclose;
	}

	public double getAvgWaitedOpen() {
		return avgWaitedOpen;
	}

	public void setAvgWaitedOpen(double avgWaitedOpen) {
		this.avgWaitedOpen = avgWaitedOpen;
	}

	public double getAvgWaitedhigh() {
		return avgWaitedhigh;
	}

	public void setAvgWaitedhigh(double avgWaitedhigh) {
		this.avgWaitedhigh = avgWaitedhigh;
	}

	public double getAvgWaitedlow() {
		return avgWaitedlow;
	}

	public void setAvgWaitedlow(double avgWaitedlow) {
		this.avgWaitedlow = avgWaitedlow;
	}

	public double getAvgWaitedclose() {
		return avgWaitedclose;
	}

	public void setAvgWaitedclose(double avgWaitedclose) {
		this.avgWaitedclose = avgWaitedclose;
	}

	public double getWaitedHighMinusLow() {
		return waitedHighMinusLow;
	}

	public void setWaitedHighMinusLow(double waitedHighMinusLow) {
		this.waitedHighMinusLow = waitedHighMinusLow;
	}

	public double getHighMinusLow() {
		return highMinusLow;
	}

	public void setHighMinusLow(double highMinusLow) {
		this.highMinusLow = highMinusLow;
	}

}