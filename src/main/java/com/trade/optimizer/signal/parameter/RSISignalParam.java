package com.trade.optimizer.signal.parameter;

import java.util.List;

import com.streamquote.utils.StreamingConfig;

public class RSISignalParam {

    Double close = StreamingConfig.MAX_VALUE;
    Double upMove = StreamingConfig.MAX_VALUE;
    Double downMove = StreamingConfig.MAX_VALUE;
    Double avgUpMove = StreamingConfig.MAX_VALUE;
    static int periods = 14;
    Double avgDownMove = StreamingConfig.MAX_VALUE;
    Double relativeStrength = StreamingConfig.MAX_VALUE;
    Double RSI = StreamingConfig.MAX_VALUE;

    public RSISignalParam(List<RSISignalParam> rsiSignalParamList, Double close) {
        if (rsiSignalParamList.size() > 0) {
            if (close > rsiSignalParamList.get(0).close)
                this.upMove = close - rsiSignalParamList.get(0).close;
            else
                this.upMove = 0.0;
            if (close < rsiSignalParamList.get(0).close)
                this.downMove = rsiSignalParamList.get(0).close - close;
            else
                this.downMove = 0.0;
            if (rsiSignalParamList.size() == periods - 1) {
                Double avgUp = this.upMove, avgDown = this.downMove;
                for (int i = 0; i < periods - 2; i++) {
                    avgUp = avgUp + rsiSignalParamList.get(i).upMove;
                    avgDown = avgDown + rsiSignalParamList.get(i).downMove;
                }
                this.avgDownMove = avgDown / (periods - 1);
                this.avgUpMove = avgUp / (periods - 1);
                if (this.avgDownMove > 0) {
                    this.relativeStrength = this.avgUpMove / this.avgDownMove;
                    this.RSI = 100 - (100 / (this.relativeStrength + 1));
                }
            } else if (rsiSignalParamList.size() >= periods) {
                this.avgUpMove = (rsiSignalParamList.get(0).avgUpMove * (periods - 1) + this.upMove)
                        / periods;
                this.avgDownMove = (rsiSignalParamList.get(0).avgDownMove * (periods - 1)
                        + this.downMove) / periods;
                if (this.avgDownMove > 0) {
                    this.relativeStrength = this.avgUpMove / this.avgDownMove;
                    this.RSI = 100 - (100 / (this.relativeStrength + 1));
                }
            }
        }
    }

    public RSISignalParam(Double previousClose, Double close, Double upMove, Double downMove,
            Double avgUpMove, Double avgDownMove, Double relativeStrength, Double rSI) {

        if (close > previousClose)
            this.upMove = close - previousClose;
        else
            this.upMove = 0.0;
        if (close < previousClose)
            this.downMove = previousClose - close;
        else
            this.downMove = 0.0;

        this.avgUpMove = (avgUpMove * (periods - 1) + this.upMove) / periods;
        this.avgDownMove = (avgDownMove * (periods - 1) + this.downMove) / periods;
        if (this.avgDownMove > 0) {
            this.relativeStrength = this.avgUpMove / this.avgDownMove;
            this.RSI = 100 - (100 / (this.relativeStrength + 1));
        }
    }

    public RSISignalParam() {
    }

    public Double getClose() {
        return close;
    }

    public void setClose(Double close) {
        this.close = close;
    }

    public Double getUpMove() {
        return upMove;
    }

    public void setUpMove(Double upMove) {
        this.upMove = upMove;
    }

    public Double getDownMove() {
        return downMove;
    }

    public void setDownMove(Double downMove) {
        this.downMove = downMove;
    }

    public Double getAvgUpMove() {
        return avgUpMove;
    }

    public void setAvgUpMove(Double avgUpMove) {
        this.avgUpMove = avgUpMove;
    }

    public Double getAvgDownMove() {
        return avgDownMove;
    }

    public void setAvgDownMove(Double avgDownMove) {
        this.avgDownMove = avgDownMove;
    }

    public Double getRelativeStrength() {
        return relativeStrength;
    }

    public void setRelativeStrength(Double relativeStrength) {
        this.relativeStrength = relativeStrength;
    }

    public Double getRSI() {
        return RSI;
    }

    public void setRSI(Double rSI) {
        RSI = rSI;
    }
}