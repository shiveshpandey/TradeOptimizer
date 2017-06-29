package com.streamquote.dao;

import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;

import com.streamquote.model.StreamingQuoteModeQuote;
import com.trade.optimizer.models.Instrument;
import com.trade.optimizer.models.Order;
import com.trade.optimizer.models.Tick;

public interface StreamingQuoteStorage {

    public void initializeJDBCConn();

    public void closeJDBCConn();

    public void createDaysStreamingQuoteTable(String date) throws SQLException;

    public void storeData(List<StreamingQuoteModeQuote> quoteList, String tickType);

    public ArrayList<Long> getTopPrioritizedTokenList(int i);

    public List<Order> getOrderListToPlace();

    public void saveInstrumentDetails(List<Instrument> instrumentList, Timestamp time);

    public String[] getInstrumentDetailsOnTokenId(String instrumentToken);

    public void saveGeneratedSignals(Map<Long, String> signalList, List<Long> instrumentList);

    public void storeData(ArrayList<Tick> ticks);

    public ArrayList<String> getInstrumentTokenIdsFromSymbols(
            Map<String, Double> stocksSymbolArray);

    void calculateAndStoreStrategySignalParameters(String instrumentToken, Date timeNow);

    public Map<Long, String> calculateSignalsFromStrategyParams(ArrayList<Long> instrumentList);
}
