package com.streamquote.dao;

import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;

import com.trade.optimizer.models.Instrument;
import com.trade.optimizer.models.InstrumentVolatilityScore;
import com.trade.optimizer.models.Order;
import com.trade.optimizer.models.Tick;

public interface StreamingQuoteStorage {

	public void initializeJDBCConn();

	public void closeJDBCConn();

	public void createDaysStreamingQuoteTable() throws SQLException;

	public ArrayList<Long> getTopPrioritizedTokenList(int i);

	public List<Order> getOrderListToPlace();

	public void saveInstrumentDetails(List<Instrument> instrumentList, Timestamp time);

	public String getInstrumentDetailsOnTradingsymbol(String tradingSymbol);

	public void saveGeneratedSignals(Map<Long, String> signalList, List<Long> instrumentList);

	public void storeData(ArrayList<Tick> ticks);

	public void saveInstrumentTokenPriority(Map<String, InstrumentVolatilityScore> stocksSymbolArray);

	void calculateAndStoreStrategySignalParameters(String instrumentToken, Date timeNow);

	public Map<Long, String> calculateSignalsFromStrategyParams(ArrayList<Long> instrumentList);

	public void saveInstrumentVolatilityDetails(List<InstrumentVolatilityScore> instrumentVolatilityScoreList);

	public void markTradableInstruments(List<InstrumentVolatilityScore> instrumentVolatilityScoreList);

	public void saveBackendReadyFlag(boolean backendReadyForProcessing);

	public boolean getBackendReadyFlag();

	public ArrayList<String> tradingSymbolListOnInstrumentTokenId(ArrayList<Long> quoteStreamingInstrumentsArr);

	public void orderStatusSyncBetweenLocalAndMarket(String tradingSymbol, String transactionType, String quantity,
			String status, String tag);

	public void fetchAllOrdersForDayOffActivity(ArrayList<Long> quoteStreamingInstrumentsArr);
}
