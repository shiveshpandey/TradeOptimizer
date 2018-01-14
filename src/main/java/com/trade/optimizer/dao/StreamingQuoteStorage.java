package com.trade.optimizer.dao;

import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.trade.optimizer.models.Instrument;
import com.trade.optimizer.models.InstrumentOHLCData;
import com.trade.optimizer.models.InstrumentVolatilityScore;
import com.trade.optimizer.models.Order;
import com.trade.optimizer.models.Tick;

public interface StreamingQuoteStorage {

	void initializeJDBCConn();

	void storeData(ArrayList<Tick> ticks);

	void calculateAndSaveStrategy(String instrumentToken, Date endingTimeToMatch);

	void saveInstrumentDetails(List<Instrument> instrumentList, Timestamp time);

	List<Order> fetchOrderListToPlace();

	ArrayList<Long> getTopPrioritizedTokenList(int i);

	void closeJDBCConn();

	void createDaysStreamingQuoteTable() throws SQLException;

	void saveInstrumentTokenPriorityData(Map<String, InstrumentVolatilityScore> stocksSymbolArray);

	void saveInstrumentVolumeData(HashMap<String, ArrayList<InstrumentOHLCData>> stocksSymbolArray);

	void saveInstrumentVolatilityData(HashMap<String, ArrayList<InstrumentOHLCData>> instrumentVolatilityScoreList);

	void saveBackendReadyFlag(boolean backendReadyForProcessing);

	ArrayList<String> tradingSymbolListOnInstrumentTokenId(ArrayList<Long> quoteStreamingInstrumentsArr);

	void orderStatusSyncBetweenLocalAndMarket(String tradingSymbol, String transactionType, String quantity,
			String status, String tagId);

	boolean getBackendReadyFlag();

	void markInstrumentsTradable(List<InstrumentVolatilityScore> instrumentVolatilityScoreList);

	void saveLast10DaysOHLCData(HashMap<String, ArrayList<InstrumentOHLCData>> stocksSymbolArray);

	void executeMagicBeeStrategyCloseRoundOff(String instrumentToken) throws SQLException;
}
