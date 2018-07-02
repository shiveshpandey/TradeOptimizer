package com.gold.buzzer.dao;

import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.gold.buzzer.models.InstrumentOHLCData;
import com.gold.buzzer.models.InstrumentVolatilityScore;
import com.zerodhatech.models.HistoricalData;
import com.zerodhatech.models.Instrument;
import com.zerodhatech.models.Order;
import com.zerodhatech.models.Tick;

public interface StreamingQuoteStorage {

	void initializeJDBCConn();

	void storeTickData(ArrayList<Tick> ticks);

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

	void executeGoldBuzzStrategyCloseRoundOff(String instrumentToken) throws SQLException;

	void saveHistoryDataOfSelectedInstruments(List<HistoricalData> dataArrayList, String instrumentToken);
}
