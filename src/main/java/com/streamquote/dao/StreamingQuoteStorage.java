package com.streamquote.dao;

import java.util.List;
import java.util.Map;

import com.streamquote.model.OHLCquote;
import com.streamquote.model.StreamingQuote;
import com.streamquote.model.StreamingQuoteModeQuote;
import com.trade.optimizer.models.Instrument;
import com.trade.optimizer.models.Order;

public interface StreamingQuoteStorage {

	public void initializeJDBCConn();

	public void closeJDBCConn();

	public void createDaysStreamingQuoteTable(String date);

	public void storeData(StreamingQuote quote);

	public OHLCquote getOHLCDataByTimeRange(String instrumentToken, String prevTime, String currTime);

	public List<StreamingQuote> getQuoteListByTimeRange(String instrumentToken, String prevTime, String currTime);

	void storeData(List<StreamingQuoteModeQuote> quoteList, String tickType);

	public String[] getTopPrioritizedTokenList(int i);

	public List<Order> getOrderListToPlace();

	public void saveInstrumentDetails(List<Instrument> instrumentList, String string);

	public String[] getInstrumentDetailsOnTokenId(String instrumentToken);

	public List<StreamingQuoteModeQuote> getProcessableQuoteDataOnTokenId(String instrumentToken, int count);

	public void saveGeneratedSignals(Map<String, String> signalList, List<String> instrumentList);
}
