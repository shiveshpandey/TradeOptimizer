package com.streamquote.dao;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import com.streamquote.model.OHLCquote;
import com.streamquote.model.StreamingQuote;
import com.streamquote.model.StreamingQuoteModeQuote;
import com.trade.optimizer.models.Instrument;
import com.trade.optimizer.models.Order;
import com.trade.optimizer.models.Tick;

public interface StreamingQuoteStorage {

	public void initializeJDBCConn();

	public void closeJDBCConn();

	public void createDaysStreamingQuoteTable(String date);

	public void storeData(StreamingQuote quote);

	public OHLCquote getOHLCDataByTimeRange(String instrumentToken, String prevTime, String currTime);

	void storeData(List<StreamingQuoteModeQuote> quoteList, String tickType);

	public ArrayList<Long> getTopPrioritizedTokenList(int i);

	public List<Order> getOrderListToPlace();

	public void saveInstrumentDetails(List<Instrument> instrumentList, String string);

	public String[] getInstrumentDetailsOnTokenId(String instrumentToken);

	public List<StreamingQuoteModeQuote> getProcessableQuoteDataOnTokenId(String instrumentToken, int count);

	public void saveGeneratedSignals(Map<Long, String> signalList, List<Long> instrumentList);

	public void storeData(ArrayList<Tick> ticks);
}
