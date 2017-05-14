package com.streamquote.dao;

import java.util.List;

import com.streamquote.model.OHLCquote;
import com.streamquote.model.StreamingQuote;
import com.streamquote.model.StreamingQuoteModeQuote;

public interface IStreamingQuoteStorage {

	public void initializeJDBCConn();

	public void closeJDBCConn();

	public void createDaysStreamingQuoteTable(String date);

	public void storeData(StreamingQuote quote);

	public OHLCquote getOHLCDataByTimeRange(String instrumentToken, String prevTime, String currTime);

	public List<StreamingQuote> getQuoteListByTimeRange(String instrumentToken, String prevTime, String currTime);

	void storeData(List<StreamingQuoteModeQuote> quoteList, String tickType);

	public String[] getTopPrioritizedTokenList(int i);
}
