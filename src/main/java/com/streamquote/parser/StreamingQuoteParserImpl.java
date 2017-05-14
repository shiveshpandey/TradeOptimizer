package com.streamquote.parser;

import java.nio.ByteBuffer;

import com.streamquote.model.StreamingQuote;
import com.streamquote.model.StreamingQuoteModeQuote;

public class StreamingQuoteParserImpl implements StreamingQuoteParser {

	@Override
	public StreamingQuote parse(ByteBuffer pktBuffer, String time) {
		int instrumentToken = pktBuffer.getInt();
		int ltp = pktBuffer.getInt();
		int lastTradedQty = pktBuffer.getInt();
		int avgTradedPrice = pktBuffer.getInt();
		int vol = pktBuffer.getInt();
		int buyQty = pktBuffer.getInt();
		int sellQty = pktBuffer.getInt();
		int openPrice = pktBuffer.getInt();
		int highPrice = pktBuffer.getInt();
		int lowPrice = pktBuffer.getInt();
		int closePrice = pktBuffer.getInt();

		StreamingQuote streamingQuote = new StreamingQuoteModeQuote(time, convertIntToIntString(instrumentToken),
				convertIntToDouble(ltp), convertIntToLong(lastTradedQty), convertIntToDouble(avgTradedPrice),
				convertIntToLong(vol), convertIntToLong(buyQty), convertIntToLong(sellQty),
				convertIntToDouble(openPrice), convertIntToDouble(highPrice), convertIntToDouble(lowPrice),
				convertIntToDouble(closePrice));

		return streamingQuote;
	}

	/**
	 * convertIntToIntString - private method to convert Integer to Integer
	 * String
	 * 
	 * @param quoteParam
	 * @return Integer String
	 */
	private String convertIntToIntString(int quoteParam) {
		String quoteParamString = (new Integer(quoteParam)).toString();
		return quoteParamString;
	}

	/**
	 * convertIntToDouble - private method to convert int to Double
	 * 
	 * @param quoteParam
	 * @return Double value
	 */
	private Double convertIntToDouble(int quoteParam) {
		return Double.valueOf(quoteParam);
	}

	/**
	 * convertIntToLong - private method to convert int to Long
	 * 
	 * @param quoteParam
	 * @return Long value
	 */
	private Long convertIntToLong(int quoteParam) {
		Long quoteParamLong = new Long(convertIntToIntString(quoteParam));
		return quoteParamLong;
	}
}
