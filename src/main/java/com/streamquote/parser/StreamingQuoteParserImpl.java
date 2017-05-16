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

		return new StreamingQuoteModeQuote(time, convertIntToIntString(instrumentToken), convertIntToDouble(ltp),
				convertIntToLong(lastTradedQty), convertIntToDouble(avgTradedPrice), convertIntToLong(vol),
				convertIntToLong(buyQty), convertIntToLong(sellQty), convertIntToDouble(openPrice),
				convertIntToDouble(highPrice), convertIntToDouble(lowPrice), convertIntToDouble(closePrice));
	}

	private String convertIntToIntString(int quoteParam) {
		String quoteParamString = (new Integer(quoteParam)).toString();
		return quoteParamString;
	}

	private Double convertIntToDouble(int quoteParam) {
		return Double.valueOf(quoteParam);
	}

	private Long convertIntToLong(int quoteParam) {
		Long quoteParamLong = new Long(convertIntToIntString(quoteParam));
		return quoteParamLong;
	}
}
