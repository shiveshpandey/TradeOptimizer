package com.streamquote.parser;

public class StreamingQuoteParserFactory {

	public static StreamingQuoteParser getStreamingQuoteParser() {
		return new StreamingQuoteParserImpl();
	}
}
