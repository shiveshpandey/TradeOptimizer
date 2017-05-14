package com.streamquote.parser;

import java.nio.ByteBuffer;

import com.streamquote.model.StreamingQuote;

public interface StreamingQuoteParser {

	public StreamingQuote parse(ByteBuffer pktBuffer, String time);
}
