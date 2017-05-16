package com.streamquote.parser;

import com.streamquote.utils.StreamingConfig;

public class StreamingQuoteParserFactory {

	/**
	 * getStreamingQuoteParser - StreamingQuoteParser Instance provider factory
	 * 
	 * @return StreamingQuoteParser Instance
	 */
	public static StreamingQuoteParser getStreamingQuoteParser() {
		StreamingQuoteParser streamingQuoteParser = null;

		if (StreamingConfig.QUOTE_STREAMING_DEFAULT_MODE.equals(StreamingConfig.QUOTE_STREAMING_MODE_QUOTE)) {
			streamingQuoteParser = new StreamingQuoteParserImpl();
		} else {
			System.out.println("StreamingQuoteParserFactory.getStreamingQuoteParser(): ERROR: "
					+ "Current Parsing Strategy not supported for Quote type ["
					+ StreamingConfig.QUOTE_STREAMING_DEFAULT_MODE + "]");
		}

		return streamingQuoteParser;
	}
}
