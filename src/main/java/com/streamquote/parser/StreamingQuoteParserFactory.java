package com.streamquote.parser;

import com.streamquote.app.StreamingConfig;

public class StreamingQuoteParserFactory {

	/**
	 * getStreamingQuoteParser - StreamingQuoteParser Instance provider factory
	 * 
	 * @return StreamingQuoteParser Instance
	 */
	public static IStreamingQuoteParser getStreamingQuoteParser() {
		IStreamingQuoteParser streamingQuoteParser = null;

		if (StreamingConfig.getStreamingQuoteMode().equals(
				StreamingConfig.QUOTE_STREAMING_MODE_LTP)) {
			streamingQuoteParser = new StreamingQuoteParserModeLtp();
		} else if (StreamingConfig.getStreamingQuoteMode().equals(
				StreamingConfig.QUOTE_STREAMING_MODE_QUOTE)) {
			streamingQuoteParser = new StreamingQuoteParserModeQuote();
		} else if (StreamingConfig.getStreamingQuoteMode().equals(
				StreamingConfig.QUOTE_STREAMING_MODE_FULL)) {
			streamingQuoteParser = new StreamingQuoteParserModeFull();
		} else {
			System.out
					.println("StreamingQuoteParserFactory.getStreamingQuoteParser(): ERROR: "
							+ "Current Parsing Strategy not supported for Quote type ["
							+ StreamingConfig.getStreamingQuoteMode() + "]");
		}

		return streamingQuoteParser;
	}
}
