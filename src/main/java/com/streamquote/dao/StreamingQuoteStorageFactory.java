package com.streamquote.dao;

import com.streamquote.app.StreamingConfig;

public class StreamingQuoteStorageFactory {

	/**
	 * getStreamingQuoteStorage - StreamingQuoteStorage Instance provider
	 * factory
	 * 
	 * @return StreamingQuoteStorage Instance
	 */
	public static IStreamingQuoteStorage getStreamingQuoteStorage() {
		IStreamingQuoteStorage streamingQuoteStorage = null;

		if (StreamingConfig.getStreamingQuoteMode().equals(
				StreamingConfig.QUOTE_STREAMING_MODE_LTP)) {
			streamingQuoteStorage = new StreamingQuoteDAOModeLtp();
		} else if (StreamingConfig.getStreamingQuoteMode().equals(
				StreamingConfig.QUOTE_STREAMING_MODE_QUOTE)) {
			streamingQuoteStorage = new StreamingQuoteDAOModeQuote();
		} else if (StreamingConfig.getStreamingQuoteMode().equals(
				StreamingConfig.QUOTE_STREAMING_MODE_FULL)) {
			streamingQuoteStorage = new StreamingQuoteDAOModeFull();
		} else {
			System.out
					.println("StreamingQuoteStorageFactory.getStreamingQuoteStorage(): ERROR: "
							+ "Current DB storage type not supported for Quote type ["
							+ StreamingConfig.getStreamingQuoteMode() + "]");
		}

		return streamingQuoteStorage;
	}
}
