package com.streamquote.dao;

import com.streamquote.utils.StreamingConfig;

public class StreamingQuoteStorageFactory {

	/**
	 * getStreamingQuoteStorage - StreamingQuoteStorage Instance provider
	 * factory
	 * 
	 * @return StreamingQuoteStorage Instance
	 */
	public static StreamingQuoteStorage getStreamingQuoteStorage() {
		StreamingQuoteStorage streamingQuoteStorage = null;

		if (StreamingConfig.QUOTE_STREAMING_DEFAULT_MODE.equals(StreamingConfig.QUOTE_STREAMING_MODE_QUOTE)) {
			streamingQuoteStorage = new StreamingQuoteStorageImpl();
		} else {
			System.out.println("StreamingQuoteStorageFactory.getStreamingQuoteStorage(): ERROR: "
					+ "Current DB storage type not supported for Quote type ["
					+ StreamingConfig.QUOTE_STREAMING_DEFAULT_MODE + "]");
		}

		return streamingQuoteStorage;
	}
}
