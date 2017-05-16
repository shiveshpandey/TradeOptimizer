package com.streamquote.dao;

public class StreamingQuoteStorageFactory {

	public static StreamingQuoteStorage getStreamingQuoteStorage() {

		return new StreamingQuoteStorageImpl();
	}
}
