package com.streamquote.parser;

import java.nio.ByteBuffer;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.TimeZone;
import java.util.concurrent.BlockingQueue;

import com.streamquote.dao.StreamingQuoteStorage;
import com.streamquote.model.StreamingQuote;
import com.streamquote.utils.StreamingConfig;

public class StreamingQuoteParserThread implements Runnable {
	// Quote Buffer Queue
	private BlockingQueue<Object> quoteBufferQ = null;

	// running status
	private boolean runStatus = false;

	// Time formats
	private DateFormat timeFormat = null;
	private TimeZone timeZone = null;

	// Quote Parser
	private StreamingQuoteParser streamingQuoteParser = null;

	// Quote Storage
	private StreamingQuoteStorage streamingQuoteStorage = null;

	/**
	 * constructor
	 * 
	 * @param quoteBufferQ
	 * @param streamingQuoteStorage
	 */
	public StreamingQuoteParserThread(BlockingQueue<Object> quoteBufferQ, StreamingQuoteStorage streamingQuoteStorage) {
		this.quoteBufferQ = quoteBufferQ;
		this.runStatus = true;

		// get quote parser
		streamingQuoteParser = StreamingQuoteParserFactory.getStreamingQuoteParser();
		if (streamingQuoteParser == null) {
//			LOGGER.info(
//					"StreamingQuoteParserThread.StreamingQuoteParserThread(): ERROR: Quote Parser is null... !!!");
		}

		if (StreamingConfig.QUOTE_STREAMING_DB_STORE_REQD) {
			this.streamingQuoteStorage = streamingQuoteStorage;
		}

		timeFormat = new SimpleDateFormat("HH:mm:ss");
		timeZone = TimeZone.getTimeZone("IST");
		timeFormat.setTimeZone(timeZone);
	}

	@Override
	public void run() {
		while (runStatus) {
			// listen for any quote buffer in Q
			try {
				Object bufferObj = quoteBufferQ.take();
				if (bufferObj instanceof ByteBuffer) {
					String time = timeFormat.format(Calendar.getInstance(timeZone).getTime());
					ByteBuffer buffer = (ByteBuffer) bufferObj;
					try {
						parseBuffer(buffer, time);
					} catch (Exception e) {
						// If its an exception while parsing the data, we dont
						// require this data
//						LOGGER.info(
//								"StreamingQuoteParserThread.run(): ERROR: Exception in parsing the Buffer, reason: "
//										+ e.getMessage());
					}
				}
			} catch (InterruptedException e) {
//				LOGGER.info(
//						"StreamingQuoteParserThread.run(): ERROR: InterruptedException on take from quoteBufferQ");
//				e.printStackTrace();
			}
		}
	}

	/**
	 * stopThread - method to stop the quote parser thread
	 */
	public void stopThread() {
		runStatus = false;
	}

	/**
	 * parseBuffer - private method to parse the bytebuffer recieved from WS
	 * 
	 * @param buffer
	 * @param time
	 */
	@SuppressWarnings("unused")
	private void parseBuffer(ByteBuffer buffer, String time) {
		// start parse Buffer array
		int start = buffer.position();
		int buffLen = buffer.capacity();
		if (buffLen == 1) {
			// HeartBeat
			if (StreamingConfig.QUOTE_STREAMING_HEART_BIT_MSG_PRINT) {
				//LOGGER.info("StreamingQuoteParserThread.parseBuffer(): WS HEARTBIT Byte");
			}
		} else {
			// num of Packets
			int numPackets = buffer.getShort();
			if (numPackets == 0) {
				// Invalid Case: Zero Num of Packets - ignore
                // LOGGER.info(
//						"StreamingQuoteParserThread.parseBuffer(): ERROR: WS Byte numPackets is 0 in WS message, Ignoring !!!");
			} else {
				start += 2;
				// LOGGER.info("numPackets: " + numPackets);
				for (int i = 0; i < numPackets; i++) {
					// each packet
					// LOGGER.info("packet num: " + i);
					int numBytes = buffer.getShort();
					if (numBytes != 0) {
						// Valid Number of Bytes
						start += 2;

						// packet structure
						byte[] pkt = new byte[numBytes];
						buffer.get(pkt, 0, numBytes);
						ByteBuffer pktBuffer = ByteBuffer.wrap(pkt);
						if (pktBuffer != null) {
							// parse quote packet buffer
							parseQuotePktBuffer(pktBuffer, time);
							start += numBytes;
						} else {
							// Invalid Case: ByteBuffer could not wrap the bytes
							// - ignore
//							LOGGER.info(
//									"StreamingQuoteParserThread.parseBuffer(): ERROR: pktBuffer is null in WS message, Ignoring !!!");
						}
					} else {
						// Invalid Case: Zero Num of Bytes in packet - ignore
//						LOGGER.info(
//								"StreamingQuoteParserThread.parseBuffer(): ERROR: numBytes is 0 in WS message packet["
//										+ i + "], Ignoring !!!");
					}
				}
			}
		}
	}

	/**
	 * parseQuotePktBuffer - private method to parse the Quote packet buffer
	 * 
	 * @param pktBuffer
	 * @param time
	 */
	private void parseQuotePktBuffer(ByteBuffer pktBuffer, String time) {
		StreamingQuote streamingQuote = null;

		if (streamingQuoteParser != null) {
			streamingQuote = streamingQuoteParser.parse(pktBuffer, time);
		}

		if (StreamingConfig.QUOTE_STREAMING_DB_STORE_REQD && (streamingQuoteStorage != null)
				&& streamingQuote != null) {
			streamingQuoteStorage.storeData(streamingQuote);
		}
	}
}
