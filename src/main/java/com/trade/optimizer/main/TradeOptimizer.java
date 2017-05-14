package com.trade.optimizer.main;

import java.io.IOException;
import java.math.BigDecimal;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Date;
import java.util.List;
import java.util.TimeZone;

import org.json.JSONException;

import com.streamquote.app.StreamingConfig;
import com.streamquote.dao.IStreamingQuoteStorage;
import com.streamquote.dao.StreamingQuoteDAOModeQuote;
import com.streamquote.model.StreamingQuoteModeQuote;
import com.streamquote.websocket.WebsocketThread;
import com.trade.optimizer.exceptions.KiteException;
import com.trade.optimizer.kiteconnect.KiteConnect;
import com.trade.optimizer.kiteconnect.SessionExpiryHook;
import com.trade.optimizer.models.HistoricalData;
import com.trade.optimizer.models.Instrument;
import com.trade.optimizer.models.Order;

public class TradeOptimizer {

	private static IStreamingQuoteStorage streamingQuoteDAOModeQuote = new StreamingQuoteDAOModeQuote();
	private static StreamingQuoteModeQuote quote = null;
	private static HistoricalData historicalData = null;
	private static TimeZone timeZone = TimeZone.getTimeZone("IST");
	private static String todaysDate = null;
	static DateFormat dtFmt = new SimpleDateFormat("yyyy-MM-dd");
	private static boolean streamingQuoteStarted = false;
	private static WebsocketThread websocketThread = null;

	private static IStreamingQuoteStorage streamingQuoteStorage = null;

	public static void main(String[] args) {
		try {
			dtFmt.setTimeZone(timeZone);
			todaysDate = dtFmt.format(Calendar.getInstance(timeZone).getTime());

			KiteConnect kiteconnect = new KiteConnect("nwjyiwe@qm6z4pmii");
			kiteconnect.setUserId("RS4216");

			@SuppressWarnings("unused")
			String url = kiteconnect.getLoginUrl();

			kiteconnect.registerHook(new SessionExpiryHook() {
				@Override
				public void sessionExpired() {
					System.out.println("session expired");
				}
			});

			// UserModel userModel =
			// kiteconnect.requestAccessToken("z45ui9@l8qgewru3bokx52eh35zgjmg7p",
			// "67s1q1k5yl9il439@f80nrv6fbunmwhq6");
			//
			// kiteconnect.setAccessToken(userModel.accessToken);
			// kiteconnect.setPublicToken(userModel.publicToken);

			createInitialDayTables();

			startStreamForHistoricalInstrumentsData(kiteconnect);

			startLiveStreamOfSelectedInstruments(kiteconnect);

			checkForSignalsFromStrategies();

			executeOrdersOnSignals(kiteconnect);

			closingDayRoundOffOperations(kiteconnect);

		} catch (JSONException e) {
			e.printStackTrace();
		} catch (KiteException e) {
			e.printStackTrace();
		}
	}

	private static void createInitialDayTables() {
		DateFormat quoteTableDtFmt = new SimpleDateFormat("ddMMyyyy");
		quoteTableDtFmt.setTimeZone(timeZone);
		String date = quoteTableDtFmt.format(Calendar.getInstance(timeZone).getTime());

		if (StreamingConfig.isStreamingQuoteStoringRequired() && (streamingQuoteStorage != null)) {

			streamingQuoteStorage.initializeJDBCConn();
			streamingQuoteStorage.createDaysStreamingQuoteTable(date);
		}
	}

	private static void closingDayRoundOffOperations(final KiteConnect kiteconnect) throws KiteException {

		Thread t = new Thread(new Runnable() {
			private boolean runnable = true;
			private DateFormat dtFmt = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
			private TimeZone timeZone = TimeZone.getTimeZone("IST");
			private String quoteEndTime = todaysDate + " " + StreamingConfig.QUOTE_STREAMING_END_TIME;
			List<Order> list = new TradeOperations().getOrders(kiteconnect);

			@Override
			public void run() {
				dtFmt.setTimeZone(timeZone);
				try {
					Date timeEnd = dtFmt.parse(quoteEndTime);
					while (runnable) {
						Date timeNow = Calendar.getInstance(timeZone).getTime();
						if (timeNow.compareTo(timeEnd) >= 0) {
							runnable = false;
							for (int index = 0; index < list.size(); index++) {
								int rountOffRequired = 0;
								if (list.get(index).status.equalsIgnoreCase("OPEN"))
									rountOffRequired = 1;
								if (list.get(index).status.equalsIgnoreCase("SELL"))
									rountOffRequired = 2;
								if (list.get(index).status.equalsIgnoreCase("BUY"))
									rountOffRequired = 3;

								if (rountOffRequired == 1) {
									TradeOperations ex = new TradeOperations();
									ex.cancelOrder(kiteconnect);
								}
								if (rountOffRequired == 2) {// BUY Here
									TradeOperations ex = new TradeOperations();
									ex.placeOrder(kiteconnect);
								}
								if (rountOffRequired == 3) {// SELL Here
									TradeOperations ex = new TradeOperations();
									ex.placeOrder(kiteconnect);
								}
							}
						} else {
							Thread.sleep(1800000);
							runnable = true;
						}
					}
				} catch (ParseException e) {
					e.printStackTrace();
				} catch (InterruptedException e) {
					e.printStackTrace();
				} catch (KiteException e) {
					e.printStackTrace();
				}
			}
		});
		t.start();
	}

	private static void executeOrdersOnSignals(final KiteConnect kiteconnect) {

		Thread t = new Thread(new Runnable() {
			private boolean runnable = true;
			private DateFormat dtFmt = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
			private TimeZone timeZone = TimeZone.getTimeZone("IST");
			private String quoteStartTime = todaysDate + " " + StreamingConfig.QUOTE_STREAMING_START_TIME;
			private String quoteEndTime = todaysDate + " " + StreamingConfig.QUOTE_STREAMING_END_TIME;

			@Override
			public void run() {
				dtFmt.setTimeZone(timeZone);
				try {
					Date timeStart = dtFmt.parse(quoteStartTime);
					Date timeEnd = dtFmt.parse(quoteEndTime);
					while (runnable) {
						Date timeNow = Calendar.getInstance(timeZone).getTime();
						if (timeNow.compareTo(timeStart) >= 0 && timeNow.compareTo(timeEnd) <= 0) {
							TradeOperations ex = new TradeOperations();
							List<Order> signalList = getOrderListToPlaceAsPerStrategySignals();
							for (int index = 0; index < signalList.size(); index++)
								ex.placeOrder(kiteconnect);
							Thread.sleep(1000);
						} else {
							runnable = false;
						}
					}
				} catch (ParseException e) {
					e.printStackTrace();
				} catch (InterruptedException e) {
					e.printStackTrace();
				} catch (KiteException e) {
					e.printStackTrace();
				}
			}
		});
		t.start();
	}

	private static List<Order> getOrderListToPlaceAsPerStrategySignals() {
		// TODO Auto-generated method stub
		return null;
	}

	private static void startLiveStreamOfSelectedInstruments(final KiteConnect kiteconnect) {
		Thread t = new Thread(new Runnable() {
			private boolean runnable = true;
			private DateFormat dtFmt = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
			private TimeZone timeZone = TimeZone.getTimeZone("IST");
			private String quoteStartTime = todaysDate + " " + StreamingConfig.QUOTE_STREAMING_START_TIME;
			private String quoteEndTime = todaysDate + " " + StreamingConfig.QUOTE_STREAMING_END_TIME;

			@Override
			public void run() {
				dtFmt.setTimeZone(timeZone);
				try {
					Date timeStart = dtFmt.parse(quoteStartTime);
					Date timeEnd = dtFmt.parse(quoteEndTime);
					while (runnable) {
						Date timeNow = Calendar.getInstance(timeZone).getTime();
						if (timeNow.compareTo(timeStart) >= 0 && timeNow.compareTo(timeEnd) <= 0) {
							startStreamingQuote(kiteconnect.getApiKey(), kiteconnect.getUserId(),
									kiteconnect.getPublicToken());
							Thread.sleep(10000);
						} else {
							runnable = false;
							stopStreamingQuote();
						}
					}
				} catch (ParseException e) {
					e.printStackTrace();
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}
		});
		t.start();
	}

	private static void startStreamForHistoricalInstrumentsData(final KiteConnect kiteconnect) {
		Thread t = new Thread(new Runnable() {
			private boolean runnable = true;
			private DateFormat dtFmt = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
			private TimeZone timeZone = TimeZone.getTimeZone("IST");
			private String quoteStartTime = todaysDate + " " + StreamingConfig.HISTORICAL_DATA_STREAM_START_TIME;
			private String quoteEndTime = todaysDate + " " + StreamingConfig.QUOTE_STREAMING_END_TIME;

			@Override
			public void run() {
				dtFmt.setTimeZone(timeZone);
				try {
					Date timeStart = dtFmt.parse(quoteStartTime);
					Date timeEnd = dtFmt.parse(quoteEndTime);
					while (runnable) {
						Date timeNow = Calendar.getInstance(timeZone).getTime();
						if (timeNow.compareTo(timeStart) >= 0 && timeNow.compareTo(timeEnd) <= 0) {
							TradeOperations tradeOperations = new TradeOperations();

							List<Instrument> instrumentList = tradeOperations.getInstrumentsForExchange(kiteconnect,
									"NSE");
							instrumentList.addAll(tradeOperations.getInstrumentsForExchange(kiteconnect, "BSE"));
							instrumentList.addAll(tradeOperations.getInstrumentsForExchange(kiteconnect, "NFO"));
							instrumentList.addAll(tradeOperations.getInstrumentsForExchange(kiteconnect, "BFO"));

							List<StreamingQuoteModeQuote> quoteList = new ArrayList<StreamingQuoteModeQuote>();
							for (int index = 0; index < instrumentList.size(); index++) {
								historicalData = tradeOperations.getHistoricalData(kiteconnect, "2017-05-08",
										"2017-05-12", "minute",
										Long.toString(instrumentList.get(index).getInstrument_token()));
								for (int count = 0; count < historicalData.dataArrayList.size(); count++) {
									quote = new StreamingQuoteModeQuote(
											historicalData.dataArrayList.get(count).timeStamp,
											Long.toString(instrumentList.get(index).getInstrument_token()),
											new BigDecimal(0), new Long(0), new BigDecimal(0),
											Long.valueOf(historicalData.dataArrayList.get(count).volume), new Long(0),
											new Long(0), new BigDecimal(historicalData.dataArrayList.get(count).open),
											new BigDecimal(historicalData.dataArrayList.get(count).high),
											new BigDecimal(historicalData.dataArrayList.get(count).low),
											new BigDecimal(historicalData.dataArrayList.get(count).close));
									quoteList.add(quote);
								}
								streamingQuoteDAOModeQuote.storeData(quoteList, "minute");
							}

							StreamingConfig.QUOTE_STREAMING_INSTRUMENTS_ARR = streamingQuoteDAOModeQuote
									.getTopPrioritizedTokenList(10);

							roundOfNonPerformingBoughtStocks(StreamingConfig.QUOTE_STREAMING_INSTRUMENTS_ARR,
									tradeOperations.getOrders(kiteconnect), kiteconnect);

							Thread.sleep(3600);
						} else {
							runnable = false;
						}
					}
				} catch (ParseException e) {
					e.printStackTrace();
				} catch (InterruptedException e) {
					e.printStackTrace();
				} catch (IOException e) {
					e.printStackTrace();
				} catch (KiteException e) {
					e.printStackTrace();
				}
			}
		});
		t.start();
	}

	private static void roundOfNonPerformingBoughtStocks(String[] qUOTE_STREAMING_INSTRUMENTS_ARR, List<Order> list,
			KiteConnect kiteconnect) throws KiteException {
		for (int index = 0; index < list.size(); index++) {
			int rountOffRequired = 0;
			for (int count = 0; count < qUOTE_STREAMING_INSTRUMENTS_ARR.length; count++) {
				if (list.get(index).tradingSymbol.equalsIgnoreCase(qUOTE_STREAMING_INSTRUMENTS_ARR[count]))
					if (list.get(index).status.equalsIgnoreCase("OPEN"))
						rountOffRequired = 1;
				if (list.get(index).status.equalsIgnoreCase("SELL"))
					rountOffRequired = 2;
				if (list.get(index).status.equalsIgnoreCase("BUY"))
					rountOffRequired = 3;
			}
			if (rountOffRequired == 1) {
				TradeOperations ex = new TradeOperations();
				ex.cancelOrder(kiteconnect);
			}
			if (rountOffRequired == 2) {// BUY Here
				TradeOperations ex = new TradeOperations();
				ex.placeOrder(kiteconnect);
			}
			if (rountOffRequired == 3) {// SELL Here
				TradeOperations ex = new TradeOperations();
				ex.placeOrder(kiteconnect);
			}
		}
	}

	private static void checkForSignalsFromStrategies() {

		Thread t = new Thread(new Runnable() {
			private boolean runnable = true;
			private DateFormat dtFmt = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
			private TimeZone timeZone = TimeZone.getTimeZone("IST");
			private String quoteStartTime = todaysDate + " " + StreamingConfig.QUOTE_STREAMING_START_TIME;
			private String quoteEndTime = todaysDate + " " + StreamingConfig.QUOTE_STREAMING_END_TIME;

			@Override
			public void run() {
				dtFmt.setTimeZone(timeZone);
				try {
					Date timeStart = dtFmt.parse(quoteStartTime);
					Date timeEnd = dtFmt.parse(quoteEndTime);
					while (runnable) {
						Date timeNow = Calendar.getInstance(timeZone).getTime();
						if (timeNow.compareTo(timeStart) >= 0 && timeNow.compareTo(timeEnd) <= 0) {
							Thread.sleep(1000);
						} else {
							runnable = false;
						}
					}
				} catch (ParseException e) {
					e.printStackTrace();
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}
		});
		t.start();
	}

	private static void startStreamingQuote(String apiKey, String userId, String publicToken) {
		String URIstring = StreamingConfig.STREAMING_QUOTE_WS_URL_TEMPLATE + "api_key=" + apiKey + "&user_id=" + userId
				+ "&public_token=" + publicToken;

		List<String> instrumentList = getInstrumentTokensList();

		websocketThread = new WebsocketThread(URIstring, instrumentList, streamingQuoteStorage);
		streamingQuoteStarted = websocketThread.startWS();
		if (streamingQuoteStarted) {
			Thread t = new Thread(websocketThread);
			t.start();
		} else {
			System.out.println(
					"ZStreamingQuoteControl.startStreamingQuote(): ERROR: WebSocket Streaming Quote not started !!!");
		}
	}

	private static List<String> getInstrumentTokensList() {
		String[] instrumentsArr = StreamingConfig.getInstrumentTokenArr();
		List<String> instrumentList = Arrays.asList(instrumentsArr);
		return instrumentList;
	}

	private static void stopStreamingQuote() {
		if (streamingQuoteStarted && websocketThread != null) {
			websocketThread.stopWS();
			try {
				Thread.sleep(1000);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}

		if (StreamingConfig.isStreamingQuoteStoringRequired() && (streamingQuoteStorage != null)) {
			streamingQuoteStorage.closeJDBCConn();
		}
	}

}
