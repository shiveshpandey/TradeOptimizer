package com.trade.optimizer.main;

import java.io.IOException;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Date;
import java.util.List;
import java.util.TimeZone;

import javax.servlet.ServletContext;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.json.JSONException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.servlet.view.RedirectView;

import com.streamquote.dao.StreamingQuoteStorage;
import com.streamquote.dao.StreamingQuoteStorageImpl;
import com.streamquote.model.StreamingQuoteModeQuote;
import com.streamquote.utils.StreamingConfig;
import com.streamquote.websocket.WebsocketThread;
import com.trade.optimizer.exceptions.KiteException;
import com.trade.optimizer.kiteconnect.KiteConnect;
import com.trade.optimizer.kiteconnect.SessionExpiryHook;
import com.trade.optimizer.models.HistoricalData;
import com.trade.optimizer.models.Holding;
import com.trade.optimizer.models.Instrument;
import com.trade.optimizer.models.Order;
import com.trade.optimizer.models.UserModel;

@Controller
public class TradeOptimizer {

	private static StreamingQuoteStorage streamingQuoteDAOModeQuote = new StreamingQuoteStorageImpl();
	private static StreamingQuoteModeQuote quote = null;
	private static HistoricalData historicalData = null;
	private static TimeZone timeZone = TimeZone.getTimeZone("IST");
	private static String todaysDate = null;
	static DateFormat dtFmt = new SimpleDateFormat("yyyy-MM-dd");
	private static boolean streamingQuoteStarted = false;
	private static WebsocketThread websocketThread = null;

	private static StreamingQuoteStorage streamingQuoteStorage = null;

	private KiteConnect kiteconnect = new KiteConnect(StreamingConfig.API_KEY);

	private String url = kiteconnect.getLoginUrl();

	@Autowired
	ServletContext context;
	@Autowired
	HttpServletRequest request;
	@Autowired
	HttpServletResponse response;

	@RequestMapping(value = "/AuthRedirectWithToken", method = RequestMethod.GET)
	public String authRedirectWithTokenPost(@RequestParam String status, @RequestParam String request_token) {
		try {
			UserModel userModel = kiteconnect.requestAccessToken(request_token, StreamingConfig.API_SECRET_KEY);
			kiteconnect.setAccessToken(userModel.accessToken);
			kiteconnect.setPublicToken(userModel.publicToken);
			startProcess();
		} catch (JSONException | KiteException e) {
			e.printStackTrace();
			return "error";
		}
		return "index";
	}

	@RequestMapping(value = "/start", method = { RequestMethod.POST, RequestMethod.GET })
	public RedirectView localRedirect() {
		RedirectView redirectView = new RedirectView();
		redirectView.setUrl(url);
		return redirectView;
	}

	public void startProcess() {
		try {

			dtFmt.setTimeZone(timeZone);
			todaysDate = dtFmt.format(Calendar.getInstance(timeZone).getTime());

			kiteconnect.registerHook(new SessionExpiryHook() {
				@Override
				public void sessionExpired() {
					System.out.println("session expired");
				}
			});

			createInitialDayTables();

			startStreamForHistoricalInstrumentsData(kiteconnect);

			startLiveStreamOfSelectedInstruments(kiteconnect);

			checkForSignalsFromStrategies();

			executeOrdersOnSignals(kiteconnect);

			checkAndProcessPendingOrdersOnMarketQueue(kiteconnect);

			closingDayRoundOffOperations(kiteconnect);

		} catch (JSONException e) {
			e.printStackTrace();
		} catch (KiteException e) {
			e.printStackTrace();
		}
	}

	private void checkAndProcessPendingOrdersOnMarketQueue(KiteConnect kiteconnect) {
		// TODO Auto-generated method stub
	}

	private void createInitialDayTables() {
		DateFormat quoteTableDtFmt = new SimpleDateFormat("ddMMyyyy");
		quoteTableDtFmt.setTimeZone(timeZone);
		String date = quoteTableDtFmt.format(Calendar.getInstance(timeZone).getTime());

		if (StreamingConfig.QUOTE_STREAMING_DB_STORE_REQD && (streamingQuoteStorage != null)) {

			streamingQuoteStorage.initializeJDBCConn();
			streamingQuoteStorage.createDaysStreamingQuoteTable(date);
		}
	}

	private void closingDayRoundOffOperations(final KiteConnect kiteconnect) throws KiteException {

		Thread t = new Thread(new Runnable() {
			private boolean runnable = true;
			private DateFormat dtFmt = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
			private TimeZone timeZone = TimeZone.getTimeZone("IST");
			private String quoteEndTime = todaysDate + " " + StreamingConfig.QUOTE_STREAMING_END_TIME;
			List<Order> orderList = new TradeOperations().getOrders(kiteconnect);
			List<Holding> holdings = new TradeOperations().getHoldings(kiteconnect).holdings;

			@Override
			public void run() {
				dtFmt.setTimeZone(timeZone);
				try {
					Date timeEnd = dtFmt.parse(quoteEndTime);
					while (runnable) {
						Date timeNow = Calendar.getInstance(timeZone).getTime();
						if (timeNow.compareTo(timeEnd) >= 0) {
							runnable = false;
							for (int index = 0; index < orderList.size(); index++) {
								if (orderList.get(index).status.equalsIgnoreCase("OPEN")) {
									TradeOperations ex = new TradeOperations();
									ex.cancelOrder(kiteconnect, orderList.get(index));
								}
							}
							for (int index = 0; index < holdings.size(); index++) {
								TradeOperations ex = new TradeOperations();
								ex.placeOrder(kiteconnect, holdings.get(index).instrumentToken, "SELL",
										holdings.get(index).quantity);
							}
						} else {
							Thread.sleep(1000);
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

	private void executeOrdersOnSignals(final KiteConnect kiteconnect) {

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
								ex.placeOrder(kiteconnect, signalList.get(index).symbol,
										signalList.get(index).transactionType, signalList.get(index).quantity);
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

	private List<Order> getOrderListToPlaceAsPerStrategySignals() {
		return streamingQuoteStorage.getOrderListToPlace();
	}

	private void startLiveStreamOfSelectedInstruments(final KiteConnect kiteconnect) {
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
							Thread.sleep(1000);
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

	private void startStreamForHistoricalInstrumentsData(final KiteConnect kiteconnect) {
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

							HistoricalData histData;
							for (int index = 0; index < instrumentList.size(); index++) {

								long dayHighVolume = 0, dayLowVolume = 1999999999, dayCloseVolume = 0;
								double dayHighPrice = 0, dayLowPrice = 1999999999, dayClosePrice = 0;

								historicalData = tradeOperations.getHistoricalData(kiteconnect,
										StreamingConfig.HIST_DATA_START_DATE, StreamingConfig.HIST_DATA_END_DATE,
										"minute", Long.toString(instrumentList.get(index).getInstrument_token()));
								for (int count = 0; count < historicalData.dataArrayList.size(); count++) {
									histData = historicalData.dataArrayList.get(count);

									if (dayHighVolume < Long.valueOf(histData.volume))
										dayHighVolume = Long.valueOf(histData.volume);
									if (dayLowVolume > Long.valueOf(histData.volume))
										dayLowVolume = Long.valueOf(histData.volume);
									if (count == historicalData.dataArrayList.size() - 1)
										dayCloseVolume = Long.valueOf(histData.volume);

									if (dayHighPrice < Double.valueOf(histData.high))
										dayHighPrice = Double.valueOf(histData.high);
									if (dayLowPrice > Double.valueOf(histData.low))
										dayLowPrice = Double.valueOf(histData.low);
									if (count == historicalData.dataArrayList.size() - 1)
										dayClosePrice = Double.valueOf(histData.close);

									double priorityPoint = (dayClosePrice / (dayHighPrice - dayLowPrice)) * 100
											* (dayCloseVolume / (dayHighVolume - dayLowVolume));

									quote = new StreamingQuoteModeQuote(
											historicalData.dataArrayList.get(count).timeStamp,
											Long.toString(instrumentList.get(index).getInstrument_token()),
											new Double(priorityPoint), new Long(0), new Double(0),
											Long.valueOf(histData.volume), new Long(0), new Long(0),
											new Double(histData.open), new Double(histData.high),
											new Double(histData.low), new Double(histData.close));
									quoteList.add(quote);
								}
								streamingQuoteDAOModeQuote.storeData(quoteList, "minute");
							}
							streamingQuoteDAOModeQuote.saveInstrumentDetails(instrumentList, new Date().toString());

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

	private void roundOfNonPerformingBoughtStocks(String[] qUOTE_STREAMING_INSTRUMENTS_ARR, List<Order> list,
			KiteConnect kiteconnect) throws KiteException {

		for (int index = 0; index < list.size(); index++) {
			for (int count = 0; count < qUOTE_STREAMING_INSTRUMENTS_ARR.length; count++) {
				if (list.get(index).tradingSymbol.equalsIgnoreCase(qUOTE_STREAMING_INSTRUMENTS_ARR[count]))
					if (list.get(index).status.equalsIgnoreCase("OPEN")) {
						TradeOperations ex = new TradeOperations();
						ex.cancelOrder(kiteconnect, list.get(index));
					}
			}
		}
		List<Holding> holdings = new TradeOperations().getHoldings(kiteconnect).holdings;

		for (int index = 0; index < holdings.size(); index++) {
			for (int count = 0; count < qUOTE_STREAMING_INSTRUMENTS_ARR.length; count++) {
				if (holdings.get(index).instrumentToken.equalsIgnoreCase(qUOTE_STREAMING_INSTRUMENTS_ARR[count])) {
					TradeOperations ex = new TradeOperations();
					ex.placeOrder(kiteconnect, holdings.get(index).instrumentToken, "SELL",
							holdings.get(index).quantity);
				}
			}
		}
	}

	private void checkForSignalsFromStrategies() {

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

	private void startStreamingQuote(String apiKey, String userId, String publicToken) {
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

	private List<String> getInstrumentTokensList() {
		String[] instrumentsArr = StreamingConfig.QUOTE_STREAMING_INSTRUMENTS_ARR;
		List<String> instrumentList = Arrays.asList(instrumentsArr);
		return instrumentList;
	}

	private void stopStreamingQuote() {
		if (streamingQuoteStarted && websocketThread != null) {
			websocketThread.stopWS();
			try {
				Thread.sleep(1000);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}

		if (StreamingConfig.QUOTE_STREAMING_DB_STORE_REQD && (streamingQuoteStorage != null)) {
			streamingQuoteStorage.closeJDBCConn();
		}
	}
}
