package com.trade.optimizer.main;

import java.io.IOException;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;
import java.util.logging.Logger;

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

import com.neovisionaries.ws.client.WebSocketException;
import com.streamquote.dao.StreamingQuoteStorage;
import com.streamquote.dao.StreamingQuoteStorageImpl;
import com.streamquote.model.StreamingQuoteModeQuote;
import com.streamquote.utils.StreamingConfig;
import com.trade.optimizer.exceptions.KiteException;
import com.trade.optimizer.kiteconnect.KiteConnect;
import com.trade.optimizer.kiteconnect.SessionExpiryHook;
import com.trade.optimizer.models.HistoricalData;
import com.trade.optimizer.models.Holding;
import com.trade.optimizer.models.Instrument;
import com.trade.optimizer.models.Order;
import com.trade.optimizer.models.Tick;
import com.trade.optimizer.models.UserModel;
import com.trade.optimizer.tickerws.KiteTicker;
import com.trade.optimizer.tickerws.OnConnect;
import com.trade.optimizer.tickerws.OnDisconnect;
import com.trade.optimizer.tickerws.OnTick;

@Controller
public class TradeOptimizer {

	private final static Logger LOGGER = Logger.getLogger(TradeOptimizer.class.getName());

	private boolean liveStreamFirstRun = true;
	boolean firstTimeDayHistoryRun = false;

	private int tokenCountForTrade = 10;
	private int seconds = 1000;
	private int a = 0, b = 0, d = 0, e = 0, f = 0;

	private StreamingQuoteStorage streamingQuoteStorage = new StreamingQuoteStorageImpl();
	private KiteConnect kiteconnect = new KiteConnect(StreamingConfig.API_KEY);
	private TradeOperations tradeOperations = new TradeOperations();
	private List<Instrument> instrumentList = new ArrayList<Instrument>();
	private ArrayList<Long> quoteStreamingInstrumentsArr = new ArrayList<Long>();
	private ArrayList<Long> tokenListForTick = null;
	private KiteTicker tickerProvider;

	private String url = kiteconnect.getLoginUrl();
	private String todaysDate, quoteStartTime, quoteEndTime, histQuoteStartTime, histQuoteEndTime;
	private String histDataStreamEndDateTime = StreamingConfig.HIST_DATA_END_DATE,
			histDataStreamStartDateTime = StreamingConfig.HIST_DATA_START_DATE;

	private SimpleDateFormat dtTmZFmt = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ssZ");
	private DateFormat dtFmt = new SimpleDateFormat("yyyy-MM-dd");
	private DateFormat tmFmt = new SimpleDateFormat("HH:mm:ss");
	private DateFormat dtTmFmt = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
	private Date timeStart, timeEnd, histTimeStart, histTimeEnd;
	private TimeZone timeZone = TimeZone.getTimeZone("IST");

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
			LOGGER.info(e.getMessage());
			return "error";
		}
		return "index";
	}

	@RequestMapping(value = "/start", method = { RequestMethod.POST, RequestMethod.GET })
	public RedirectView localRedirect() {

		kiteconnect.registerHook(new SessionExpiryHook() {
			@Override
			public void sessionExpired() {
				LOGGER.info("session expired");
			}
		});

		RedirectView redirectView = new RedirectView();
		redirectView.setUrl(url);
		return redirectView;
	}

	public void startProcess() {
		try {
			LOGGER.info("startProcess Entry");

			tmFmt.setTimeZone(timeZone);
			dtFmt.setTimeZone(timeZone);
			dtTmFmt.setTimeZone(timeZone);
			dtTmZFmt.setTimeZone(timeZone);

			todaysDate = dtFmt.format(Calendar.getInstance(timeZone).getTime());
			histDataStreamEndDateTime = todaysDate;
			quoteStartTime = todaysDate + " " + StreamingConfig.QUOTE_STREAMING_START_TIME;
			quoteEndTime = todaysDate + " " + StreamingConfig.QUOTE_STREAMING_END_TIME;
			histQuoteStartTime = todaysDate + " " + StreamingConfig.HISTORICAL_DATA_STREAM_START_TIME;
			histQuoteEndTime = todaysDate + " " + StreamingConfig.HISTORICAL_DATA_STREAM_END_TIME;
			timeStart = dtTmFmt.parse(quoteStartTime);
			timeEnd = dtTmFmt.parse(quoteEndTime);
			histTimeStart = dtTmFmt.parse(histQuoteStartTime);
			histTimeEnd = dtTmFmt.parse(histQuoteEndTime);

			createInitialDayTables();

			startStreamForHistoricalInstrumentsData();

			// tickerSettingInitialization();

			startLiveStreamOfSelectedInstruments();

			applyStrategiesAndGenerateSignals();

			executeOrdersOnSignals();

			// checkAndProcessPendingOrdersOnMarketQueue();

			closingDayRoundOffOperations();
			LOGGER.info("startProcess Exit");
		} catch (JSONException | ParseException | KiteException e) {
			LOGGER.info(e.getMessage());
		}
	}

	private void tickerSettingInitialization() {
		tickerProvider = new KiteTicker(kiteconnect);
		tickerProvider.setTryReconnection(true);
		try {
			tickerProvider.setTimeIntervalForReconnection(5);
		} catch (KiteException e) {
			LOGGER.info(e.getMessage());
		}
		tickerProvider.setMaxRetries(10);

		if (null != tokenListForTick && !tokenListForTick.isEmpty())
			tickerProvider.setMode(tokenListForTick, KiteTicker.modeQuote);

		tickerProvider.setOnConnectedListener(new OnConnect() {
			@Override
			public void onConnected() {
				try {
					System.out.println("TradeOptimizer onConnected then subscribe");

					tickerProvider.subscribe(tokenListForTick);
				} catch (IOException e) {
					LOGGER.info(e.getMessage());
				} catch (WebSocketException e) {
					LOGGER.info(e.getMessage());
				} catch (KiteException e) {
					LOGGER.info(e.getMessage());
				}
			}
		});

		tickerProvider.setOnDisconnectedListener(new OnDisconnect() {
			@Override
			public void onDisconnected() {
				System.out.println("TradeOptimizer onDisconnected then unsub");

				tickerProvider.unsubscribe(tokenListForTick);

			}
		});

		tickerProvider.setOnTickerArrivalListener(new OnTick() {
			@Override
			public void onTick(ArrayList<Tick> ticks) {
				System.out.println(ticks.size());

				if (ticks.size() > 0)
					streamingQuoteStorage.storeData(ticks);
			}
		});
	}

	private void createInitialDayTables() {
		LOGGER.info("createInitialDayTables Entry");
		if (streamingQuoteStorage != null) {

			DateFormat quoteTableDtFmt = new SimpleDateFormat("ddMMyyyy");
			quoteTableDtFmt.setTimeZone(timeZone);

			streamingQuoteStorage.initializeJDBCConn();
			try {
				streamingQuoteStorage.createDaysStreamingQuoteTable(
						quoteTableDtFmt.format(Calendar.getInstance(timeZone).getTime()));
			} catch (SQLException e) {
				LOGGER.info(e.getMessage());
			}
		}
		LOGGER.info("createInitialDayTables Exit");
	}

	private void closingDayRoundOffOperations() throws KiteException {
		LOGGER.info("closingDayRoundOffOperations Entry");
		Thread t = new Thread(new Runnable() {
			private boolean runnable = true;

			@Override
			public void run() {
				try {
					while (runnable) {
						Date timeNow = Calendar.getInstance(timeZone).getTime();
						if (timeNow.compareTo(timeEnd) >= 0) {
							runnable = false;
							List<Order> orderList = tradeOperations.getOrders(kiteconnect);
							List<Holding> holdings = tradeOperations.getHoldings(kiteconnect).holdings;

							for (int index = 0; index < orderList.size(); index++) {
								if (orderList.get(index).status.equalsIgnoreCase("OPEN")) {
									tradeOperations.cancelOrder(kiteconnect, orderList.get(index));
								}
							}
							for (int index = 0; index < holdings.size(); index++) {
								tradeOperations.placeOrder(kiteconnect, holdings.get(index).instrumentToken, "SELL",
										holdings.get(index).quantity, streamingQuoteStorage);
							}
						} else {
							LOGGER.info("in closingDayRoundOffOperations" + f++);
							Thread.sleep(10 * seconds);
							runnable = true;
						}
					}
				} catch (InterruptedException e) {
					LOGGER.info(e.getMessage());
				} catch (KiteException e) {
					LOGGER.info(e.getMessage());
				}
			}
		});
		t.start();
	}

	private void executeOrdersOnSignals() {
		LOGGER.info("executeOrdersOnSignals entry");
		Thread t = new Thread(new Runnable() {
			private boolean runnable = true;

			@Override
			public void run() {
				try {
					while (runnable) {
						Date timeNow = Calendar.getInstance(timeZone).getTime();
						if (timeNow.compareTo(timeStart) >= 0 && timeNow.compareTo(timeEnd) <= 0) {
							LOGGER.info("in executeOrdersOnSignals" + e++);
							List<Order> signalList = getOrderListToPlaceAsPerStrategySignals();
							for (int index = 0; index < signalList.size(); index++)
								tradeOperations.placeOrder(kiteconnect, signalList.get(index).symbol,
										signalList.get(index).transactionType, signalList.get(index).quantity,
										streamingQuoteStorage);
							Thread.sleep(2 * seconds);
						} else {
							runnable = false;
						}
					}
				} catch (InterruptedException e) {
					LOGGER.info(e.getMessage());
				} catch (KiteException e) {
					LOGGER.info(e.getMessage());
				}
			}
		});
		t.start();
	}

	private List<Order> getOrderListToPlaceAsPerStrategySignals() {
		LOGGER.info("getOrderListToPlaceAsPerStrategySignals entry");
		return streamingQuoteStorage.getOrderListToPlace();
	}

	private void startLiveStreamOfSelectedInstruments() {
		LOGGER.info("startLiveStreamOfSelectedInstruments entry");
		Thread t = new Thread(new Runnable() {
			private boolean runnable = true;

			@Override
			public void run() {
				try {
					while (runnable) {
						Date timeNow = Calendar.getInstance(timeZone).getTime();
						if (timeNow.compareTo(timeStart) >= 0 && timeNow.compareTo(timeEnd) <= 0) {
							LOGGER.info("in startLiveStreamOfSelectedInstruments" + d++);
							startStreamingQuote();
							Thread.sleep(10 * seconds);
						} else {
							runnable = false;
							stopStreamingQuote();
						}
					}
				} catch (InterruptedException e) {
					LOGGER.info(e.getMessage());
				}
			}
		});
		t.start();
	}

	private void threadEnabledHistoricalDataFetch(List<Instrument> instrumentList) {
		StreamingQuoteModeQuote quote = null;
		HistoricalData historicalDataList = null, histData = null;
		ArrayList<StreamingQuoteModeQuote> quoteList = null;
		Instrument instrument = null;
		double priorityPoint = 0;

		for (int index = 0; index < instrumentList.size(); index++) {
			try {
				instrument = instrumentList.get(index);
				quoteList = new ArrayList<StreamingQuoteModeQuote>();
				priorityPoint = 0;
				historicalDataList = tradeOperations.getHistoricalData(kiteconnect, histDataStreamStartDateTime,
						histDataStreamEndDateTime, "minute", Long.toString(instrument.getInstrument_token()));

				for (int count = 0; count < historicalDataList.dataArrayList.size(); count++) {
					histData = historicalDataList.dataArrayList.get(count);
					priorityPoint += (Double.valueOf(histData.volume) / 1000);
					quote = new StreamingQuoteModeQuote(historicalDataList.dataArrayList.get(count).timeStamp,
							Long.toString(instrument.getInstrument_token()), new Double(0), new Long(0), new Double(0),
							Long.valueOf(histData.volume), new Long(0), new Long(0), new Double(histData.open),
							new Double(histData.high), new Double(histData.low), new Double(histData.close));
					quoteList.add(quote);
				}
				quoteList.get(0).ltp = priorityPoint;
				streamingQuoteStorage.storeData(quoteList, "minute");
			} catch (ArithmeticException e) {
				if (e.getMessage().trim().equalsIgnoreCase("/ by zero"))
					continue;
				else {
					LOGGER.info(e.getMessage());
					continue;
				}
			} catch (KiteException e) {
				if (e.message.equalsIgnoreCase("No candles found based on token and time and candleType or Server busy")
						&& e.code == 400)
					continue;
				else {
					LOGGER.info(e.message);
					continue;
				}
			}
		}
	}

	private void startStreamForHistoricalInstrumentsData() {
		LOGGER.info("startStreamForHistoricalInstrumentsData entry");

		Thread t = new Thread(new Runnable() {
			private boolean runnable = true;

			@Override
			public void run() {
				while (runnable) {
					try {
						Date timeNow = Calendar.getInstance(timeZone).getTime();
						if (timeNow.compareTo(histTimeStart) >= 0 && timeNow.compareTo(histTimeEnd) <= 0) {
							LOGGER.info("in startStreamForHistoricalInstrumentsData" + a++);
							if (firstTimeDayHistoryRun) {
								try {
									instrumentList = tradeOperations.getInstrumentsForExchange(kiteconnect, "NSE");
									streamingQuoteStorage.saveInstrumentDetails(instrumentList,
											new Timestamp(new Date().getTime()));

									/*
									 * instrumentList.addAll(tradeOperations
									 * .getInstrumentsForExchange(kiteconnect,
									 * "BSE"));
									 * instrumentList.addAll(tradeOperations
									 * .getInstrumentsForExchange(kiteconnect,
									 * "NFO"));
									 * instrumentList.addAll(tradeOperations
									 * .getInstrumentsForExchange(kiteconnect,
									 * "BFO"));
									 */
									firstTimeDayHistoryRun = false;
								} catch (KiteException e) {

									LOGGER.info(e.message);
								}
							}

							/*
							 * int size = instrumentList.size() / 2;
							 * 
							 * for (int k = 0; k < 2; k++) { LOGGER.info(new
							 * Date().toString() + ">>>>>>>>>>>>>>>>>>>>>");
							 * final List<Instrument> instrument =
							 * instrumentList.subList(size*k, size*(k+1)-1);
							 * LOGGER.info(new Date().toString() +
							 * ">>>outer loop>>> " + k + " " +
							 * instrument.size());
							 * 
							 * LOGGER.info(new Date().toString() +
							 * ">>>InThread>>>" + instrument.size()); Thread t =
							 * new Thread(new Runnable() {
							 * 
							 * @Override public void run() {
							 * 
							 * }
							 * 
							 * }); t.start();
							 */
							List<Instrument> instrument = instrumentList;

							threadEnabledHistoricalDataFetch(instrument);
							ArrayList<Long> previousQuoteStreamingInstrumentsArr = quoteStreamingInstrumentsArr;
							quoteStreamingInstrumentsArr = streamingQuoteStorage
									.getTopPrioritizedTokenList(tokenCountForTrade);
							try {
								roundOfNonPerformingBoughtStocks(quoteStreamingInstrumentsArr,
										previousQuoteStreamingInstrumentsArr, kiteconnect);
							} catch (KiteException e) {
								LOGGER.info(e.getMessage());
							}
							histDataStreamStartDateTime = todaysDate + " " + tmFmt.format(new Date());
							Thread.sleep(1800 * seconds);
							histDataStreamEndDateTime = todaysDate + " " + tmFmt.format(new Date());
						} else {
							runnable = false;
						}
					} catch (InterruptedException e) {
						LOGGER.info(e.getMessage());
					} catch (IOException e) {
						LOGGER.info(e.getMessage());
					}
				}
			}
		});
		t.start();
	}

	private void roundOfNonPerformingBoughtStocks(ArrayList<Long> quoteStreamingInstrumentsArr,
			ArrayList<Long> previousQuoteStreamingInstrumentsArr, KiteConnect kiteconnect) throws KiteException {
		LOGGER.info("roundOfNonPerformingBoughtStocks Entry");

		ArrayList<Long> unSubList = new ArrayList<Long>();
		ArrayList<Long> subList = new ArrayList<Long>();
		for (int index = 0; index < previousQuoteStreamingInstrumentsArr.size(); index++) {
			boolean quoteExistInNewList = false;
			for (int count = 0; count < quoteStreamingInstrumentsArr.size(); count++) {
				if (quoteStreamingInstrumentsArr.get(count).longValue() == previousQuoteStreamingInstrumentsArr
						.get(index).longValue()) {
					quoteExistInNewList = true;
				}
			}
			if (!quoteExistInNewList) {
				unSubList.add(previousQuoteStreamingInstrumentsArr.get(index));
			}
		}
		for (int index = 0; index < quoteStreamingInstrumentsArr.size(); index++) {
			boolean newQuote = true;
			for (int count = 0; count < previousQuoteStreamingInstrumentsArr.size(); count++) {
				if (quoteStreamingInstrumentsArr.get(index).longValue() == previousQuoteStreamingInstrumentsArr
						.get(count).longValue()) {
					newQuote = false;
				}
			}
			if (newQuote) {
				unSubList.add(quoteStreamingInstrumentsArr.get(index));
			}
		}
		List<Order> orders = tradeOperations.getOrders(kiteconnect);

		for (int index = 0; index < orders.size(); index++) {
			boolean OrderCancelRequired = true;
			for (int count = 0; count < quoteStreamingInstrumentsArr.size(); count++) {
				if (orders.get(index).tradingSymbol
						.equalsIgnoreCase(quoteStreamingInstrumentsArr.get(count).toString())) {
					OrderCancelRequired = false;
				}
			}
			if (OrderCancelRequired) {
				if (orders.get(index).status.equalsIgnoreCase("OPEN"))
					tradeOperations.cancelOrder(kiteconnect, orders.get(index));
			}
		}

		List<Holding> holdings = tradeOperations.getHoldings(kiteconnect).holdings;

		for (int index = 0; index < holdings.size(); index++) {
			boolean orderSellOutRequired = true;
			for (int count = 0; count < quoteStreamingInstrumentsArr.size(); count++) {
				if (holdings.get(index).instrumentToken
						.equalsIgnoreCase(quoteStreamingInstrumentsArr.get(count).toString())) {
					orderSellOutRequired = false;
				}
			}
			if (orderSellOutRequired) {
				tradeOperations.placeOrder(kiteconnect, holdings.get(index).instrumentToken, "SELL",
						holdings.get(index).quantity, streamingQuoteStorage);
			}
		}
		if (unSubList.size() > 0)
			tickerProvider.unsubscribe(unSubList);
		if (subList.size() > 0)
			try {
				tickerProvider.subscribe(subList);
			} catch (IOException | WebSocketException e) {
				LOGGER.info(e.getMessage());
			}
	}

	private void applyStrategiesAndGenerateSignals() {
		LOGGER.info("checkForSignalsFromStrategies Entry");
		Thread t = new Thread(new Runnable() {
			private boolean runnable = true;

			@Override
			public void run() {
				try {
					while (runnable) {
						Date timeNow = Calendar.getInstance(timeZone).getTime();
						if (timeNow.compareTo(timeStart) >= 0 && timeNow.compareTo(timeEnd) <= 0) {
							LOGGER.info("in checkForSignalsFromStrategies" + b++);
							applySmaStragegy();
							Thread.sleep(10 * seconds);
						} else {
							runnable = false;
						}
					}
				} catch (InterruptedException e) {
					LOGGER.info(e.getMessage());
				}
			}
		});
		t.start();
	}

	private void applySmaStragegy() {
		LOGGER.info("applySmaStragegy entry");

		ArrayList<Long> instrumentList = getInstrumentTokensList();
		if (null != instrumentList && instrumentList.size() > 0) {
			Map<Long, String> signalList = new HashMap<Long, String>();
			List<StreamingQuoteModeQuote> quoteData = null;
			for (int i = 0; i < instrumentList.size(); i++) {
				quoteData = streamingQuoteStorage.getProcessableQuoteDataOnTokenId(instrumentList.get(i).toString(),
						60);
				if (null == quoteData || quoteData.size() < 60)
					continue;
				//signalList.putAll(new SmaStragegyProcessor(quoteData, instrumentList.get(i)).getSignals());
			}
			streamingQuoteStorage.saveGeneratedSignals(signalList, instrumentList);
		}
		// try inplementing at data change trigger level, then only sma ema
		// value fetch and compare would be ok
	}

	private void startStreamingQuote() {
		LOGGER.info("startStreamingQuote Entry");
		tokenListForTick = getInstrumentTokensList();
		if (null != tokenListForTick && tokenListForTick.size() > 0) {
			try {
				if (liveStreamFirstRun) {
					tickerSettingInitialization();
					liveStreamFirstRun = false;
				}
				System.out.println("TradeOptimizer First Connect");
				tickerProvider.connect();

			} catch (IOException | WebSocketException e) {
				LOGGER.info(e.getMessage());
			}
		}
	}

	private ArrayList<Long> getInstrumentTokensList() {
		LOGGER.info("getInstrumentTokensList Entry");
		if (null != quoteStreamingInstrumentsArr)
			return quoteStreamingInstrumentsArr;
		else
			return new ArrayList<Long>();
	}

	private void stopStreamingQuote() {
		LOGGER.info("stopStreamingQuote Entry");
		tickerProvider.disconnect();

		if (streamingQuoteStorage != null) {
			streamingQuoteStorage.closeJDBCConn();
		}
	}
}
