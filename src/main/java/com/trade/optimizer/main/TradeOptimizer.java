package com.trade.optimizer.main;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
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

import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.impl.client.DefaultHttpClient;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
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

@SuppressWarnings("deprecation")
@Controller
public class TradeOptimizer {

	private final static Logger LOGGER = Logger.getLogger(TradeOptimizer.class.getName());

	private boolean liveStreamFirstRun = true;
	private boolean firstTimeDayHistoryRun = true;
	private boolean tickerStarted = false;

	private int tokenCountForTrade = 10;
	private int seconds = 1000;

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
			e.printStackTrace();
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

	public void fetchNSEActiveSymbolList() {
		@SuppressWarnings({ "resource" })
		HttpClient client = new DefaultHttpClient();
		Map<String, Double> stocksSymbolArray = new HashMap<String, Double>();

		for (int count = 0; count < StreamingConfig.stockListCollectingUrls.length; count++) {

			HttpPost post = new HttpPost(StreamingConfig.stockListCollectingUrls[count]);
			post.addHeader(StreamingConfig.ACCESS_CONTROL_ALLOW_ORIGIN, "*");
			post.addHeader("user-agent", StreamingConfig.USER_AGENT_VALUE);

			try {
				HttpResponse response = client.execute(post);
				BufferedReader rd = new BufferedReader(new InputStreamReader(response.getEntity().getContent()));
				String line = "", lineAll = "";

				while ((line = rd.readLine()) != null)
					lineAll = lineAll + line;

				JSONObject obj = new JSONObject(lineAll);
				JSONArray arr = obj.getJSONArray("data");

				for (int i = 0; i < arr.length(); i++)
					if (!stocksSymbolArray.containsKey(arr.getJSONObject(i).getString("symbol")))
						stocksSymbolArray.put(arr.getJSONObject(i).getString("symbol"),
								(Math.abs(Double.valueOf(arr.getJSONObject(i).getString("netPrice"))) * (Double
										.valueOf(arr.getJSONObject(i).getString("tradedQuantity").replaceAll(",", ""))
										/ 100000)));
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		streamingQuoteStorage.getInstrumentTokenIdsFromSymbols(stocksSymbolArray);
	}

	public void startProcess() {
		try {
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

			startLiveStreamOfSelectedInstruments();

			applyStrategiesAndGenerateSignals();

			executeOrdersOnSignals();

			// checkAndProcessPendingOrdersOnMarketQueue();

			closingDayRoundOffOperations();
		} catch (JSONException | ParseException | KiteException e) {
			e.printStackTrace();
		}
	}

	private void tickerSettingInitialization() {
		tickerProvider = new KiteTicker(kiteconnect);
		tickerProvider.setTryReconnection(true);
		try {
			tickerProvider.setTimeIntervalForReconnection(5);
		} catch (KiteException e) {
			e.printStackTrace();
		}
		tickerProvider.setMaxRetries(-1);

		if (null != tokenListForTick && !tokenListForTick.isEmpty())
			tickerProvider.setMode(tokenListForTick, KiteTicker.modeQuote);

		tickerProvider.setOnConnectedListener(new OnConnect() {
			@Override
			public void onConnected() {
			}
		});

		tickerProvider.setOnDisconnectedListener(new OnDisconnect() {
			@Override
			public void onDisconnected() {
				tickerProvider.unsubscribe(tokenListForTick);
			}
		});

		tickerProvider.setOnTickerArrivalListener(new OnTick() {
			@Override
			public void onTick(ArrayList<Tick> ticks) {
				//if (null != quoteStreamingInstrumentsArr && quoteStreamingInstrumentsArr.size() > 0)
				//	ticks = testingTickerData(quoteStreamingInstrumentsArr);
				if (ticks.size() > 0)
					streamingQuoteStorage.storeData(ticks);
			}
		});
	}

	protected ArrayList<Tick> testingTickerData(ArrayList<Long> quoteStreamingInstrumentsArr) {

		ArrayList<Tick> ticks = new ArrayList<Tick>();
		for (int i = 0; i < quoteStreamingInstrumentsArr.size(); i++) {
			Tick tick = new Tick();
			tick.setToken(Integer.parseInt(quoteStreamingInstrumentsArr.get(i).toString()));
			tick.setClosePrice(2000.0 + Math.random() * (5.0));
			tick.setLowPrice(tick.getClosePrice() - (Math.random() * (3.0 * Math.random())));
			tick.setHighPrice(tick.getClosePrice() + (Math.random() * (3.0 * Math.random())));
			if (Math.random() > 0.5)
				tick.setClosePrice(tick.getClosePrice() - Math.random());
			else
				tick.setClosePrice(tick.getClosePrice() + Math.random());
			if (Math.random() > 0.5)
				tick.setLowPrice(tick.getLowPrice() - Math.random());
			else
				tick.setLowPrice(tick.getLowPrice() + Math.random());
			if (Math.random() > 0.5)
				tick.setHighPrice(tick.getHighPrice() - Math.random());
			else
				tick.setHighPrice(tick.getHighPrice() + Math.random());
			ticks.add(tick);
		}
		return ticks;
	}

	private void createInitialDayTables() {
		if (streamingQuoteStorage != null) {

			DateFormat quoteTableDtFmt = new SimpleDateFormat("ddMMyyyy");
			quoteTableDtFmt.setTimeZone(timeZone);

			streamingQuoteStorage.initializeJDBCConn();
			try {
				streamingQuoteStorage.createDaysStreamingQuoteTable(
						quoteTableDtFmt.format(Calendar.getInstance(timeZone).getTime()));
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
	}

	private void closingDayRoundOffOperations() throws KiteException {
		Thread t = new Thread(new Runnable() {
			private boolean runnable = true;

			@Override
			public void run() {
				while (runnable) {
					try {
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
							Thread.sleep(10 * seconds);
							runnable = true;
						}

					} catch (InterruptedException e) {
						e.printStackTrace();
					} catch (KiteException e) {
						e.printStackTrace();
					} catch (Exception e) {
						e.printStackTrace();
					}
				}
			}
		});
		t.start();
	}

	private void executeOrdersOnSignals() {
		Thread t = new Thread(new Runnable() {
			private boolean runnable = true;

			@Override
			public void run() {
				while (runnable) {
					try {
						Date timeNow = Calendar.getInstance(timeZone).getTime();
						if (timeNow.compareTo(timeStart) >= 0 && timeNow.compareTo(timeEnd) <= 0) {
							List<Order> signalList = getOrderListToPlaceAsPerStrategySignals();
							for (int index = 0; index < signalList.size(); index++)
								tradeOperations.placeOrder(kiteconnect, signalList.get(index).symbol,
										signalList.get(index).transactionType, signalList.get(index).quantity,
										streamingQuoteStorage);
							Thread.sleep(10 * seconds);
						} else {
							runnable = false;
						}

					} catch (InterruptedException e) {
						e.printStackTrace();
					} catch (KiteException e) {
						e.printStackTrace();
					} catch (Exception e) {
						e.printStackTrace();
					}
				}
			}
		});
		t.start();
	}

	private List<Order> getOrderListToPlaceAsPerStrategySignals() {
		return streamingQuoteStorage.getOrderListToPlace();
	}

	private void startLiveStreamOfSelectedInstruments() {
		Thread t = new Thread(new Runnable() {
			private boolean runnable = true;

			@Override
			public void run() {
				while (runnable) {
					try {
						Date timeNow = Calendar.getInstance(timeZone).getTime();
						if (timeNow.compareTo(timeStart) >= 0 && timeNow.compareTo(timeEnd) <= 0) {
							startStreamingQuote();
							Thread.sleep(10 * seconds);
						} else {
							runnable = false;
							stopStreamingQuote();
						}

					} catch (InterruptedException e) {
						e.printStackTrace();
					} catch (Exception e) {
						e.printStackTrace();
					}
				}
			}
		});
		t.start();
	}

	@SuppressWarnings("unused")
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
					e.printStackTrace();
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

		Thread t = new Thread(new Runnable() {
			private boolean runnable = true;

			@Override
			public void run() {
				while (runnable) {
					try {
						Date timeNow = Calendar.getInstance(timeZone).getTime();
						if (timeNow.compareTo(histTimeStart) >= 0 && timeNow.compareTo(histTimeEnd) <= 0) {
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
								} catch (Exception e) {
									e.printStackTrace();
								}
							}

							// List<Instrument> instrument = instrumentList;
							// threadEnabledHistoricalDataFetch(instrument);

							ArrayList<Long> previousQuoteStreamingInstrumentsArr = quoteStreamingInstrumentsArr;

							fetchNSEActiveSymbolList();

							quoteStreamingInstrumentsArr = streamingQuoteStorage
									.getTopPrioritizedTokenList(tokenCountForTrade);
							try {
								if (null != quoteStreamingInstrumentsArr
										&& null != previousQuoteStreamingInstrumentsArr)
									roundOfNonPerformingBoughtStocks(quoteStreamingInstrumentsArr,
											previousQuoteStreamingInstrumentsArr);
							} catch (KiteException e) {
								e.printStackTrace();
							} catch (Exception e) {
								e.printStackTrace();
							}

							histDataStreamStartDateTime = todaysDate + " " + tmFmt.format(new Date());
							Thread.sleep(900 * seconds);
							histDataStreamEndDateTime = todaysDate + " " + tmFmt.format(new Date());
						} else {
							runnable = false;
						}
					} catch (InterruptedException e) {
						e.printStackTrace();
					} catch (Exception e) {
						e.printStackTrace();
					}
				}
			}
		});
		t.start();
	}

	private void roundOfNonPerformingBoughtStocks(ArrayList<Long> quoteStreamingInstrumentsArr,
			ArrayList<Long> previousQuoteStreamingInstrumentsArr) throws KiteException {

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
		if (null != orders && orders.size() > 0)
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

		Holding holdingList = tradeOperations.getHoldings(kiteconnect);
		List<Holding> holdings = new ArrayList<Holding>();
		if (null != holdingList && null != holdingList.holdings)
			holdings = holdingList.holdings;
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
		if (liveStreamFirstRun) {
			tickerSettingInitialization();
			try {
				tickerProvider.connect();
			} catch (WebSocketException | IOException e) {
				e.printStackTrace();
			}
			tickerStarted = true;
		}
		if (null != unSubList && unSubList.size() > 0)
			tickerProvider.unsubscribe(unSubList);
		if (null != subList && subList.size() > 0)
			try {
				tickerProvider.subscribe(subList);
			} catch (IOException | WebSocketException e) {
				e.printStackTrace();
			}
	}

	private void applyStrategiesAndGenerateSignals() {
		Thread t = new Thread(new Runnable() {
			private boolean runnable = true;

			@Override
			public void run() {

				while (runnable) {
					try {
						Date timeNow = Calendar.getInstance(timeZone).getTime();
						if (timeNow.compareTo(timeStart) >= 0 && timeNow.compareTo(timeEnd) <= 0) {
							ArrayList<Long> instrumentList = getInstrumentTokensList();
							Map<Long, String> signalList = new HashMap<Long, String>();

							if (null != instrumentList && instrumentList.size() > 0) {
								for (int i = 0; i < instrumentList.size(); i++)
									streamingQuoteStorage.calculateAndStoreStrategySignalParameters(
											instrumentList.get(i).toString(), timeNow);
								signalList = streamingQuoteStorage.calculateSignalsFromStrategyParams(instrumentList);
								streamingQuoteStorage.saveGeneratedSignals(signalList, instrumentList);
							}
							Thread.sleep(60 * seconds);
						} else {
							runnable = false;
						}

					} catch (InterruptedException e) {
						e.printStackTrace();
					} catch (Exception e) {
						e.printStackTrace();
					}
				}
			}
		});
		t.start();
	}

	private void startStreamingQuote() {
		tokenListForTick = getInstrumentTokensList();
		if (null != tokenListForTick && tokenListForTick.size() > 0) {
			try {
				if (liveStreamFirstRun) {
					if (!tickerStarted) {
						tickerSettingInitialization();
						tickerProvider.connect();
					}
					tickerProvider.subscribe(tokenListForTick);
					liveStreamFirstRun = false;
				}
			} catch (IOException | WebSocketException | KiteException e) {
				e.printStackTrace();
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	}

	private ArrayList<Long> getInstrumentTokensList() {
		if (null != quoteStreamingInstrumentsArr)
			return quoteStreamingInstrumentsArr;
		else
			return new ArrayList<Long>();
	}

	private void stopStreamingQuote() {
		tickerProvider.disconnect();

		if (streamingQuoteStorage != null) {
			streamingQuoteStorage.closeJDBCConn();
		}
	}
}
