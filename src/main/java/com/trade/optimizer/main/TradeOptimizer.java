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
import org.apache.http.client.methods.HttpGet;
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
import com.streamquote.utils.StreamingConfig;
import com.trade.optimizer.exceptions.KiteException;
import com.trade.optimizer.kiteconnect.KiteConnect;
import com.trade.optimizer.kiteconnect.SessionExpiryHook;
import com.trade.optimizer.models.Holding;
import com.trade.optimizer.models.Instrument;
import com.trade.optimizer.models.InstrumentVolatilityScore;
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

	private boolean liveStreamFirstRun = false;
	private boolean tickerStarted = false;

	private int tokenCountForTrade = 30;
	private int seconds = 1000;

	private StreamingQuoteStorage streamingQuoteStorage = new StreamingQuoteStorageImpl();
	private KiteConnect kiteconnect = new KiteConnect(StreamingConfig.API_KEY);
	private TradeOperations tradeOperations = new TradeOperations();
	private List<Instrument> instrumentList = new ArrayList<Instrument>();
	private ArrayList<Long> quoteStreamingInstrumentsArr = new ArrayList<Long>();
	private ArrayList<Long> tokenListForTick = null;
	private KiteTicker tickerProvider;
	private List<InstrumentVolatilityScore> instrumentVolatilityScoreList = new ArrayList<InstrumentVolatilityScore>();
	private String url = kiteconnect.getLoginUrl();
	private String todaysDate, quoteStartTime, quoteEndTime;

	private SimpleDateFormat dtTmZFmt = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ssZ");
	private DateFormat dtFmt = new SimpleDateFormat("yyyy-MM-dd");
	private DateFormat tmFmt = new SimpleDateFormat("HH:mm:ss");
	private DateFormat dtTmFmt = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
	private Date timeStart, timeEnd;
	private TimeZone timeZone = TimeZone.getTimeZone("IST");

	@Autowired
	ServletContext context;
	@Autowired
	HttpServletRequest request;
	@Autowired
	HttpServletResponse response;

	@RequestMapping(value = "/AuthRedirectWithToken", method = RequestMethod.GET)
	public String authRedirectWithTokenPost(@RequestParam String status, @RequestParam String request_token) {
		// LOGGER.info("Entry TradeOptimizer.authRedirectWithTokenPost()");
		try {
			UserModel userModel = kiteconnect.requestAccessToken(request_token, StreamingConfig.API_SECRET_KEY);
			kiteconnect.setAccessToken(userModel.accessToken);
			kiteconnect.setPublicToken(userModel.publicToken);
			startProcess();
		} catch (JSONException | KiteException e) {
			LOGGER.info("Error TradeOptimizer.authRedirectWithTokenPost(): " + e.getMessage());
			return "error";
		}
		// LOGGER.info("Exit TradeOptimizer.authRedirectWithTokenPost()");
		return "index";
	}

	@RequestMapping(value = "/start", method = { RequestMethod.POST, RequestMethod.GET })
	public RedirectView localRedirect() {
		// LOGGER.info("Entry TradeOptimizer.localRedirect()");
		kiteconnect.registerHook(new SessionExpiryHook() {
			@Override
			public void sessionExpired() {
				LOGGER.info("Error TradeOptimizer.localRedirect(): Session Expired");
				localRedirect();
			}
		});
		RedirectView redirectView = new RedirectView();
		redirectView.setUrl(url);
		// LOGGER.info("Exit TradeOptimizer.localRedirect()");
		return redirectView;
	}

	public void downloadNSEReportsCSVs() {

		// LOGGER.info("Entry TradeOptimizer.downloadNSEReportsCSVs()");
		@SuppressWarnings({ "resource" })
		HttpClient client = new DefaultHttpClient();

		HttpGet get = new HttpGet(StreamingConfig.nseVolatilityDataUrl);
		get.addHeader(StreamingConfig.ACCESS_CONTROL_ALLOW_ORIGIN, "*");
		get.addHeader("user-agent", StreamingConfig.USER_AGENT_VALUE);

		try {
			HttpResponse response = client.execute(get);
			BufferedReader rd = new BufferedReader(new InputStreamReader(response.getEntity().getContent()));
			String line = "";

			String[] readData;
			boolean firstLine = true;
			InstrumentVolatilityScore instrumentVolatilityScore;
			while ((line = rd.readLine()) != null) {
				try {
					readData = line.split(",");
					if (!firstLine) {
						instrumentVolatilityScore = new InstrumentVolatilityScore();
						instrumentVolatilityScore.setInstrumentName(readData[1]);
						instrumentVolatilityScore.setDailyVolatility(Double.parseDouble(readData[6]) * 100.0);
						instrumentVolatilityScore.setAnnualVolatility(Double.parseDouble(readData[7]) * 100.0);

						instrumentVolatilityScoreList.add(instrumentVolatilityScore);
					}
					firstLine = false;
				} catch (Exception e) {
					LOGGER.info("Error TradeOptimizer :- " + e.getMessage());
				}
			}
		} catch (IOException e) {
			LOGGER.info("Error TradeOptimizer :- " + e.getMessage());
		}

		streamingQuoteStorage.saveInstrumentVolatilityDetails(instrumentVolatilityScoreList);
		// LOGGER.info("Exit TradeOptimizer.downloadNSEReportsCSVs()");
	}

	public void markTradableInstruments() {

		// LOGGER.info("Entry TradeOptimizer.markTradableInstruments()");
		@SuppressWarnings({ "resource" })
		HttpClient client = new DefaultHttpClient();

		HttpGet get = new HttpGet(StreamingConfig.nifty100InstrumentCsvUrl);
		get.addHeader(StreamingConfig.ACCESS_CONTROL_ALLOW_ORIGIN, "*");
		get.addHeader("user-agent", StreamingConfig.USER_AGENT_VALUE);
		instrumentVolatilityScoreList = new ArrayList<InstrumentVolatilityScore>();
		try {
			HttpResponse response = client.execute(get);
			BufferedReader rd = new BufferedReader(new InputStreamReader(response.getEntity().getContent()));
			String line = "";

			String[] readData;
			boolean firstLine = true;
			InstrumentVolatilityScore instrumentVolatilityScore;
			while ((line = rd.readLine()) != null) {
				try {
					readData = line.split(",");
					if (!firstLine) {
						instrumentVolatilityScore = new InstrumentVolatilityScore();
						instrumentVolatilityScore.setInstrumentName(readData[2]);
						instrumentVolatilityScore.setTradable("tradable");

						instrumentVolatilityScoreList.add(instrumentVolatilityScore);
					}
					firstLine = false;
				} catch (Exception e) {
					LOGGER.info("Error TradeOptimizer :- " + e.getMessage());
				}
			}
		} catch (IOException e) {
			LOGGER.info("Error TradeOptimizer :- " + e.getMessage());
		}
		streamingQuoteStorage.markTradableInstruments(instrumentVolatilityScoreList);
		// LOGGER.info("Exit TradeOptimizer.markTradableInstruments()");
	}

	public void fetchNSEActiveSymbolList() {
		// LOGGER.info("Entry TradeOptimizer.fetchNSEActiveSymbolList()");
		@SuppressWarnings({ "resource" })
		HttpClient client = new DefaultHttpClient();
		Map<String, Double> stocksSymbolArray = new HashMap<String, Double>();
		Double j = 1000.0;
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

				for (int i = 0; i < arr.length(); i++) {
					if (!stocksSymbolArray.containsKey(arr.getJSONObject(i).getString("symbol")))
						stocksSymbolArray.put(arr.getJSONObject(i).getString("symbol"), j);
					j = j - 1.0;
				}
			} catch (IOException e) {
				LOGGER.info("Error TradeOptimizer :- " + e.getMessage());
			}
		}
		streamingQuoteStorage.getInstrumentTokenIdsFromSymbols(stocksSymbolArray);
		// LOGGER.info("Exit TradeOptimizer.fetchNSEActiveSymbolList()");
	}

	public void startProcess() {
		try {
			// LOGGER.info("Entry TradeOptimizer.startProcess()");
			tmFmt.setTimeZone(timeZone);
			dtFmt.setTimeZone(timeZone);
			dtTmFmt.setTimeZone(timeZone);
			dtTmZFmt.setTimeZone(timeZone);

			todaysDate = dtFmt.format(Calendar.getInstance(timeZone).getTime());
			quoteStartTime = todaysDate + " " + StreamingConfig.QUOTE_STREAMING_START_TIME;
			quoteEndTime = todaysDate + " " + StreamingConfig.QUOTE_STREAMING_END_TIME;
			timeStart = dtTmFmt.parse(quoteStartTime);
			timeEnd = dtTmFmt.parse(quoteEndTime);

			createInitialDayTables();

			fetchAndProcessInstrumentsPriorityRelatedData();

			startLiveStreamOfSelectedInstruments();

			applyStrategiesAndGenerateSignals();

			// placeOrdersBasedOnSignals();

			// checkAndProcessPendingOrdersOnMarketQueue();

			// dayClosingStocksRoundOffOperations();
		} catch (JSONException | ParseException e) {
			LOGGER.info("Error TradeOptimizer.startProcess(): " + e.getMessage());
		}
		// LOGGER.info("Exit TradeOptimizer.startProcess()");
	}

	private void tickerSettingInitialization() {
		// LOGGER.info("Entry TradeOptimizer.tickerSettingInitialization()");

		tickerProvider = new KiteTicker(kiteconnect);
		tickerProvider.setTryReconnection(true);
		try {
			tickerProvider.setTimeIntervalForReconnection(5);
		} catch (KiteException e) {
			LOGGER.info("Error TradeOptimizer :- " + e.getMessage());
		}
		tickerProvider.setMaxRetries(-1);

		if (null != tokenListForTick && !tokenListForTick.isEmpty())
			tickerProvider.setMode(tokenListForTick, KiteTicker.modeQuote);

		tickerProvider.setOnConnectedListener(new OnConnect() {
			@Override
			public void onConnected() {
				if (null != tokenListForTick
						&& tickerProvider.getSubscribedTokenList().size() != tokenListForTick.size())
					try {
						tickerProvider.subscribe(tokenListForTick);
					} catch (IOException | WebSocketException | KiteException e) {
						LOGGER.info("Error TradeOptimizer :- " + e.getMessage());
					}
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
		// LOGGER.info("Exit TradeOptimizer.tickerSettingInitialization()");
	}

	protected ArrayList<Tick> testingTickerData(ArrayList<Long> quoteStreamingInstrumentsArr) {
		// LOGGER.info("Entry TradeOptimizer.testingTickerData()");

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

			tick.setLastTradedPrice(tick.getClosePrice());
			ticks.add(tick);
		}
		// LOGGER.info("Exit TradeOptimizer.testingTickerData()");
		return ticks;
	}

	private void createInitialDayTables() {
		// LOGGER.info("Entry TradeOptimizer.createInitialDayTables()");
		if (streamingQuoteStorage != null) {

			DateFormat quoteTableDtFmt = new SimpleDateFormat("ddMMyyyy");
			quoteTableDtFmt.setTimeZone(timeZone);

			streamingQuoteStorage.initializeJDBCConn();
			try {
				streamingQuoteStorage.createDaysStreamingQuoteTable(
						quoteTableDtFmt.format(Calendar.getInstance(timeZone).getTime()));
			} catch (SQLException e) {
				LOGGER.info("Error TradeOptimizer :- " + e.getMessage());
			}
		}
		// LOGGER.info("Exit TradeOptimizer.createInitialDayTables()");
	}

	private void dayClosingStocksRoundOffOperations() throws KiteException {
		// LOGGER.info("Entry
		// TradeOptimizer.dayClosingStocksRoundOffOperations()");

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
						LOGGER.info("Error TradeOptimizer :- " + e.getMessage());
					} catch (KiteException e) {
						LOGGER.info("Error TradeOptimizer :- " + e.getMessage());
					} catch (Exception e) {
						LOGGER.info("Error TradeOptimizer :- " + e.getMessage());
					}
				}
			}
		});
		t.start();
		// LOGGER.info("Exit
		// TradeOptimizer.dayClosingStocksRoundOffOperations()");
	}

	private void placeOrdersBasedOnSignals() {
		// LOGGER.info("Entry TradeOptimizer.placeOrdersBasedOnSignals()");

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
						LOGGER.info("Error TradeOptimizer :- " + e.getMessage());
					} catch (KiteException e) {
						LOGGER.info("Error TradeOptimizer :- " + e.getMessage());
					} catch (Exception e) {
						LOGGER.info("Error TradeOptimizer :- " + e.getMessage());
					}
				}
			}
		});
		t.start();
		// LOGGER.info("Exit TradeOptimizer.placeOrdersBasedOnSignals()");
	}

	private List<Order> getOrderListToPlaceAsPerStrategySignals() {
		return streamingQuoteStorage.getOrderListToPlace();
	}

	private void startLiveStreamOfSelectedInstruments() {
		// LOGGER.info("Entry
		// TradeOptimizer.startLiveStreamOfSelectedInstruments()");

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
						LOGGER.info("Error TradeOptimizer :- " + e.getMessage());
					} catch (Exception e) {
						LOGGER.info("Error TradeOptimizer :- " + e.getMessage());
					}
				}
			}
		});
		t.start();
		// LOGGER.info("Exit
		// TradeOptimizer.startLiveStreamOfSelectedInstruments()");
	}

	private void fetchAndProcessInstrumentsPriorityRelatedData() {
		// LOGGER.info("Entry
		// TradeOptimizer.fetchAndProcessInstrumentPriorityRelatedData()");

		try {
			instrumentList = tradeOperations.getInstrumentsForExchange(kiteconnect, "NSE");
			streamingQuoteStorage.saveInstrumentDetails(instrumentList, new Timestamp(new Date().getTime()));

			/*
			 * instrumentList.addAll(tradeOperations
			 * .getInstrumentsForExchange(kiteconnect, "BSE"));
			 * instrumentList.addAll(tradeOperations
			 * .getInstrumentsForExchange(kiteconnect, "NFO"));
			 * instrumentList.addAll(tradeOperations
			 * .getInstrumentsForExchange(kiteconnect, "BFO"));
			 */
			fetchNSEActiveSymbolList();
			downloadNSEReportsCSVs();
			markTradableInstruments();

			quoteStreamingInstrumentsArr = streamingQuoteStorage.getTopPrioritizedTokenList(tokenCountForTrade);
			liveStreamFirstRun = true;

		} catch (KiteException e) {
			LOGGER.info("Error TradeOptimizer :- " + e.getMessage());
		} catch (Exception e) {
			LOGGER.info("Error TradeOptimizer :- " + e.getMessage());
		}

		// LOGGER.info("Exit
		// TradeOptimizer.fetchAndProcessInstrumentPriorityRelatedData()");
	}

	private void roundOfNonPerformingBoughtStocks(ArrayList<Long> quoteStreamingInstrumentsArr,
			ArrayList<Long> previousQuoteStreamingInstrumentsArr) throws KiteException {
		// LOGGER.info("Entry
		// TradeOptimizer.roundOfNonPerformingBoughtStocks()");

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
				subList.add(quoteStreamingInstrumentsArr.get(index));
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
			try {
				if (!tickerStarted)
					tickerSettingInitialization();
				tickerProvider.connect();
				if (null != unSubList && unSubList.size() > 0)
					tickerProvider.unsubscribe(unSubList);
				if (null != subList && subList.size() > 0) {
					tickerProvider.subscribe(subList);
					tickerStarted = true;
				}
			} catch (WebSocketException | IOException e) {
				LOGGER.info("Error TradeOptimizer :- " + e.getMessage());
			}
		}

		// LOGGER.info("Exit
		// TradeOptimizer.roundOfNonPerformingBoughtStocks()");
	}

	private void applyStrategiesAndGenerateSignals() {
		// LOGGER.info("Entry
		// TradeOptimizer.applyStrategiesAndGenerateSignals()");

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
						LOGGER.info("Error TradeOptimizer :- " + e.getMessage());
					} catch (Exception e) {
						LOGGER.info("Error TradeOptimizer :- " + e.getMessage());
					}
				}
			}
		});
		t.start();
		// LOGGER.info("Exit
		// TradeOptimizer.applyStrategiesAndGenerateSignals()");
	}

	private void startStreamingQuote() {
		// LOGGER.info("Entry TradeOptimizer.startStreamingQuote()");
		tokenListForTick = getInstrumentTokensList();
		if (null != tokenListForTick && tokenListForTick.size() > 0) {
			try {
				if (liveStreamFirstRun) {
					if (!tickerStarted)
						tickerSettingInitialization();
					tickerProvider.connect();
					if (!tickerStarted)
						tickerProvider.subscribe(tokenListForTick);
					tickerStarted = true;
				}
			} catch (IOException | WebSocketException | KiteException e) {
				LOGGER.info("Error TradeOptimizer :- " + e.getMessage());
			} catch (Exception e) {
				LOGGER.info("Error TradeOptimizer :- " + e.getMessage());
			}
		}
		// LOGGER.info("Exit TradeOptimizer.startStreamingQuote()");
	}

	private ArrayList<Long> getInstrumentTokensList() {
		if (null != quoteStreamingInstrumentsArr)
			return quoteStreamingInstrumentsArr;
		else
			return new ArrayList<Long>();
	}

	private void stopStreamingQuote() {
		// LOGGER.info("Entry TradeOptimizer.stopStreamingQuote()");
		tickerProvider.disconnect();

		if (streamingQuoteStorage != null) {
			streamingQuoteStorage.closeJDBCConn();
		}
		// LOGGER.info("Exit TradeOptimizer.stopStreamingQuote()");
	}
}
