package com.trade.optimizer.main;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URL;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TimeZone;
import java.util.logging.Logger;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;

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
import com.trade.optimizer.models.Instrument;
import com.trade.optimizer.models.InstrumentOHLCData;
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

	private boolean liveStreamFirstRun = true;
	private boolean tickerStarted = false;
	private boolean backendReadyForProcessing = false;
	private boolean dayOffActivityPerformed = false;
	private boolean streamingQuoteStopped = false;

	private int tokenCountForTrade = StreamingConfig.tokenCountForTrade;
	private int seconds = StreamingConfig.secondsValue;

	private StreamingQuoteStorage streamingQuoteStorage = new StreamingQuoteStorageImpl();
	private KiteConnect kiteconnect = new KiteConnect(StreamingConfig.API_KEY);
	private TradeOperations tradeOperations = new TradeOperations();
	private List<Instrument> instrumentList = new ArrayList<Instrument>();
	private List<Instrument> tempInstrumentList = new ArrayList<Instrument>();
	private ArrayList<Long> quoteStreamingInstrumentsArr = new ArrayList<Long>();
	private ArrayList<String> operatingTradingSymbolList = new ArrayList<String>();
	private ArrayList<Long> tokenListForTick = null;
	private KiteTicker tickerProvider;
	private List<InstrumentVolatilityScore> instrumentVolatilityScoreList = new ArrayList<InstrumentVolatilityScore>();
	private static Set<String> nseTradableInstrumentSymbol = new HashSet<String>();
	private String url = kiteconnect.getLoginUrl();
	private String todaysDate, quoteStartTime, quoteEndTime, quotePrioritySettingTime, dbConnectionClosingTime;

	private DateFormat dtFmt = new SimpleDateFormat("yyyy-MM-dd");
	private DateFormat dtTmFmt = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
	private Date timeStart, timeEnd, timeForPrioritySetting, timeDBConnectionClosing;

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
		} catch (KiteException e) {
			LOGGER.info("Error TradeOptimizer.authRedirectWithTokenPost(): " + e.message + " >> " + e.code);
			return "error";
		} catch (JSONException e) {
			LOGGER.info("Error TradeOptimizer.authRedirectWithTokenPost(): " + e.getMessage() + " >> " + e.getCause());
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

		// LOGGER.info("Entry TradeOptimizer.fetchNSEActiveSymbolList()");
		@SuppressWarnings({ "resource" })
		HttpClient client = new DefaultHttpClient();
		HashMap<String, ArrayList<InstrumentOHLCData>> instrumentVolatilityScoreList = new HashMap<String, ArrayList<InstrumentOHLCData>>();
		for (int count = 0; count < StreamingConfig.nseVolatilityDataUrl.length; count++) {
			HttpGet get = new HttpGet(StreamingConfig.nseVolatilityDataUrl[count]);
			get.addHeader(StreamingConfig.ACCESS_CONTROL_ALLOW_ORIGIN, "*");
			get.addHeader("user-agent", StreamingConfig.USER_AGENT_VALUE);

			try {
				HttpResponse response = client.execute(get);
				BufferedReader rd = new BufferedReader(new InputStreamReader(response.getEntity().getContent()));
				String line = "";

				String[] readData;
				boolean firstLine = true;
				InstrumentOHLCData instrumentVolatilityScore;
				while ((line = rd.readLine()) != null) {
					try {
						readData = line.split(",");
						if (!firstLine) {
							instrumentVolatilityScore = new InstrumentOHLCData();
							instrumentVolatilityScore.setInstrumentName(readData[1]);
							instrumentVolatilityScore.setClose(Double.parseDouble(readData[2]));
							instrumentVolatilityScore.setHigh(Double.parseDouble(readData[6]) * 100.0);
							instrumentVolatilityScore.setLow(Double.parseDouble(readData[7]) * 100.0);

							if (nseTradableInstrumentSymbol.contains(readData[1])) {
								if (instrumentVolatilityScoreList.containsKey(readData[1]))
									instrumentVolatilityScoreList.get(readData[1]).add(instrumentVolatilityScore);
								else {
									ArrayList<InstrumentOHLCData> tempList = new ArrayList<InstrumentOHLCData>();
									tempList.add(instrumentVolatilityScore);
									instrumentVolatilityScoreList.put(readData[1], tempList);
								}
							}
						} else
							firstLine = false;
					} catch (Exception e) {
					}
				}
			} catch (IOException e) {
			}
		}
		streamingQuoteStorage.saveInstrumentVolatilityDetails(instrumentVolatilityScoreList);
		// LOGGER.info("Exit TradeOptimizer.fetchNSEActiveSymbolList()");
	}

	public List<InstrumentVolatilityScore> markTradableInstruments() {

		// LOGGER.info("Entry TradeOptimizer.markTradableInstruments()");
		@SuppressWarnings({ "resource" })
		HttpClient client = new DefaultHttpClient();

		HttpGet get = new HttpGet(StreamingConfig.nifty200InstrumentCsvUrl);
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
						nseTradableInstrumentSymbol.add(readData[2]);
					}
					firstLine = false;
				} catch (Exception e) {
					LOGGER.info("Error TradeOptimizer :- " + e.getMessage() + " >> " + e.getCause());
				}
			}
		} catch (IOException e) {
			LOGGER.info("Error TradeOptimizer :- " + e.getMessage() + " >> " + e.getCause());
		}
		return instrumentVolatilityScoreList;
		// LOGGER.info("Exit TradeOptimizer.markTradableInstruments()");
	}

	public void fetchNSEActiveSymbolList() {
		// LOGGER.info("Entry TradeOptimizer.fetchNSEActiveSymbolList()");
		@SuppressWarnings({ "resource" })
		HttpClient client = new DefaultHttpClient();
		Map<String, InstrumentVolatilityScore> stocksSymbolArray = new HashMap<String, InstrumentVolatilityScore>();
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
				if (null != arr)
					for (int i = 0; i < arr.length(); i++) {
						try {
							if (!stocksSymbolArray.containsKey(arr.getJSONObject(i).getString("symbol"))) {
								if (nseTradableInstrumentSymbol.contains(arr.getJSONObject(i).getString("symbol"))) {
									InstrumentVolatilityScore instrumentVolatilityScore = new InstrumentVolatilityScore();
									instrumentVolatilityScore
											.setInstrumentName(arr.getJSONObject(i).getString("symbol"));
									instrumentVolatilityScore.setCurrentVolatility(j);
									instrumentVolatilityScore.setDailyVolatility(j);

									double a = Double
											.parseDouble(arr.getJSONObject(i).getString("ltp").replaceAll(",", ""));
									double b = Double.parseDouble(
											arr.getJSONObject(i).getString("previousPrice").replaceAll(",", ""));
									double c = Double.parseDouble(
											arr.getJSONObject(i).getString("netPrice").replaceAll(",", ""));

									if (a >= b && a >= c)
										instrumentVolatilityScore.setPrice(a);
									else if (b >= a && b >= c)
										instrumentVolatilityScore.setPrice(b);
									else if (c >= a && c >= b)
										instrumentVolatilityScore.setPrice(c);
									stocksSymbolArray.put(arr.getJSONObject(i).getString("symbol"),
											instrumentVolatilityScore);
								}
							}
							j = j - 1.0;
						} catch (Exception e) {
							LOGGER.info("Error TradeOptimizer :- " + e.getMessage() + " >> " + e.getCause());
						}
					}
			} catch (IOException e) {
				LOGGER.info("Error TradeOptimizer :- " + e.getMessage() + " >> " + e.getCause());
			}
		}
		streamingQuoteStorage.saveInstrumentTokenPriority(stocksSymbolArray);
		// LOGGER.info("Exit TradeOptimizer.fetchNSEActiveSymbolList()");
	}

	public void fetchNSESymbolVolumeData() {
		// LOGGER.info("Entry TradeOptimizer.fetchNSEActiveSymbolList()");
		@SuppressWarnings({ "resource" })
		HttpClient client = new DefaultHttpClient();
		HashMap<String, ArrayList<InstrumentOHLCData>> instrumentVolumeLast10DaysDataList = new HashMap<String, ArrayList<InstrumentOHLCData>>();
		for (int count = 0; count < StreamingConfig.last10DaysVolumeDataFileUrls.length; count++) {
			HttpGet get = new HttpGet(StreamingConfig.last10DaysVolumeDataFileUrls[2]);
			get.addHeader(StreamingConfig.ACCESS_CONTROL_ALLOW_ORIGIN, "*");
			get.addHeader("user-agent", StreamingConfig.USER_AGENT_VALUE);

			try {
				HttpResponse response = client.execute(get);
				BufferedReader rd = new BufferedReader(new InputStreamReader(response.getEntity().getContent()));
				String line = "";
				String[] readData;
				int lineSkip = 4;
				InstrumentOHLCData instrumentOHLCData;
				while ((line = rd.readLine()) != null) {
					try {
						readData = line.split(",");
						if (lineSkip == 0) {
							instrumentOHLCData = new InstrumentOHLCData();
							instrumentOHLCData.setInstrumentName(readData[2]);
							instrumentOHLCData.setClose(Double.parseDouble(readData[4]));
							instrumentOHLCData.setHigh(Double.parseDouble(readData[5]));
							instrumentOHLCData.setLow(Double.parseDouble(readData[6]));
							if (nseTradableInstrumentSymbol.contains(readData[2])) {
								if (instrumentVolumeLast10DaysDataList.containsKey(readData[2]))
									instrumentVolumeLast10DaysDataList.get(readData[2]).add(instrumentOHLCData);
								else {
									ArrayList<InstrumentOHLCData> tempList = new ArrayList<InstrumentOHLCData>();
									tempList.add(instrumentOHLCData);
									instrumentVolumeLast10DaysDataList.put(readData[2], tempList);
								}
							}
						} else
							lineSkip = lineSkip - 1;
					} catch (Exception e) {
						LOGGER.info("Error TradeOptimizer :- " + e.getMessage() + " >> " + e.getCause());
					}
				}
			} catch (IOException e) {
				LOGGER.info("Error TradeOptimizer :- " + e.getMessage() + " >> " + e.getCause());
			}
		}
		streamingQuoteStorage.saveInstrumentVolumeData(instrumentVolumeLast10DaysDataList);
		// LOGGER.info("Exit TradeOptimizer.fetchNSEActiveSymbolList()");
	}
	//@RequestMapping(value = "/start", method = { RequestMethod.POST, RequestMethod.GET })	
	public void startProcess() {
		try {
			// LOGGER.info("Entry TradeOptimizer.startProcess()");
			TimeZone.setDefault(TimeZone.getTimeZone("IST"));
			todaysDate = dtFmt.format(Calendar.getInstance().getTime());
			quoteStartTime = todaysDate + " " + StreamingConfig.QUOTE_STREAMING_START_TIME;
			quoteEndTime = todaysDate + " " + StreamingConfig.QUOTE_STREAMING_END_TIME;
			quotePrioritySettingTime = todaysDate + " " + StreamingConfig.QUOTE_PRIORITY_SETTING_TIME;
			dbConnectionClosingTime = todaysDate + " " + StreamingConfig.DB_CONNECTION_CLOSING_TIME;
			timeStart = dtTmFmt.parse(quoteStartTime);
			timeEnd = dtTmFmt.parse(quoteEndTime);
			timeForPrioritySetting = dtTmFmt.parse(quotePrioritySettingTime);
			timeDBConnectionClosing = dtTmFmt.parse(dbConnectionClosingTime);

			createInitialDayTables();

			if (checkAndLoadIfBackEndReady()) {
				quoteStreamingInstrumentsArr = streamingQuoteStorage.getTopPrioritizedTokenList(tokenCountForTrade);
				operatingTradingSymbolList = streamingQuoteStorage
						.tradingSymbolListOnInstrumentTokenId(quoteStreamingInstrumentsArr);
				liveStreamFirstRun = true;
				backendReadyForProcessing = true;
			} else {
				fetchAndProcessInstrumentsPriorityRelatedData();
			}
			 startLiveStreamOfSelectedInstruments();
			//saveGoogleHistoricData(operatingTradingSymbolList);
			// applyStrategiesAndGenerateSignals();

			// placeOrdersBasedOnSignals();

			// orderStatusSyncBetweenLocalAndMarket();

			// dayClosingStocksRoundOffOperations();

		} catch (JSONException | ParseException e) {
			LOGGER.info("Error TradeOptimizer.startProcess(): " + e.getMessage() + " >> " + e.getCause());
		}
		// LOGGER.info("Exit TradeOptimizer.startProcess()");
	}

	private void saveGoogleHistoricData(ArrayList<String> operatingTradingSymbolList) {

		// LOGGER.info("Entry TradeOptimizer.fetchNSEActiveSymbolList()");
		@SuppressWarnings({ "resource" })
		HttpClient client = new DefaultHttpClient();
		ArrayList<ArrayList<InstrumentOHLCData>> instrumentVolumeLast10DaysDataList = new ArrayList<ArrayList<InstrumentOHLCData>>();
		for (int count = 0; count < operatingTradingSymbolList.size(); count++) {
			ArrayList<InstrumentOHLCData> tempList = new ArrayList<InstrumentOHLCData>();
			HttpGet get = new HttpGet(
					StreamingConfig.googleFetchDataString(operatingTradingSymbolList.get(count), 1, 60));
			get.addHeader(StreamingConfig.ACCESS_CONTROL_ALLOW_ORIGIN, "*");
			get.addHeader("user-agent", StreamingConfig.USER_AGENT_VALUE);

			try {
				HttpResponse response = client.execute(get);
				BufferedReader rd = new BufferedReader(new InputStreamReader(response.getEntity().getContent()));
				String line = "";
				String[] readData;
				int lineSkip = 8;
				InstrumentOHLCData instrumentOHLCData;
				while ((line = rd.readLine()) != null) {
					try {
						readData = line.split(",");
						if (lineSkip == 0) {
							instrumentOHLCData = new InstrumentOHLCData();
							instrumentOHLCData.setInstrumentName(operatingTradingSymbolList.get(count));
							instrumentOHLCData.setClose(Double.parseDouble(readData[1]));
							instrumentOHLCData.setHigh(Double.parseDouble(readData[2]));
							instrumentOHLCData.setLow(Double.parseDouble(readData[3]));
							instrumentOHLCData.setOpen(Double.parseDouble(readData[4]));
							tempList.add(instrumentOHLCData);
						} else
							lineSkip = lineSkip - 1;

					} catch (Exception e) {
						LOGGER.info("Error TradeOptimizer :- " + e.getMessage() + " >> " + e.getCause());
					}
				}
				instrumentVolumeLast10DaysDataList.add(tempList);
			} catch (IOException e) {
				LOGGER.info("Error TradeOptimizer :- " + e.getMessage() + " >> " + e.getCause());
			}
		}
		streamingQuoteStorage.saveGoogleHistoricalData(instrumentVolumeLast10DaysDataList);
		// LOGGER.info("Exit TradeOptimizer.fetchNSEActiveSymbolList()");

	}

	private void orderStatusSyncBetweenLocalAndMarket() {

		// LOGGER.info("Entry
		// TradeOptimizer.orderStatusSyncBetweenLocalAndMarket()");
		Thread t = new Thread(new Runnable() {
			private boolean runnable = true;

			@Override
			public void run() {
				while (runnable) {
					try {
						Date timeNow = Calendar.getInstance().getTime();
						if (timeNow.compareTo(timeStart) >= 0 && timeNow.compareTo(timeEnd) <= 0) {
							if (backendReadyForProcessing) {
								List<Order> orderList = tradeOperations.getOrders(kiteconnect);

								if (null != orderList && orderList.size() > 0 && null != operatingTradingSymbolList
										&& operatingTradingSymbolList.size() > 0)
									for (int instrumentCount = 0; instrumentCount < operatingTradingSymbolList
											.size(); instrumentCount++) {
										for (int index = 0; index < orderList.size(); index++) {
											if (orderList.get(index).tradingSymbol
													.equalsIgnoreCase(operatingTradingSymbolList.get(instrumentCount))
													&& !orderList.get(index).status.equalsIgnoreCase("OPEN")) {
												streamingQuoteStorage.orderStatusSyncBetweenLocalAndMarket(
														orderList.get(index).tradingSymbol,
														orderList.get(index).transactionType,
														orderList.get(index).quantity, orderList.get(index).status,
														orderList.get(index).tag);
											}
										}
									}
							}
							Thread.sleep(60 * seconds);
							runnable = true;
						} else {
							runnable = false;
						}
					} catch (InterruptedException e) {
						LOGGER.info("Error TradeOptimizer :- " + e.getMessage() + " >> " + e.getCause());
					} catch (KiteException e) {
						LOGGER.info("Error TradeOptimizer :- " + e.message + " >> " + e.code);
					} catch (Exception e) {
						LOGGER.info("Error TradeOptimizer :- " + e.getMessage() + " >> " + e.getCause());
					}
				}
			}
		});
		t.start();
		// LOGGER.info("Exit
		// TradeOptimizer.orderStatusSyncBetweenLocalAndMarket()");
	}

	private boolean checkAndLoadIfBackEndReady() {
		return streamingQuoteStorage.getBackendReadyFlag();
	}

	private void tickerSettingInitialization() {
		// LOGGER.info("Entry TradeOptimizer.tickerSettingInitialization()");

		tickerProvider = new KiteTicker(kiteconnect);
		tickerProvider.setTryReconnection(true);
		try {
			tickerProvider.setTimeIntervalForReconnection(5);
		} catch (KiteException e) {
			LOGGER.info("Error TradeOptimizer :- " + e.message + " >> " + e.code);
			tickerStarted = false;
		}
		tickerProvider.setMaxRetries(-1);

		

		tickerProvider.setOnConnectedListener(new OnConnect() {
			@Override
			public void onConnected() {
				if (null != tokenListForTick
						&& tickerProvider.getSubscribedTokenList().size() != tokenListForTick.size())
					try {
						tickerProvider.subscribe(tokenListForTick);
						if (null != tokenListForTick && !tokenListForTick.isEmpty())
							tickerProvider.setMode(tokenListForTick, KiteTicker.modeQuote);
					} catch (KiteException e) {
						LOGGER.info("Error TradeOptimizer :- " + e.message + " >> " + e.code);
						tickerStarted = false;
					} catch (IOException | WebSocketException e) {
						LOGGER.info("Error TradeOptimizer :- " + e.getMessage() + " >> " + e.getCause());
						tickerStarted = false;
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
				// if (null != quoteStreamingInstrumentsArr &&
				// quoteStreamingInstrumentsArr.size() > 0)
				// ticks = testingTickerData(quoteStreamingInstrumentsArr);
				if (ticks.size() > 0)
					streamingQuoteStorage.storeData(ticks);
				else if (tickerStarted && (null != tickerProvider && null != tokenListForTick
						&& tickerProvider.getSubscribedTokenList().size() != tokenListForTick.size()))
					try {
						tickerProvider.reconnect(tokenListForTick);
					} catch (Exception e) {
						LOGGER.info("Error TradeOptimizer :- " + e.getMessage() + " >> " + e.getCause());
						tickerStarted = false;
					}
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

			streamingQuoteStorage.initializeJDBCConn();
			try {
				streamingQuoteStorage.createDaysStreamingQuoteTable();
			} catch (SQLException e) {
				LOGGER.info("Error TradeOptimizer :- " + e.getMessage() + " >> " + e.getCause());
			}
		}
		// LOGGER.info("Exit TradeOptimizer.createInitialDayTables()");
	}

	private void dayClosingStocksRoundOffOperations() {
		// LOGGER.info("Entry
		// TradeOptimizer.dayClosingStocksRoundOffOperations()");

		Thread t = new Thread(new Runnable() {
			private boolean runnable = true;
			private boolean needToWaitBeforeClosingThread = false;

			@Override
			public void run() {
				while (runnable) {
					try {
						Date timeNow = Calendar.getInstance().getTime();
						if (timeNow.compareTo(timeEnd) >= 0 && backendReadyForProcessing) {
							List<Order> orderList = new ArrayList<Order>();
							streamingQuoteStorage.fetchAllOrdersForDayOffActivity(quoteStreamingInstrumentsArr);// tradeOperations.getOrders(kiteconnect);

							if (null != orderList && orderList.size() > 0 && null != operatingTradingSymbolList
									&& operatingTradingSymbolList.size() > 0)
								for (int instrumentCount = 0; instrumentCount < operatingTradingSymbolList
										.size(); instrumentCount++) {
									/*
									 * int totalQuantity = 0; for (int index =
									 * 0; index < orderList.size(); index++) {
									 * if (orderList.get(index).tradingSymbol
									 * .equalsIgnoreCase(
									 * operatingTradingSymbolList.get(
									 * instrumentCount))) { if
									 * (orderList.get(index).status.
									 * equalsIgnoreCase("OPEN")) {
									 * tradeOperations.cancelOrder(kiteconnect,
									 * orderList.get(index));
									 * needToWaitBeforeClosingThread = true; }
									 * if (orderList.get(index).status.
									 * equalsIgnoreCase("COMPLETE") &&
									 * orderList.get(index).transactionType.
									 * equalsIgnoreCase("BUY")) { totalQuantity
									 * = totalQuantity +
									 * Integer.parseInt(orderList.get(index).
									 * quantity); } if
									 * (orderList.get(index).status.
									 * equalsIgnoreCase("COMPLETE") &&
									 * orderList.get(index).transactionType.
									 * equalsIgnoreCase("SELL")) { totalQuantity
									 * = totalQuantity -
									 * Integer.parseInt(orderList.get(index).
									 * quantity); } } } if (totalQuantity > 0) {
									 * tradeOperations.placeOrder(kiteconnect,
									 * operatingTradingSymbolList.get(
									 * instrumentCount), "SELL",
									 * String.valueOf(totalQuantity), "0.0",
									 * "dayOff", streamingQuoteStorage);
									 * needToWaitBeforeClosingThread = true; }
									 * else if (totalQuantity < 0) {
									 * tradeOperations.placeOrder(kiteconnect,
									 * operatingTradingSymbolList.get(
									 * instrumentCount), "BUY",
									 * String.valueOf(totalQuantity).replaceAll(
									 * "-", ""), "0.0", "dayOff",
									 * streamingQuoteStorage);
									 * needToWaitBeforeClosingThread = true; }
									 */}
							if (!needToWaitBeforeClosingThread) {
								runnable = false;
								dayOffActivityPerformed = true;
							} else {
								Thread.sleep(10 * seconds);
								runnable = true;
							}
						} else {
							Thread.sleep(10 * seconds);
							runnable = true;
						}
					} catch (InterruptedException e) {
						LOGGER.info("Error TradeOptimizer :- " + e.getMessage() + " >> " + e.getCause());
						// } catch (KiteException e) {
						// LOGGER.info("Error TradeOptimizer :- " + e.message +
						// " >> " + e.code);
					} catch (Exception e) {
						LOGGER.info("Error TradeOptimizer :- " + e.getMessage() + " >> " + e.getCause());
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
						Date timeNow = Calendar.getInstance().getTime();
						if (timeNow.compareTo(timeStart) >= 0 && timeNow.compareTo(timeEnd) <= 0) {
							if (backendReadyForProcessing) {
								List<Order> signalList = getOrderListToPlaceAsPerStrategySignals();
								for (int index = 0; index < signalList.size(); index++)
									tradeOperations.placeOrder(kiteconnect);
							}
							Thread.sleep(10 * seconds);
						} else {
							runnable = false;
						}

					} catch (InterruptedException e) {
						LOGGER.info("Error TradeOptimizer :- " + e.getMessage() + " >> " + e.getCause());
					} catch (KiteException e) {
						LOGGER.info("Error TradeOptimizer :- " + e.message + " >> " + e.code);
					} catch (Exception e) {
						LOGGER.info("Error TradeOptimizer :- " + e.getMessage() + " >> " + e.getCause());
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
						Date timeNow = Calendar.getInstance().getTime();
						if (timeNow.compareTo(timeStart) >= 0 && timeNow.compareTo(timeEnd) <= 0) {
							if (backendReadyForProcessing)
								startStreamingQuote();
							Thread.sleep(10 * seconds);
						} else if (timeNow.compareTo(timeEnd) >= 0 && timeNow.compareTo(timeDBConnectionClosing) <= 0) {
							if (!streamingQuoteStopped)
								stopStreamingQuote();
							Thread.sleep(10 * seconds);
						} else {
							if (dayOffActivityPerformed) {
								closeDBConnection();
								runnable = false;
							} else {
								Thread.sleep(10 * seconds);
							}
						}
					} catch (InterruptedException e) {
						LOGGER.info("Error TradeOptimizer :- " + e.getMessage() + " >> " + e.getCause());
					} catch (Exception e) {
						LOGGER.info("Error TradeOptimizer :- " + e.getMessage() + " >> " + e.getCause());
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
		Thread t = new Thread(new Runnable() {
			private boolean runnable = true;

			@Override
			public void run() {

				while (runnable) {
					try {
						Date timeNow = Calendar.getInstance().getTime();
						if (timeNow.compareTo(timeStart) >= 0 && timeNow.compareTo(timeEnd) <= 0
								&& timeNow.compareTo(timeForPrioritySetting) < 0) {
							runnable = true;
							Thread.sleep(30 * seconds);
						} else if (timeNow.compareTo(timeStart) >= 0 && timeNow.compareTo(timeEnd) <= 0
								&& timeNow.compareTo(timeForPrioritySetting) >= 0) {
							try {
								instrumentVolatilityScoreList = markTradableInstruments();

								tempInstrumentList = tradeOperations.getInstrumentsForExchange(kiteconnect);
								for (int count = 0; count < tempInstrumentList.size(); count++) {

									Instrument temp = tempInstrumentList.get(count);

									if (nseTradableInstrumentSymbol.contains(temp.getTradingsymbol()))
										instrumentList.add(temp);
								}
								streamingQuoteStorage.saveInstrumentDetails(instrumentList,
										new Timestamp(Calendar.getInstance().getTime().getTime()));

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
								last10DaysOHLCData();
								fetchNSESymbolVolumeData();
								fetchNSEActiveSymbolList();
								downloadNSEReportsCSVs();
								streamingQuoteStorage.markTradableInstruments(instrumentVolatilityScoreList);

								quoteStreamingInstrumentsArr = streamingQuoteStorage
										.getTopPrioritizedTokenList(tokenCountForTrade);
								operatingTradingSymbolList = streamingQuoteStorage
										.tradingSymbolListOnInstrumentTokenId(quoteStreamingInstrumentsArr);
								liveStreamFirstRun = true;
								backendReadyForProcessing = true;
								streamingQuoteStorage.saveBackendReadyFlag(backendReadyForProcessing);
								runnable = false;
							} catch (KiteException e) {
								LOGGER.info("Error TradeOptimizer :- " + e.message + " >> " + e.code);
							} catch (Exception e) {
								LOGGER.info("Error TradeOptimizer :- " + e.getMessage() + " >> " + e.getCause());
							}
						} else {
							runnable = false;
						}

					} catch (InterruptedException e) {
						LOGGER.info("Error TradeOptimizer :- " + e.getMessage() + " >> " + e.getCause());
					} catch (Exception e) {
						LOGGER.info("Error TradeOptimizer :- " + e.getMessage() + " >> " + e.getCause());
					}
				}
			}
		});
		t.start();

		// LOGGER.info("Exit
		// TradeOptimizer.fetchAndProcessInstrumentPriorityRelatedData()");
	}

	private void last10DaysOHLCData() {
		HashMap<String, ArrayList<InstrumentOHLCData>> instrumentOHLCLast10DaysDataList = new HashMap<String, ArrayList<InstrumentOHLCData>>();
		for (int index = 0; index < StreamingConfig.last10DaysOHLCFileUrls.length; index++) {
			try {
				URL website = new URL(StreamingConfig.last10DaysOHLCFileUrls[index]);
				BufferedInputStream in = null;
				FileOutputStream fout = null;

				try {
					in = new BufferedInputStream(website.openStream());
					fout = new FileOutputStream(
							StreamingConfig.last10DaysOHLCZipFilePath + StreamingConfig.last10DaysOHLCFileNames[index]);

					final byte data[] = new byte[1024];
					int count;
					while ((count = in.read(data, 0, 1024)) != -1) {
						fout.write(data, 0, count);
					}
				} finally {
					if (in != null)
						in.close();

					if (fout != null)
						fout.close();
				}
				ZipFile zipFile = new ZipFile(
						StreamingConfig.last10DaysOHLCZipFilePath + StreamingConfig.last10DaysOHLCFileNames[index]);
				Enumeration<? extends ZipEntry> entries = zipFile.entries();

				while (entries.hasMoreElements()) {
					ZipEntry entry = entries.nextElement();
					InputStream stream = zipFile.getInputStream(entry);
					BufferedReader rd = new BufferedReader(new InputStreamReader(stream));
					String line = "";
					String[] readData;
					boolean firstLine = true;
					while ((line = rd.readLine()) != null) {
						try {
							readData = line.split(",");
							if (!firstLine) {
								InstrumentOHLCData instrumentOHLCData = new InstrumentOHLCData();
								instrumentOHLCData.setInstrumentName(readData[0]);
								instrumentOHLCData.setClose(Double.parseDouble(readData[5]));
								instrumentOHLCData.setHigh(Double.parseDouble(readData[3]));
								instrumentOHLCData.setLow(Double.parseDouble(readData[4]));
								instrumentOHLCData.setOpen(Double.parseDouble(readData[2]));
								if (nseTradableInstrumentSymbol.contains(readData[0])) {
									if (instrumentOHLCLast10DaysDataList.containsKey(readData[0]))
										instrumentOHLCLast10DaysDataList.get(readData[0]).add(instrumentOHLCData);
									else {
										ArrayList<InstrumentOHLCData> tempList = new ArrayList<InstrumentOHLCData>();
										tempList.add(instrumentOHLCData);
										instrumentOHLCLast10DaysDataList.put(readData[0], tempList);
									}
								}
							}
							firstLine = false;
						} catch (Exception e) {
							LOGGER.info("Error TradeOptimizer :- " + e.getMessage() + " >> " + e.getCause());
						}
					}
				}
				zipFile.close();
			} catch (Exception e) {
				LOGGER.info("Error TradeOptimizer :- " + e.getMessage() + " >> " + e.getCause());
			}
		}
		streamingQuoteStorage.last10DaysOHLCData(instrumentOHLCLast10DaysDataList);
	}

	// private void roundOfNonPerformingBoughtStocks(ArrayList<Long>
	// quoteStreamingInstrumentsArr,
	// ArrayList<Long> previousQuoteStreamingInstrumentsArr) throws
	// KiteException {
	// // LOGGER.info("Entry
	// // TradeOptimizer.roundOfNonPerformingBoughtStocks()");
	//
	// ArrayList<Long> unSubList = new ArrayList<Long>();
	// ArrayList<Long> subList = new ArrayList<Long>();
	// for (int index = 0; index < previousQuoteStreamingInstrumentsArr.size();
	// index++) {
	// boolean quoteExistInNewList = false;
	// for (int count = 0; count < quoteStreamingInstrumentsArr.size(); count++)
	// {
	// if (quoteStreamingInstrumentsArr.get(count).longValue() ==
	// previousQuoteStreamingInstrumentsArr
	// .get(index).longValue()) {
	// quoteExistInNewList = true;
	// }
	// }
	// if (!quoteExistInNewList) {
	// unSubList.add(previousQuoteStreamingInstrumentsArr.get(index));
	// }
	// }
	// for (int index = 0; index < quoteStreamingInstrumentsArr.size(); index++)
	// {
	// boolean newQuote = true;
	// for (int count = 0; count < previousQuoteStreamingInstrumentsArr.size();
	// count++) {
	// if (quoteStreamingInstrumentsArr.get(index).longValue() ==
	// previousQuoteStreamingInstrumentsArr
	// .get(count).longValue()) {
	// newQuote = false;
	// }
	// }
	// if (newQuote) {
	// subList.add(quoteStreamingInstrumentsArr.get(index));
	// }
	// }
	// List<Order> orders = tradeOperations.getOrders(kiteconnect);
	// if (null != orders && orders.size() > 0)
	// for (int index = 0; index < orders.size(); index++) {
	// boolean OrderCancelRequired = true;
	// for (int count = 0; count < quoteStreamingInstrumentsArr.size(); count++)
	// {
	// if (orders.get(index).tradingSymbol
	// .equalsIgnoreCase(quoteStreamingInstrumentsArr.get(count).toString())) {
	// OrderCancelRequired = false;
	// }
	// }
	// if (OrderCancelRequired) {
	// if (orders.get(index).status.equalsIgnoreCase("OPEN"))
	// tradeOperations.cancelOrder(kiteconnect, orders.get(index));
	// }
	// }
	//
	// Holding holdingList = tradeOperations.getHoldings(kiteconnect);
	// List<Holding> holdings = new ArrayList<Holding>();
	// if (null != holdingList && null != holdingList.holdings)
	// holdings = holdingList.holdings;
	// for (int index = 0; index < holdings.size(); index++) {
	// boolean orderSellOutRequired = true;
	// for (int count = 0; count < quoteStreamingInstrumentsArr.size(); count++)
	// {
	// if (holdings.get(index).instrumentToken
	// .equalsIgnoreCase(quoteStreamingInstrumentsArr.get(count).toString())) {
	// orderSellOutRequired = false;
	// }
	// }
	// if (orderSellOutRequired) {
	// tradeOperations.placeOrder(kiteconnect,
	// holdings.get(index).tradingSymbol, "SELL",
	// holdings.get(index).quantity, streamingQuoteStorage);
	// }
	// }
	//
	// if (liveStreamFirstRun) {
	// try {
	// if (!tickerStarted)
	// tickerSettingInitialization();
	// tickerProvider.connect();
	// if (null != unSubList && unSubList.size() > 0)
	// tickerProvider.unsubscribe(unSubList);
	// if (null != subList && subList.size() > 0) {
	// tickerProvider.subscribe(subList);
	// tickerStarted = true;
	// }
	// } catch (WebSocketException | IOException e) {
	// LOGGER.info("Error TradeOptimizer :- " + e.getMessage() +" >> " +
	// e.getCause());
	// }
	// }
	//
	// // LOGGER.info("Exit
	// // TradeOptimizer.roundOfNonPerformingBoughtStocks()");
	// }

	private void applyStrategiesAndGenerateSignals() {
		// LOGGER.info("Entry
		// TradeOptimizer.applyStrategiesAndGenerateSignals()");

		Thread t = new Thread(new Runnable() {
			private boolean runnable = true;

			@Override
			public void run() {

				while (runnable) {
					try {
						Date timeNow = Calendar.getInstance().getTime();
						if (timeNow.compareTo(timeStart) >= 0 && timeNow.compareTo(timeEnd) <= 0) {
							if (backendReadyForProcessing) {
								ArrayList<Long> instrumentList = getInstrumentTokensList();
								Map<Long, String> signalList = new HashMap<Long, String>();

								if (null != instrumentList && instrumentList.size() > 0) {
									for (int i = 0; i < instrumentList.size(); i++)
										streamingQuoteStorage.calculateAndStoreStrategySignalParameters(
												instrumentList.get(i).toString(), timeNow);
									signalList = streamingQuoteStorage
											.calculateSignalsFromStrategyParams(instrumentList);
									streamingQuoteStorage.saveGeneratedSignals(signalList, instrumentList);
								}
							}
							Thread.sleep(60 * seconds);
						} else {
							runnable = false;
						}

					} catch (InterruptedException e) {
						LOGGER.info("Error TradeOptimizer :- " + e.getMessage() + " >> " + e.getCause());
					} catch (Exception e) {
						LOGGER.info("Error TradeOptimizer :- " + e.getMessage() + " >> " + e.getCause());
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
					if (!tickerStarted || (null != tickerProvider && tickerProvider.getSubscribedTokenList().size() == 0
							&& tokenListForTick.size() > 0))
						tickerSettingInitialization();
					tickerProvider.connect();
					if (!tickerStarted || (null != tickerProvider && tickerProvider.getSubscribedTokenList().size() == 0
							&& tokenListForTick.size() > 0))
						tickerProvider.subscribe(tokenListForTick);
					tickerStarted = true;
				}
			} catch (IOException | WebSocketException e) {
				LOGGER.info("Error TradeOptimizer :- " + e.getMessage() + " >> " + e.getCause());
				tickerStarted = false;
			} catch (KiteException e) {
				LOGGER.info("Error TradeOptimizer :- " + e.message + " >> " + e.code);
				tickerStarted = false;
			} catch (Exception e) {
				LOGGER.info("Error TradeOptimizer :- " + e.getMessage() + " >> " + e.getCause());
				tickerStarted = false;
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
		streamingQuoteStopped = true;
		// LOGGER.info("Exit TradeOptimizer.stopStreamingQuote()");
	}

	private void closeDBConnection() {
		// LOGGER.info("Entry TradeOptimizer.closeDBConnection()");
		if (streamingQuoteStorage != null) {
			streamingQuoteStorage.closeJDBCConn();
		}
		// LOGGER.info("Exit TradeOptimizer.closeDBConnection()");
	}
}
