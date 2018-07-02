package com.gold.buzzer.main;

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

import com.gold.buzzer.dao.StreamingQuoteStorage;
import com.gold.buzzer.dao.StreamingQuoteStorageImpl;
import com.gold.buzzer.models.InstrumentOHLCData;
import com.gold.buzzer.models.InstrumentVolatilityScore;
import com.gold.buzzer.utils.StreamingConfig;
import com.zerodhatech.kiteconnect.KiteConnect;
import com.zerodhatech.kiteconnect.kitehttp.exceptions.KiteException;
import com.zerodhatech.models.Instrument;
import com.zerodhatech.models.Order;
import com.zerodhatech.models.Tick;
import com.zerodhatech.models.User;
import com.zerodhatech.ticker.KiteTicker;
import com.zerodhatech.ticker.OnConnect;
import com.zerodhatech.ticker.OnDisconnect;
import com.zerodhatech.ticker.OnTicks;

@SuppressWarnings("deprecation")
@Controller
public class GRSRuleEngine {

	private final static Logger LOGGER = Logger.getLogger(GRSRuleEngine.class.getName());

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
	private String url = kiteconnect.getLoginURL();
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
		try {
			User userModel = kiteconnect.generateSession(request_token, StreamingConfig.API_SECRET_KEY);
			kiteconnect.setAccessToken(userModel.accessToken);
			kiteconnect.setPublicToken(userModel.publicToken);
			bulkProcessSignal();
		} catch (KiteException e) {
			LOGGER.info("Error GRSRuleEngine.authRedirectWithTokenPost(): " + e.message + " >> " + e.code);
			return "error";
		} catch (JSONException e) {
			LOGGER.info("Error GRSRuleEngine.authRedirectWithTokenPost(): " + e.getMessage() + " >> " + e.getCause());
			return "error";
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return "index";
	}

	// @RequestMapping(value = "/start", method = { RequestMethod.POST,
	// RequestMethod.GET })
	public RedirectView localRedirect() {
		RedirectView redirectView = new RedirectView();
		redirectView.setUrl(url);
		return redirectView;
	}

	public void saveInstrumentVolatilityData() {

		@SuppressWarnings({ "resource" })
		HttpClient client = new DefaultHttpClient();
		HashMap<String, ArrayList<InstrumentOHLCData>> instrumentVolatilityScoreList = new HashMap<String, ArrayList<InstrumentOHLCData>>();
		for (int count = 0; count < StreamingConfig.getNseVolatilityDataUrl().length; count++) {
			HttpGet get = new HttpGet(StreamingConfig.getNseVolatilityDataUrl()[count]);
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
						LOGGER.info("Error GRSRuleEngine :- " + e.getMessage() + " >> " + e.getCause());
					}
				}
			} catch (IOException e) {
				LOGGER.info("Error GRSRuleEngine :- " + e.getMessage() + " >> " + e.getCause());
			}
		}
		streamingQuoteStorage.saveInstrumentVolatilityData(instrumentVolatilityScoreList);
	}

	public List<InstrumentVolatilityScore> markInstrumentsTradable() {

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
					LOGGER.info("Error GRSRuleEngine :- " + e.getMessage() + " >> " + e.getCause());
				}
			}
		} catch (IOException e) {
			LOGGER.info("Error GRSRuleEngine :- " + e.getMessage() + " >> " + e.getCause());
		}
		return instrumentVolatilityScoreList;
	}

	public void saveInstrumentTokenPriorityData() {
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
							LOGGER.info("Error GRSRuleEngine :- " + e.getMessage() + " >> " + e.getCause());
						}
					}
			} catch (IOException e) {
				LOGGER.info("Error GRSRuleEngine :- " + e.getMessage() + " >> " + e.getCause());
			}
		}
		streamingQuoteStorage.saveInstrumentTokenPriorityData(stocksSymbolArray);
	}

	public void saveInstrumentVolumeData() {
		@SuppressWarnings({ "resource" })
		HttpClient client = new DefaultHttpClient();
		HashMap<String, ArrayList<InstrumentOHLCData>> instrumentVolumeLast10DaysDataList = new HashMap<String, ArrayList<InstrumentOHLCData>>();
		for (int count = 0; count < StreamingConfig.getLast10DaysVolumeDataFileUrls().length; count++) {
			HttpGet get = new HttpGet(StreamingConfig.getLast10DaysVolumeDataFileUrls()[2]);
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
						LOGGER.info("Error GRSRuleEngine :- " + e.getMessage() + " >> " + e.getCause());
					}
				}
			} catch (IOException e) {
				LOGGER.info("Error GRSRuleEngine :- " + e.getMessage() + " >> " + e.getCause());
			}
		}
		streamingQuoteStorage.saveInstrumentVolumeData(instrumentVolumeLast10DaysDataList);
	}

	@RequestMapping(value = "/start", method = { RequestMethod.POST, RequestMethod.GET })
	public void bulkProcessSignal() {
		/*
		 * StreamingConfig.TEMP_TEST_DATE = "01012018"; startProcess();
		 * StreamingConfig.TEMP_TEST_DATE = "02012018"; startProcess();
		 */
		// StreamingConfig.TEMP_TEST_DATE = "03012018";
		// startProcess();
		StreamingConfig.TEMP_PRE_TEST_DATE = "03012018";
		StreamingConfig.TEMP_TEST_DATE = "04012018";
		startProcess();
		/*
		 * StreamingConfig.TEMP_TEST_DATE = "05012018"; startProcess();
		 * StreamingConfig.TEMP_TEST_DATE = "08012018"; startProcess();
		 * StreamingConfig.TEMP_TEST_DATE = "09012018"; startProcess();
		 * StreamingConfig.TEMP_TEST_DATE = "10012018"; startProcess();
		 * StreamingConfig.TEMP_TEST_DATE = "11012018"; startProcess();
		 * StreamingConfig.TEMP_TEST_DATE = "12012018"; startProcess(); /*
		 * StreamingConfig.TEMP_TEST_DATE = "15012018"; startProcess();
		 * StreamingConfig.TEMP_TEST_DATE = "16012018"; startProcess();
		 * StreamingConfig.TEMP_TEST_DATE = "17012018"; startProcess();
		 * StreamingConfig.TEMP_TEST_DATE = "18012018"; startProcess();
		 * StreamingConfig.TEMP_TEST_DATE = "19012018"; startProcess();
		 * StreamingConfig.TEMP_TEST_DATE = "22012018"; startProcess();
		 * StreamingConfig.TEMP_TEST_DATE = "23012018"; startProcess();
		 * StreamingConfig.TEMP_TEST_DATE = "24012018"; startProcess();
		 * StreamingConfig.TEMP_TEST_DATE = "25012018"; startProcess();
		 * StreamingConfig.TEMP_TEST_DATE = "29012018"; startProcess();
		 * StreamingConfig.TEMP_TEST_DATE = "30012018"; startProcess();
		 * StreamingConfig.TEMP_TEST_DATE = "31012018"; startProcess();
		 */
		System.out.println("ALL DONE !!");
	}

	// @RequestMapping(value = "/start", method = { RequestMethod.POST,
	// RequestMethod.GET })
	public void startProcess() {
		try {
			System.out.println(StreamingConfig.TEMP_TEST_DATE);
			StreamingConfig.TEMP_TEST_DATE = StreamingConfig.TEMP_TEST_DATE + "_hist";
			StreamingConfig.TEMP_PRE_TEST_DATE = StreamingConfig.TEMP_PRE_TEST_DATE + "_hist";
			nseTradableInstrumentSymbol = new HashSet<String>();
			instrumentVolatilityScoreList = new ArrayList<InstrumentVolatilityScore>();
			/*
			 * TimeZone.setDefault(TimeZone.getTimeZone("IST")); todaysDate =
			 * dtFmt.format(Calendar.getInstance().getTime()); quoteStartTime = todaysDate +
			 * " " + StreamingConfig.QUOTE_STREAMING_START_TIME; quoteEndTime = todaysDate +
			 * " " + StreamingConfig.QUOTE_STREAMING_END_TIME; quotePrioritySettingTime =
			 * todaysDate + " " + StreamingConfig.QUOTE_PRIORITY_SETTING_TIME;
			 * dbConnectionClosingTime = todaysDate + " " +
			 * StreamingConfig.DB_CONNECTION_CLOSING_TIME; timeStart =
			 * dtTmFmt.parse(quoteStartTime); timeEnd = dtTmFmt.parse(quoteEndTime);
			 * timeForPrioritySetting = dtTmFmt.parse(quotePrioritySettingTime);
			 * timeDBConnectionClosing = dtTmFmt.parse(dbConnectionClosingTime);
			 */
			createInitialDayTables();
			/*
			 * if (checkAndLoadIfBackEndReady()) {
			 */
			quoteStreamingInstrumentsArr = streamingQuoteStorage.getTopPrioritizedTokenList(tokenCountForTrade);
			/*
			 * operatingTradingSymbolList = streamingQuoteStorage
			 * .tradingSymbolListOnInstrumentTokenId(quoteStreamingInstrumentsArr);
			 * liveStreamFirstRun = true; backendReadyForProcessing = true; } else {
			 * fetchAndSaveInstrumentsInitialParamAndData(); }
			 */
			// startLiveStreamOfSelectedInstruments();
			// startHistoryDataCollectorOfSelectedInstruments();

			calculateStrategyAndSaveSignals();

			// placeOrdersBasedOnSignals();

			// orderStatusSyncBetweenLocalAndMarket();

			// dayClosingStocksRoundOffOperations();

		} catch (JSONException e) {
			LOGGER.info("Error GRSRuleEngine.startProcess(): " + e.getMessage() + " >> " + e.getCause());
		}
	}

	private void startHistoryDataCollectorOfSelectedInstruments() {

		SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		Date from = new Date();
		Date to = new Date();

		String[] dateList = { "01012018", "02012018", "03012018", "04012018", "05012018", "08012018", "09012018",
				"10012018", "11012018", "12012018", "15012018", "16012018", "17012018", "18012018", "19012018",
				"22012018", "23012018", "24012018", "25012018", "29012018", "30012018", "31012018" };

		String[] dateList2 = { "2018-01-01", "2018-01-02", "2018-01-03", "2018-01-04", "2018-01-05", "2018-01-08",
				"2018-01-09", "2018-01-10", "2018-01-11", "2018-01-12", "2018-01-15", "2018-01-16", "2018-01-17",
				"2018-01-18", "2018-01-19", "2018-01-22", "2018-01-23", "2018-01-24", "2018-01-25", "2018-01-29",
				"2018-01-30", "2018-01-31" };

		instrumentVolatilityScoreList = markInstrumentsTradable();

		try {
			tempInstrumentList = tradeOperations.getInstrumentsForExchange(kiteconnect, "NSE");
		} catch (IOException | KiteException e) {
			e.printStackTrace();
		}

		for (int count = 0; count < tempInstrumentList.size(); count++) {
			Instrument temp = tempInstrumentList.get(count);
			if (nseTradableInstrumentSymbol.contains(temp.getTradingsymbol()))
				instrumentList.add(temp);
		}
		String[] QUOTE_STREAMING_TRADING_HOLIDAYS = { "25-12-2017", "26-01-2018", "13-02-2018", "02-03-2018",
				"29-03-2018", "30-03-2018", "01-05-2018", "15-08-2018", "22-08-2018", "13-09-2018", "20-09-2018",
				"02-10-2018", "18-10-2018", "07-11-2018", "08-11-2018", "23-11-2018", "25-12-2018" };

		for (int index = 0; index < dateList.length; index++) {
			StreamingConfig.TEMP_TEST_DATE = dateList[index] + "_hist";
			createInitialDayTables();
			StreamingConfig.last10DaysOHLCFileNames = StreamingConfig.last10DaysFileNameListString(
					QUOTE_STREAMING_TRADING_HOLIDAYS, "cm", "bhav.csv.zip", "ddMMMyyyy", 10, true,
					StreamingConfig.TEMP_TEST_DATE);

			StreamingConfig.last10DaysVolumeDataFileNames = StreamingConfig.last10DaysFileNameListString(
					QUOTE_STREAMING_TRADING_HOLIDAYS, "MTO_", ".DAT", "ddMMyyyy", 10, false,
					StreamingConfig.TEMP_TEST_DATE);

			StreamingConfig.nseVolatilityDataFileNames = StreamingConfig.last10DaysFileNameListString(
					QUOTE_STREAMING_TRADING_HOLIDAYS, "CMVOLT_", ".CSV", "ddMMyyyy", 10, false,
					StreamingConfig.TEMP_TEST_DATE);

			// streamingQuoteStorage.saveInstrumentDetails(instrumentList,
			// new Timestamp(Calendar.getInstance().getTime().getTime()));

			saveLast10DaysOHLCData();
			saveInstrumentVolumeData();
			saveInstrumentVolatilityData();
			streamingQuoteStorage.markInstrumentsTradable(instrumentVolatilityScoreList);

			/*
			 * for (int count = 0; count < instrumentList.size(); count++) { try { from =
			 * formatter.parse(dateList2[index] + " 09:15:00"); to =
			 * formatter.parse(dateList2[index] + " 15:30:00"); HistoricalData
			 * historyDataList = kiteconnect.getHistoricalData(from, to,
			 * String.valueOf(instrumentList.get(count).getInstrument_token()), "minute",
			 * false); if (historyDataList.dataArrayList.size() > 0)
			 * streamingQuoteStorage.saveHistoryDataOfSelectedInstruments(historyDataList.
			 * dataArrayList,
			 * String.valueOf(instrumentList.get(count).getInstrument_token())); } catch
			 * (IOException | KiteException | ParseException e) { e.printStackTrace(); } }
			 */
			System.out.println(dateList[index]);
		}
	}

	private void orderStatusSyncBetweenLocalAndMarket() {

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
						LOGGER.info("Error GRSRuleEngine :- " + e.getMessage() + " >> " + e.getCause());
					} catch (KiteException e) {
						LOGGER.info("Error GRSRuleEngine :- " + e.message + " >> " + e.code);
					} catch (Exception e) {
						LOGGER.info("Error GRSRuleEngine :- " + e.getMessage() + " >> " + e.getCause());
					}
				}
			}
		});
		t.start();
	}

	private boolean checkAndLoadIfBackEndReady() {
		return streamingQuoteStorage.getBackendReadyFlag();
	}

	private void tickerSettingInitialization() throws KiteException {
		tickerProvider = new KiteTicker(kiteconnect.getAccessToken(), kiteconnect.getApiKey());
		tickerProvider.setTryReconnection(true);
		try {
			tickerProvider.setMaximumRetryInterval(5);
		} catch (KiteException e) {
			LOGGER.info("Error GRSRuleEngine :- " + e.message + " >> " + e.code);
			tickerStarted = false;
		}
		tickerProvider.setMaximumRetries(10);

		tickerProvider.setOnConnectedListener(new OnConnect() {
			@Override
			public void onConnected() {
				if (null != tokenListForTick
						&& tickerProvider.getSubscribedTokens().size() != tokenListForTick.size()) {
					tickerProvider.subscribe(tokenListForTick);
					if (null != tokenListForTick && !tokenListForTick.isEmpty())
						tickerProvider.setMode(tokenListForTick, KiteTicker.modeFull);
				}
			}
		});

		tickerProvider.setOnDisconnectedListener(new OnDisconnect() {
			@Override
			public void onDisconnected() {
				tickerProvider.unsubscribe(tokenListForTick);
			}
		});

		tickerProvider.setOnTickerArrivalListener(new OnTicks() {
			@Override
			public void onTicks(ArrayList<Tick> ticks) {
				// if (null != quoteStreamingInstrumentsArr &&
				// quoteStreamingInstrumentsArr.size() > 0)
				// ticks = testingTickerData(quoteStreamingInstrumentsArr);
				if (ticks.size() > 0)
					streamingQuoteStorage.storeTickData(ticks);
				else if (tickerStarted && (null != tickerProvider && null != tokenListForTick
						&& tickerProvider.getSubscribedTokens().size() != tokenListForTick.size()))
					try {
						// if (!tickerProvider.isConnectionOpen())
						tickerProvider.addAllTokens(tokenListForTick);
						tickerProvider.reconnect(tokenListForTick);
					} catch (Exception e) {
						LOGGER.info("Error GRSRuleEngine :- " + e.getMessage() + " >> " + e.getCause());
						tickerStarted = false;
					}
			}
		});
	}

	protected ArrayList<Tick> testingTickerData(ArrayList<Long> quoteStreamingInstrumentsArr) {

		ArrayList<Tick> ticks = new ArrayList<Tick>();
		for (int i = 0; i < quoteStreamingInstrumentsArr.size(); i++) {
			Tick tick = new Tick();
			tick.setInstrumentToken(Integer.parseInt(quoteStreamingInstrumentsArr.get(i).toString()));
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
		return ticks;
	}

	private void createInitialDayTables() {
		if (streamingQuoteStorage != null) {

			streamingQuoteStorage.initializeJDBCConn();
			try {
				streamingQuoteStorage.createDaysStreamingQuoteTable();
			} catch (SQLException e) {
				LOGGER.info("Error GRSRuleEngine :- " + e.getMessage() + " >> " + e.getCause());
			}
		}
	}

	private void dayClosingStocksRoundOffOperations() {

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
							for (int index = 0; index < quoteStreamingInstrumentsArr.size(); index++)
								streamingQuoteStorage.executeGoldBuzzStrategyCloseRoundOff(
										Long.toString(quoteStreamingInstrumentsArr.get(index)));// tradeOperations.getOrders(kiteconnect);

							if (null != orderList && orderList.size() > 0 && null != operatingTradingSymbolList
									&& operatingTradingSymbolList.size() > 0)
								for (int instrumentCount = 0; instrumentCount < operatingTradingSymbolList
										.size(); instrumentCount++) {
									/*
									 * int totalQuantity = 0; for (int index = 0; index < orderList.size(); index++)
									 * { if (orderList.get(index).tradingSymbol .equalsIgnoreCase(
									 * operatingTradingSymbolList.get( instrumentCount))) { if
									 * (orderList.get(index).status. equalsIgnoreCase("OPEN")) {
									 * tradeOperations.cancelOrder(kiteconnect, orderList.get(index));
									 * needToWaitBeforeClosingThread = true; } if (orderList.get(index).status.
									 * equalsIgnoreCase("COMPLETE") && orderList.get(index).transactionType.
									 * equalsIgnoreCase("BUY")) { totalQuantity = totalQuantity +
									 * Integer.parseInt(orderList.get(index). quantity); } if
									 * (orderList.get(index).status. equalsIgnoreCase("COMPLETE") &&
									 * orderList.get(index).transactionType. equalsIgnoreCase("SELL")) {
									 * totalQuantity = totalQuantity - Integer.parseInt(orderList.get(index).
									 * quantity); } } } if (totalQuantity > 0) {
									 * tradeOperations.placeOrder(kiteconnect, operatingTradingSymbolList.get(
									 * instrumentCount), "SELL", String.valueOf(totalQuantity), "0.0", "dayOff",
									 * streamingQuoteStorage); needToWaitBeforeClosingThread = true; } else if
									 * (totalQuantity < 0) { tradeOperations.placeOrder(kiteconnect,
									 * operatingTradingSymbolList.get( instrumentCount), "BUY",
									 * String.valueOf(totalQuantity).replaceAll( "-", ""), "0.0", "dayOff",
									 * streamingQuoteStorage); needToWaitBeforeClosingThread = true; }
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
						LOGGER.info("Error GRSRuleEngine :- " + e.getMessage() + " >> " + e.getCause());
						// } catch (KiteException e) {
						// LOGGER.info("Error GRSRuleEngine :- " + e.message +
						// " >> " + e.code);
					} catch (Exception e) {
						LOGGER.info("Error GRSRuleEngine :- " + e.getMessage() + " >> " + e.getCause());
					}
				}
			}
		});
		t.start();
	}

	private void placeOrdersBasedOnSignals() {

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
						LOGGER.info("Error GRSRuleEngine :- " + e.getMessage() + " >> " + e.getCause());
					} catch (KiteException e) {
						LOGGER.info("Error GRSRuleEngine :- " + e.message + " >> " + e.code);
					} catch (Exception e) {
						LOGGER.info("Error GRSRuleEngine :- " + e.getMessage() + " >> " + e.getCause());
					}
				}
			}
		});
		t.start();
	}

	private List<Order> getOrderListToPlaceAsPerStrategySignals() {
		return streamingQuoteStorage.fetchOrderListToPlace();
	}

	private void startLiveStreamOfSelectedInstruments() {

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
						LOGGER.info("Error GRSRuleEngine :- " + e.getMessage() + " >> " + e.getCause());
					} catch (Exception e) {
						LOGGER.info("Error GRSRuleEngine :- " + e.getMessage() + " >> " + e.getCause());
					}
				}
			}
		});
		t.start();
	}

	private void fetchAndSaveInstrumentsInitialParamAndData() {
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
								&& timeNow.compareTo(timeForPrioritySetting) >= 0 && runnable) {
							try {
								runnable = false;
								instrumentVolatilityScoreList = markInstrumentsTradable();

								tempInstrumentList = tradeOperations.getInstrumentsForExchange(kiteconnect, "NSE");
								for (int count = 0; count < tempInstrumentList.size(); count++) {

									Instrument temp = tempInstrumentList.get(count);

									if (nseTradableInstrumentSymbol.contains(temp.getTradingsymbol()))
										instrumentList.add(temp);
								}
								streamingQuoteStorage.saveInstrumentDetails(instrumentList,
										new Timestamp(Calendar.getInstance().getTime().getTime()));

								/*
								 * instrumentList.addAll(tradeOperations .getInstrumentsForExchange(kiteconnect,
								 * "BSE")); instrumentList.addAll(tradeOperations
								 * .getInstrumentsForExchange(kiteconnect, "NFO"));
								 * instrumentList.addAll(tradeOperations .getInstrumentsForExchange(kiteconnect,
								 * "BFO"));
								 */
								saveLast10DaysOHLCData();
								saveInstrumentVolumeData();
								saveInstrumentTokenPriorityData();
								saveInstrumentVolatilityData();
								streamingQuoteStorage.markInstrumentsTradable(instrumentVolatilityScoreList);

								quoteStreamingInstrumentsArr = streamingQuoteStorage
										.getTopPrioritizedTokenList(tokenCountForTrade);
								operatingTradingSymbolList = streamingQuoteStorage
										.tradingSymbolListOnInstrumentTokenId(quoteStreamingInstrumentsArr);
								liveStreamFirstRun = true;
								backendReadyForProcessing = true;
								streamingQuoteStorage.saveBackendReadyFlag(backendReadyForProcessing);
								runnable = false;
							} catch (KiteException e) {
								LOGGER.info("Error GRSRuleEngine :- " + e.message + " >> " + e.code);
							} catch (Exception e) {
								LOGGER.info("Error GRSRuleEngine :- " + e.getMessage() + " >> " + e.getCause());
							}
						} else {
							runnable = false;
						}

					} catch (InterruptedException e) {
						LOGGER.info("Error GRSRuleEngine :- " + e.getMessage() + " >> " + e.getCause());
					} catch (Exception e) {
						LOGGER.info("Error GRSRuleEngine :- " + e.getMessage() + " >> " + e.getCause());
					}
				}
			}
		});
		t.start();
	}

	private void saveLast10DaysOHLCData() {
		HashMap<String, ArrayList<InstrumentOHLCData>> instrumentOHLCLast10DaysDataList = new HashMap<String, ArrayList<InstrumentOHLCData>>();
		for (int index = 0; index < StreamingConfig.getLast10DaysOHLCFileUrls().length; index++) {
			try {
				URL website = new URL(StreamingConfig.getLast10DaysOHLCFileUrls()[index]);
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
							LOGGER.info("Error GRSRuleEngine :- " + e.getMessage() + " >> " + e.getCause());
						}
					}
				}
				zipFile.close();
			} catch (Exception e) {
				LOGGER.info("Error GRSRuleEngine :- " + e.getMessage() + " >> " + e.getCause());
			}
		}
		streamingQuoteStorage.saveLast10DaysOHLCData(instrumentOHLCLast10DaysDataList);
	}

	// private void roundOfNonPerformingBoughtStocks(ArrayList<Long>
	// quoteStreamingInstrumentsArr,
	// ArrayList<Long> previousQuoteStreamingInstrumentsArr) throws
	// KiteException {
	// // LOGGER.info("Entry
	// // GRSRuleEngine.roundOfNonPerformingBoughtStocks()");
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
	// LOGGER.info("Error GRSRuleEngine :- " + e.getMessage() +" >> " +
	// e.getCause());
	// }
	// }
	//
	// // LOGGER.info("Exit
	// // GRSRuleEngine.roundOfNonPerformingBoughtStocks()");
	// }

	private void calculateStrategyAndSaveSignals() {

		/*
		 * Thread t = new Thread(new Runnable() { private boolean runnable = true;
		 * 
		 * @Override public void run() {
		 * 
		 * while (runnable) { try {
		 */
		Date timeNow = Calendar.getInstance().getTime();
		// if (timeNow.compareTo(timeStart) >= 0 && timeNow.compareTo(timeEnd)
		// <= 0) {
		// if (backendReadyForProcessing) {
		ArrayList<Long> instrumentList = getInstrumentTokensList();

		if (null != instrumentList && instrumentList.size() > 0) {
			for (int i = 0; i < instrumentList.size(); i++)
				streamingQuoteStorage.calculateAndSaveStrategy(instrumentList.get(i).toString(), timeNow);
		}
		System.out.println("completed");
		/*
		 * } Thread.sleep(60 * seconds); } else { runnable = false; }
		 * 
		 * } catch (InterruptedException e) { LOGGER.info("Error GRSRuleEngine :- " +
		 * e.getMessage() + " >> " + e.getCause()); } catch (Exception e) {
		 * LOGGER.info("Error GRSRuleEngine :- " + e.getMessage() + " >> " +
		 * e.getCause()); } } } }); t.start();
		 */
	}

	private void startStreamingQuote() {
		tokenListForTick = getInstrumentTokensList();
		if (null != tokenListForTick && tokenListForTick.size() > 0) {
			try {
				if (liveStreamFirstRun) {
					if (!tickerStarted || (null != tickerProvider && tickerProvider.getSubscribedTokens().size() == 0
							&& tokenListForTick.size() > 0)) {
						tickerSettingInitialization();

						if (tickerProvider.isConnectionCreated())
							tickerProvider.connect();
						else if (!tickerProvider.isConnectionOpen())
							tickerProvider.reconnect(tokenListForTick);

						tickerProvider.subscribe(tokenListForTick);
						tickerStarted = true;
					}
				}
			} catch (Exception | KiteException e) {
				LOGGER.info("Error GRSRuleEngine :- " + e.getMessage() + " >> " + e.getCause());
				tickerStarted = false;
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
		streamingQuoteStopped = true;
	}

	private void closeDBConnection() {
		if (streamingQuoteStorage != null) {
			streamingQuoteStorage.closeJDBCConn();
		}
	}
}
