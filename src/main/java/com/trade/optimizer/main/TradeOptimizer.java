package com.trade.optimizer.main;

import java.io.IOException;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
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

import com.neovisionaries.ws.client.WebSocketException;
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
import com.trade.optimizer.models.Tick;
import com.trade.optimizer.models.UserModel;
import com.trade.optimizer.tickerws.KiteTicker;
import com.trade.optimizer.tickerws.OnConnect;
import com.trade.optimizer.tickerws.OnDisconnect;
import com.trade.optimizer.tickerws.OnTick;

@Controller
public class TradeOptimizer {

    private static int tokenCountForTrade = 10;
    private static int seconds = 1000;
    private static StreamingQuoteStorage streamingQuoteStorage = new StreamingQuoteStorageImpl();
    private static StreamingQuoteModeQuote quote = null;
    private static HistoricalData historicalData = null;
    private static TimeZone timeZone = TimeZone.getTimeZone("IST");
    private static String todaysDate = null;
    static DateFormat dtFmt = new SimpleDateFormat("yyyy-MM-dd");
    private static boolean streamingQuoteStarted = false;
    private static WebsocketThread websocketThread = null;

    private KiteConnect kiteconnect = new KiteConnect(StreamingConfig.API_KEY);
    KiteTicker tickerProvider = new KiteTicker(kiteconnect);

    private String url = kiteconnect.getLoginUrl();

    private TradeOperations tradeOperations = new TradeOperations();
    private static int a = 0, b = 0, g = 0, d = 0, e = 0, f = 0;
    List<Instrument> instrumentList = new ArrayList<Instrument>();
    boolean firstTimeDayHistoryRun = false;
    private String[] QUOTE_STREAMING_INSTRUMENTS_ARR = null;

    @Autowired
    ServletContext context;
    @Autowired
    HttpServletRequest request;
    @Autowired
    HttpServletResponse response;

    @RequestMapping(value = "/AuthRedirectWithToken", method = RequestMethod.GET)
    public String authRedirectWithTokenPost(@RequestParam String status,
            @RequestParam String request_token) {
        try {
            UserModel userModel = kiteconnect.requestAccessToken(request_token,
                    StreamingConfig.API_SECRET_KEY);
            kiteconnect.setAccessToken(userModel.accessToken);
            kiteconnect.setPublicToken(userModel.publicToken);
            startProcess();
        } catch (JSONException | KiteException e) {
            System.out.println(e.getMessage());
            return "error";
        }
        return "index";
    }

    @RequestMapping(value = "/start", method = { RequestMethod.POST, RequestMethod.GET })
    public RedirectView localRedirect() {

        kiteconnect.registerHook(new SessionExpiryHook() {
            @Override
            public void sessionExpired() {
                System.out.println("session expired");
            }
        });

        RedirectView redirectView = new RedirectView();
        redirectView.setUrl(url);
        return redirectView;
    }

    public void startProcess() {
        try {
            System.out.println("startProcess Entry");
            dtFmt.setTimeZone(timeZone);
            todaysDate = dtFmt.format(Calendar.getInstance(timeZone).getTime());
            StreamingConfig.HIST_DATA_END_DATE = todaysDate;

            createInitialDayTables();

            startStreamForHistoricalInstrumentsData();

            startLiveStreamOfSelectedInstruments();

            // applyStrategiesAndGenerateSignals();

            // executeOrdersOnSignals();

            // checkAndProcessPendingOrdersOnMarketQueue();

            // closingDayRoundOffOperations();
            System.out.println("startProcess Exit");
        } catch (JSONException e) {
            System.out.println(e.getMessage());
        }
    }

    private void checkAndProcessPendingOrdersOnMarketQueue() {
        // TODO Auto-generated method stub
    }

    private void createInitialDayTables() {
        System.out.println("Thread  for createInitialDayTables Entry");
        if (StreamingConfig.QUOTE_STREAMING_DB_STORE_REQD && (streamingQuoteStorage != null)) {

            DateFormat quoteTableDtFmt = new SimpleDateFormat("ddMMyyyy");
            quoteTableDtFmt.setTimeZone(timeZone);

            streamingQuoteStorage.initializeJDBCConn();
            streamingQuoteStorage.createDaysStreamingQuoteTable(
                    quoteTableDtFmt.format(Calendar.getInstance(timeZone).getTime()));
        }
    }

    private void closingDayRoundOffOperations() throws KiteException {
        System.out.println("Thread  for closingDayRoundOffOperations Entry");
        Thread t = new Thread(new Runnable() {
            private boolean runnable = true;
            private DateFormat dtFmt = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
            private TimeZone timeZone = TimeZone.getTimeZone("IST");
            private String quoteEndTime = todaysDate + " "
                    + StreamingConfig.QUOTE_STREAMING_END_TIME;
            List<Order> orderList = tradeOperations.getOrders(kiteconnect);
            List<Holding> holdings = tradeOperations.getHoldings(kiteconnect).holdings;

            @Override
            public void run() {
                System.out.println("Thread  for closingDayRoundOffOperations" + f++);
                dtFmt.setTimeZone(timeZone);
                try {
                    Date timeEnd = dtFmt.parse(quoteEndTime);
                    while (runnable) {
                        Date timeNow = Calendar.getInstance(timeZone).getTime();
                        if (timeNow.compareTo(timeEnd) >= 0) {
                            runnable = false;
                            for (int index = 0; index < orderList.size(); index++) {
                                if (orderList.get(index).status.equalsIgnoreCase("OPEN")) {
                                    tradeOperations.cancelOrder(kiteconnect, orderList.get(index));
                                }
                            }
                            for (int index = 0; index < holdings.size(); index++) {
                                tradeOperations.placeOrder(kiteconnect,
                                        holdings.get(index).instrumentToken, "SELL",
                                        holdings.get(index).quantity);
                            }
                        } else {
                            Thread.sleep(10 * seconds);
                            runnable = true;
                        }
                    }
                } catch (ParseException e) {
                    System.out.println(e.getMessage());
                } catch (InterruptedException e) {
                    System.out.println(e.getMessage());
                } catch (KiteException e) {
                    System.out.println(e.getMessage());
                }
            }
        });
        t.start();
    }

    private void executeOrdersOnSignals() {
        System.out.println("Thread  for executeOrdersOnSignals entry");
        Thread t = new Thread(new Runnable() {
            private boolean runnable = true;
            private DateFormat dtFmt = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
            private TimeZone timeZone = TimeZone.getTimeZone("IST");
            private String quoteStartTime = todaysDate + " "
                    + StreamingConfig.QUOTE_STREAMING_START_TIME;
            private String quoteEndTime = todaysDate + " "
                    + StreamingConfig.QUOTE_STREAMING_END_TIME;

            @Override
            public void run() {
                System.out.println("Thread  for executeOrdersOnSignals" + e++);
                dtFmt.setTimeZone(timeZone);
                try {
                    Date timeStart = dtFmt.parse(quoteStartTime);
                    Date timeEnd = dtFmt.parse(quoteEndTime);
                    while (runnable) {
                        Date timeNow = Calendar.getInstance(timeZone).getTime();
                        if (timeNow.compareTo(timeStart) >= 0 && timeNow.compareTo(timeEnd) <= 0) {
                            List<Order> signalList = getOrderListToPlaceAsPerStrategySignals();
                            for (int index = 0; index < signalList.size(); index++)
                                tradeOperations.placeOrder(kiteconnect,
                                        signalList.get(index).symbol,
                                        signalList.get(index).transactionType,
                                        signalList.get(index).quantity);
                            Thread.sleep(2 * seconds);
                        } else {
                            runnable = false;
                        }
                    }
                } catch (ParseException e) {
                    System.out.println(e.getMessage());
                } catch (InterruptedException e) {
                    System.out.println(e.getMessage());
                } catch (KiteException e) {
                    System.out.println(e.getMessage());
                }
            }
        });
        t.start();
    }

    private List<Order> getOrderListToPlaceAsPerStrategySignals() {
        System.out.println("Thread  for getOrderListToPlaceAsPerStrategySignals entry");
        return streamingQuoteStorage.getOrderListToPlace();
    }

    private void startLiveStreamOfSelectedInstruments() {
        System.out.println("Thread  for startLiveStreamOfSelectedInstruments entry");
        Thread t = new Thread(new Runnable() {
            private boolean runnable = true;
            private DateFormat dtFmt = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
            private TimeZone timeZone = TimeZone.getTimeZone("IST");
            private String quoteStartTime = todaysDate + " "
                    + StreamingConfig.QUOTE_STREAMING_START_TIME;
            private String quoteEndTime = todaysDate + " "
                    + StreamingConfig.QUOTE_STREAMING_END_TIME;

            @Override
            public void run() {
                System.out.println("Thread  for startLiveStreamOfSelectedInstruments" + d++);
                dtFmt.setTimeZone(timeZone);
                try {
                    Date timeStart = dtFmt.parse(quoteStartTime);
                    Date timeEnd = dtFmt.parse(quoteEndTime);
                    while (runnable) {
                        Date timeNow = Calendar.getInstance(timeZone).getTime();
                        if (timeNow.compareTo(timeStart) >= 0 && timeNow.compareTo(timeEnd) <= 0) {
                            startStreamingQuote(kiteconnect.getApiKey(), kiteconnect.getUserId(),
                                    kiteconnect.getPublicToken());
                            Thread.sleep(10 * seconds);
                        } else {
                            runnable = false;
                            stopStreamingQuote();
                        }
                    }
                } catch (ParseException e) {
                    System.out.println(e.getMessage());
                } catch (InterruptedException e) {
                    System.out.println(e.getMessage());
                }
            }
        });
        t.start();
    }

    private void startStreamForHistoricalInstrumentsData() {
        System.out.println("Thread  for startStreamForHistoricalInstrumentsData entry");

        Thread t = new Thread(new Runnable() {
            private boolean runnable = true;
            private DateFormat dtFmt = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
            private TimeZone timeZone = TimeZone.getTimeZone("IST");
            private String quoteStartTime = todaysDate + " "
                    + StreamingConfig.HISTORICAL_DATA_STREAM_START_TIME;
            private String quoteEndTime = todaysDate + " "
                    + StreamingConfig.QUOTE_STREAMING_END_TIME;
            private SimpleDateFormat fmt = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ssZ");

            @Override
            public void run() {
                System.out.println("Thread  for startStreamForHistoricalInstrumentsData" + a++);
                dtFmt.setTimeZone(timeZone);

                Date timeStart = null, timeEnd = null;
                try {
                    timeStart = dtFmt.parse(quoteStartTime);
                    timeEnd = dtFmt.parse(quoteEndTime);
                } catch (ParseException e) {

                    System.out.println(e.getMessage());
                }
                while (runnable) {
                    try {
                        Date timeNow = Calendar.getInstance(timeZone).getTime();
                        if (timeNow.compareTo(timeStart) >= 0 && timeNow.compareTo(timeEnd) <= 0) {

                            if (firstTimeDayHistoryRun) {
                                try {
                                    instrumentList = tradeOperations
                                            .getInstrumentsForExchange(kiteconnect, "BFO");
                                    streamingQuoteStorage.saveInstrumentDetails(instrumentList,
                                            new Date().toString());

                                    /*
                                     * instrumentList.addAll(tradeOperations
                                     * .getInstrumentsForExchange(kiteconnect, "BSE"));
                                     * instrumentList.addAll(tradeOperations
                                     * .getInstrumentsForExchange(kiteconnect, "NFO"));
                                     * instrumentList.addAll(tradeOperations
                                     * .getInstrumentsForExchange(kiteconnect, "BFO"));
                                     */
                                    firstTimeDayHistoryRun = false;
                                } catch (KiteException e) {

                                    System.out.println(e.message);
                                }
                            }
                            List<StreamingQuoteModeQuote> quoteList = null;

                            HistoricalData histData;
                            for (int index = 0; index < instrumentList.size(); index++) {
                                try {
                                    double priorityPoint = 0;
                                    quoteList = new ArrayList<StreamingQuoteModeQuote>();
                                    historicalData = tradeOperations.getHistoricalData(kiteconnect,
                                            StreamingConfig.HIST_DATA_START_DATE,
                                            StreamingConfig.HIST_DATA_END_DATE, "minute",
                                            Long.toString(instrumentList.get(index)
                                                    .getInstrument_token()));

                                    for (int count = 0; count < historicalData.dataArrayList
                                            .size(); count++) {
                                        histData = historicalData.dataArrayList.get(count);

                                        priorityPoint += (Double.valueOf(histData.volume) / 1000);

                                        quote = new StreamingQuoteModeQuote(
                                                dtFmt.format(fmt.parse(historicalData.dataArrayList
                                                        .get(count).timeStamp).getTime()),
                                                Long.toString(instrumentList.get(index)
                                                        .getInstrument_token()),
                                                new Double(0), new Long(0), new Double(0),
                                                Long.valueOf(histData.volume), new Long(0),
                                                new Long(0), new Double(histData.open),
                                                new Double(histData.high), new Double(histData.low),
                                                new Double(histData.close));
                                        quoteList.add(quote);
                                    }
                                    quoteList.get(0).ltp = priorityPoint;
                                    streamingQuoteStorage.storeData(quoteList, "minute");
                                } catch (ArithmeticException e) {
                                    if (e.getMessage().trim().equalsIgnoreCase("/ by zero"))
                                        continue;
                                    else {
                                        System.out.println(e.getMessage());
                                        continue;
                                    }
                                } catch (KiteException e) {
                                    if (e.message.equalsIgnoreCase(
                                            "No candles found based on token and time and candleType or Server busy")
                                            && e.code == 400)
                                        continue;
                                    else {
                                        System.out.println(e.message);
                                        continue;
                                    }
                                }
                            }
                            QUOTE_STREAMING_INSTRUMENTS_ARR = streamingQuoteStorage
                                    .getTopPrioritizedTokenList(tokenCountForTrade);

                            try {
                                roundOfNonPerformingBoughtStocks(QUOTE_STREAMING_INSTRUMENTS_ARR,
                                        tradeOperations.getOrders(kiteconnect), kiteconnect);
                            } catch (KiteException e) {
                                System.out.println(e.getMessage());
                            }
                            DateFormat dtFmt1 = new SimpleDateFormat("HH:mm:ss");
                            dtFmt1.setTimeZone(TimeZone.getTimeZone("IST"));
                            StreamingConfig.HIST_DATA_START_DATE = todaysDate
                                    + dtFmt1.format(new Date());
                            Thread.sleep(1800 * seconds);
                            StreamingConfig.HIST_DATA_END_DATE = todaysDate
                                    + dtFmt1.format(new Date());
                        } else {
                            runnable = false;
                        }
                    } catch (ParseException e) {
                        System.out.println(e.getMessage());
                    } catch (InterruptedException e) {
                        System.out.println(e.getMessage());
                    } catch (IOException e) {
                        System.out.println(e.getMessage());
                    }
                }

            }
        });
        t.start();
    }

    private void roundOfNonPerformingBoughtStocks(String[] qUOTE_STREAMING_INSTRUMENTS_ARR,
            List<Order> list, KiteConnect kiteconnect) throws KiteException {
        System.out.println("Thread  for roundOfNonPerformingBoughtStocks Entry");
        for (int index = 0; index < list.size(); index++) {
            for (int count = 0; count < qUOTE_STREAMING_INSTRUMENTS_ARR.length; count++) {
                if (list.get(index).tradingSymbol
                        .equalsIgnoreCase(qUOTE_STREAMING_INSTRUMENTS_ARR[count]))
                    if (list.get(index).status.equalsIgnoreCase("OPEN")) {
                        tradeOperations.cancelOrder(kiteconnect, list.get(index));
                    }
            }
        }
        List<Holding> holdings = tradeOperations.getHoldings(kiteconnect).holdings;

        for (int index = 0; index < holdings.size(); index++) {
            for (int count = 0; count < qUOTE_STREAMING_INSTRUMENTS_ARR.length; count++) {
                if (holdings.get(index).instrumentToken
                        .equalsIgnoreCase(qUOTE_STREAMING_INSTRUMENTS_ARR[count])) {
                    tradeOperations.placeOrder(kiteconnect, holdings.get(index).instrumentToken,
                            "SELL", holdings.get(index).quantity);
                }
            }
        }
    }

    private void applyStrategiesAndGenerateSignals() {
        System.out.println("Thread  for checkForSignalsFromStrategies Entry");
        Thread t = new Thread(new Runnable() {

            private boolean runnable = true;
            private DateFormat dtFmt = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
            private TimeZone timeZone = TimeZone.getTimeZone("IST");
            private String quoteStartTime = todaysDate + " "
                    + StreamingConfig.QUOTE_STREAMING_START_TIME;
            private String quoteEndTime = todaysDate + " "
                    + StreamingConfig.QUOTE_STREAMING_END_TIME;

            @Override
            public void run() {
                System.out.println("Thread  for checkForSignalsFromStrategies" + b++);
                dtFmt.setTimeZone(timeZone);
                try {
                    Date timeStart = dtFmt.parse(quoteStartTime);
                    Date timeEnd = dtFmt.parse(quoteEndTime);
                    while (runnable) {
                        Date timeNow = Calendar.getInstance(timeZone).getTime();
                        if (timeNow.compareTo(timeStart) >= 0 && timeNow.compareTo(timeEnd) <= 0) {
                            applySmaStragegy();
                            Thread.sleep(10 * seconds);
                        } else {
                            runnable = false;
                        }
                    }
                } catch (ParseException e) {
                    System.out.println(e.getMessage());
                } catch (InterruptedException e) {
                    System.out.println(e.getMessage());
                }
            }

        });
        t.start();
    }

    private void applySmaStragegy() {
        ArrayList<String> instrumentList = getInstrumentTokensList();
        Map<String, String> signalList = new HashMap<String, String>();
        List<StreamingQuoteModeQuote> quoteData = null;
        for (int i = 0; i < instrumentList.size(); i++) {
            quoteData = streamingQuoteStorage
                    .getProcessableQuoteDataOnTokenId(instrumentList.get(i).toString(), 26);
            double sma11 = 0, sma12 = 0, sma13 = 0, sma21 = 0, sma22 = 0, sma23 = 0, sma31 = 0,
                    sma32 = 0, sma33 = 0;
            for (int index = quoteData.size(); index > 0; index--) {
                if (quoteData.size() - index < 10) {
                    sma11 += quoteData.get(index - 1).closePrice;
                    sma12 += quoteData.get(index - 2).closePrice;
                    sma13 += quoteData.get(index - 3).closePrice;
                }
                if (quoteData.size() - index < 13) {
                    sma21 += quoteData.get(index - 1).closePrice;
                    sma22 += quoteData.get(index - 2).closePrice;
                    sma23 += quoteData.get(index - 3).closePrice;
                }
                if (quoteData.size() - index < 23) {
                    sma31 += quoteData.get(index - 1).closePrice;
                    sma32 += quoteData.get(index - 2).closePrice;
                    sma33 += quoteData.get(index - 3).closePrice;
                }

            }
            sma11 = sma11 / 9;
            sma21 = sma21 / 12;
            sma31 = sma31 / 22;
            sma12 = sma12 / 9;
            sma22 = sma22 / 12;
            sma32 = sma32 / 22;
            sma13 = sma13 / 9;
            sma23 = sma23 / 12;
            sma33 = sma33 / 22;

            if (sma11 <= sma12 && sma12 <= sma13 && sma21 <= sma22 && sma22 <= sma23
                    && sma31 <= sma32 && sma32 <= sma33) {
                signalList.put(instrumentList.get(i).toString(), "SELL");
            }
            if (sma11 > sma12 && sma12 > sma13 && sma21 > sma22 && sma22 > sma23 && sma31 > sma32
                    && sma32 > sma33) {
                signalList.put(instrumentList.get(i).toString(), "BUY");
            }
        }
        streamingQuoteStorage.saveGeneratedSignals(signalList, instrumentList);
        // try inplementing at data change trigger level, then only sma ema
        // value fetch and compare would be ok
    }

    private void startStreamingQuote(String apiKey, String userId, String publicToken) {
        String URIstring = StreamingConfig.STREAMING_QUOTE_WS_URL_TEMPLATE + "api_key=" + apiKey
                + "&user_id=" + userId + "&public_token=" + publicToken;
        System.out.println("Thread  for startStreamingQuote Entry");
        ArrayList<String> instrumentList = getInstrumentTokensList();
        if (null != instrumentList) {
            try {
                websocketThread = new WebsocketThread(URIstring, instrumentList,
                        streamingQuoteStorage);
                streamingQuoteStarted = websocketThread.startWS();
                if (streamingQuoteStarted) {
                    Thread t = new Thread(websocketThread);
                    t.start();
                } else {
                    System.out.println(
                            "ZStreamingQuoteControl.startStreamingQuote(): ERROR: WebSocket Streaming Quote not started !!!");
                }
            } catch (IOException | WebSocketException | KiteException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            } /*
               * websocketThread = new WebsocketThread(URIstring, instrumentList,
               * streamingQuoteStorage); streamingQuoteStarted = websocketThread.startWS(); if
               * (streamingQuoteStarted) { Thread t = new Thread(websocketThread); t.start(); } else
               * { System.out.println(
               * "ZStreamingQuoteControl.startStreamingQuote(): ERROR: WebSocket Streaming Quote not started !!!"
               * ); }
               */
        }

    }

    private ArrayList<String> getInstrumentTokensList() {
        System.out.println("Thread  for getInstrumentTokensList Entry");
        if (null != QUOTE_STREAMING_INSTRUMENTS_ARR) {
            String[] instrumentsArr = QUOTE_STREAMING_INSTRUMENTS_ARR;
            List<String> instrumentList = Arrays.asList(instrumentsArr);
            ArrayList<Long> instruments = new ArrayList<Long>();
            for (int i = 0; i < instrumentList.size(); i++)
                instruments.add(Long.parseLong(instrumentList.get(i)));
            return (ArrayList<String>) instrumentList;
        } else
            return null;
    }

    private void stopStreamingQuote() {
        System.out.println("Thread  for stopStreamingQuote Entry");
        if (streamingQuoteStarted && websocketThread != null) {
            websocketThread.stopWS();
            try {
                System.out.println("Thread  for stopStreamingQuote" + g++);
                Thread.sleep(2 * seconds);
            } catch (InterruptedException e) {
                System.out.println(e.getMessage());
            }
        }

        if (StreamingConfig.QUOTE_STREAMING_DB_STORE_REQD && (streamingQuoteStorage != null)) {
            streamingQuoteStorage.closeJDBCConn();
        }
    }

    public void tickerUsage(final ArrayList<Long> tokens)
            throws IOException, WebSocketException, KiteException {

        tickerProvider.setOnConnectedListener(new OnConnect() {
            @Override
            public void onConnected() {
                try {
                    tickerProvider.subscribe(tokens);
                } catch (IOException e) {
                    e.printStackTrace();
                } catch (WebSocketException e) {
                    e.printStackTrace();
                } catch (KiteException e) {
                    e.printStackTrace();
                }
            }
        });

        tickerProvider.setOnDisconnectedListener(new OnDisconnect() {
            @Override
            public void onDisconnected() {
                // your code goes here
            }
        });

        tickerProvider.setOnTickerArrivalListener(new OnTick() {
            @Override
            public void onTick(ArrayList<Tick> ticks) {
                System.out.println(ticks.size());
            }
        });

        tickerProvider.setTryReconnection(true);

        tickerProvider.setTimeIntervalForReconnection(5);

        tickerProvider.setMaxRetries(10);

        tickerProvider.connect();

        boolean isConnected = tickerProvider.isConnectionOpen();
        System.out.println(isConnected);

        tickerProvider.setMode(tokens, KiteTicker.modeLTP);

        tickerProvider.unsubscribe(tokens);

        tickerProvider.disconnect();
    }
}
