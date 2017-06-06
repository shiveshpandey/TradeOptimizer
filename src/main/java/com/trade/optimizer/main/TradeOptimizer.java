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

    private static StreamingQuoteStorage streamingQuoteStorage = new StreamingQuoteStorageImpl();
    private static StreamingQuoteModeQuote quote = null;
    private static HistoricalData historicalData = null;
    private static TimeZone timeZone = TimeZone.getTimeZone("IST");
    private static String todaysDate = null;
    static DateFormat dtFmt = new SimpleDateFormat("yyyy-MM-dd");
    private static boolean streamingQuoteStarted = false;
    private static WebsocketThread websocketThread = null;

    private KiteConnect kiteconnect = new KiteConnect(StreamingConfig.API_KEY);

    private String url = kiteconnect.getLoginUrl();

    private TradeOperations tradeOperations = new TradeOperations();
    private static int a = 0, b = 0, g = 0, d = 0, e = 0, f = 0;
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

            createInitialDayTables();

            startStreamForHistoricalInstrumentsData();

            // startLiveStreamOfSelectedInstruments();

            // checkForSignalsFromStrategies();

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
                            Thread.sleep(1000);
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
                            Thread.sleep(1000);
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
                            Thread.sleep(1000);
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

                            List<Instrument> instrumentList = null;
                            try {
                                instrumentList = tradeOperations
                                        .getInstrumentsForExchange(kiteconnect, "NSE");

                                /*
                                 * instrumentList.addAll(tradeOperations
                                 * .getInstrumentsForExchange(kiteconnect, "BSE"));
                                 * instrumentList.addAll(tradeOperations
                                 * .getInstrumentsForExchange(kiteconnect, "NFO"));
                                 * instrumentList.addAll(tradeOperations
                                 * .getInstrumentsForExchange(kiteconnect, "BFO"));
                                 */
                            } catch (KiteException e) {

                                System.out.println(e.message);
                            }
                            List<StreamingQuoteModeQuote> quoteList = null;

                            HistoricalData histData;
                            for (int index = 0; index < instrumentList.size(); index++) {
                                try {
                                    long dayHighVolume = 0, dayLowVolume = 1999999999,
                                            dayCloseVolume = 0;
                                    double dayHighPrice = 0, dayLowPrice = 1999999999,
                                            dayClosePrice = 0;
                                    quoteList = new ArrayList<StreamingQuoteModeQuote>();
                                    historicalData = tradeOperations.getHistoricalData(kiteconnect,
                                            StreamingConfig.HIST_DATA_START_DATE,
                                            StreamingConfig.HIST_DATA_END_DATE, "minute",
                                            Long.toString(instrumentList.get(index)
                                                    .getInstrument_token()));

                                    for (int count = 0; count < historicalData.dataArrayList
                                            .size(); count++) {
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
                                    double priorityPoint = 0;
                                    if (dayHighPrice - dayLowPrice > 0
                                            && dayHighVolume - dayLowVolume > 0)
                                        priorityPoint = (dayClosePrice
                                                / (dayHighPrice - dayLowPrice)) * 100
                                                * (dayCloseVolume / (dayHighVolume - dayLowVolume));
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
                            streamingQuoteStorage.saveInstrumentDetails(instrumentList,
                                    new Date().toString());

                            StreamingConfig.QUOTE_STREAMING_INSTRUMENTS_ARR = streamingQuoteStorage
                                    .getTopPrioritizedTokenList(10);

                            try {
                                roundOfNonPerformingBoughtStocks(
                                        StreamingConfig.QUOTE_STREAMING_INSTRUMENTS_ARR,
                                        tradeOperations.getOrders(kiteconnect), kiteconnect);
                            } catch (KiteException e) {
                                System.out.println(e.getMessage());
                            }

                            Thread.sleep(3600);
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

    private void checkForSignalsFromStrategies() {
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
                            Thread.sleep(1000);
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

    private void startStreamingQuote(String apiKey, String userId, String publicToken) {
        String URIstring = StreamingConfig.STREAMING_QUOTE_WS_URL_TEMPLATE + "api_key=" + apiKey
                + "&user_id=" + userId + "&public_token=" + publicToken;
        System.out.println("Thread  for startStreamingQuote Entry");
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
        System.out.println("Thread  for getInstrumentTokensList Entry");
        String[] instrumentsArr = StreamingConfig.QUOTE_STREAMING_INSTRUMENTS_ARR;
        List<String> instrumentList = Arrays.asList(instrumentsArr);
        return instrumentList;
    }

    private void stopStreamingQuote() {
        System.out.println("Thread  for stopStreamingQuote Entry");
        if (streamingQuoteStarted && websocketThread != null) {
            websocketThread.stopWS();
            try {
                System.out.println("Thread  for stopStreamingQuote" + g++);
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                System.out.println(e.getMessage());
            }
        }

        if (StreamingConfig.QUOTE_STREAMING_DB_STORE_REQD && (streamingQuoteStorage != null)) {
            streamingQuoteStorage.closeJDBCConn();
        }
    }
}
