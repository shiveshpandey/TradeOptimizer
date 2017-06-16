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

    private boolean liveStreamFirstRun = true;
    boolean firstTimeDayHistoryRun = false;

    private int tokenCountForTrade = 10;
    private int seconds = 1000;
    private int a = 0, b = 0, d = 0, e = 0, f = 0;

    private StreamingQuoteStorage streamingQuoteStorage = new StreamingQuoteStorageImpl();
    private KiteConnect kiteconnect = new KiteConnect(StreamingConfig.API_KEY);
    private TradeOperations tradeOperations = new TradeOperations();
    private List<Instrument> instrumentList = new ArrayList<Instrument>();
    private String[] quoteStreamingInstrumentsArr = null;
    private ArrayList<Long> tokenListForTick = null;
    private KiteTicker tickerProvider;
    private StreamingQuoteModeQuote quote = null;
    private HistoricalData historicalData = null;

    private String todaysDate, quoteStartTime, quoteEndTime, histQuoteStartTime, histQuoteEndTime;
    private String url = kiteconnect.getLoginUrl();

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

            tmFmt.setTimeZone(timeZone);
            dtFmt.setTimeZone(timeZone);
            dtTmFmt.setTimeZone(timeZone);
            dtTmZFmt.setTimeZone(timeZone);

            todaysDate = dtFmt.format(Calendar.getInstance(timeZone).getTime());
            StreamingConfig.HIST_DATA_END_DATE = todaysDate;
            quoteStartTime = todaysDate + " " + StreamingConfig.QUOTE_STREAMING_START_TIME;
            quoteEndTime = todaysDate + " " + StreamingConfig.QUOTE_STREAMING_END_TIME;
            histQuoteStartTime = todaysDate + " "
                    + StreamingConfig.HISTORICAL_DATA_STREAM_START_TIME;
            histQuoteEndTime = todaysDate + " " + StreamingConfig.HISTORICAL_DATA_STREAM_END_TIME;
            timeStart = dtTmFmt.parse(quoteStartTime);
            timeEnd = dtTmFmt.parse(quoteEndTime);
            histTimeStart = dtTmFmt.parse(histQuoteStartTime);
            histTimeEnd = dtTmFmt.parse(histQuoteEndTime);

            createInitialDayTables();

            startStreamForHistoricalInstrumentsData();

            tickerSettingInitialization();

            startLiveStreamOfSelectedInstruments();

            applyStrategiesAndGenerateSignals();

            executeOrdersOnSignals();

            // checkAndProcessPendingOrdersOnMarketQueue();

            closingDayRoundOffOperations();
            System.out.println("startProcess Exit");
        } catch (JSONException | ParseException | KiteException e) {
            System.out.println(e.getMessage());
        }
    }

    private void tickerSettingInitialization() {
        tickerProvider = new KiteTicker(kiteconnect);
        tickerProvider.setTryReconnection(true);
        try {
            tickerProvider.setTimeIntervalForReconnection(5);
        } catch (KiteException e1) {
            e1.printStackTrace();
        }
        tickerProvider.setMaxRetries(10);
        // if (null != tokenListForTick && !tokenListForTick.isEmpty())
        // tickerProvider.setMode(tokenListForTick, KiteTicker.modeLTP);

        tickerProvider.setOnConnectedListener(new OnConnect() {
            @Override
            public void onConnected() {
                try {
                    tickerProvider.subscribe(tokenListForTick);
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
                try {
                    tickerProvider.connect();
                } catch (WebSocketException | IOException e) {
                    e.printStackTrace();
                }
            }
        });

        tickerProvider.setOnTickerArrivalListener(new OnTick() {
            @Override
            public void onTick(ArrayList<Tick> ticks) {
                if (ticks.size() > 0)
                    streamingQuoteStorage.storeData(ticks);
            }
        });
    }

    private void createInitialDayTables() {
        System.out.println("createInitialDayTables Entry");
        if (streamingQuoteStorage != null) {

            DateFormat quoteTableDtFmt = new SimpleDateFormat("ddMMyyyy");
            quoteTableDtFmt.setTimeZone(timeZone);

            streamingQuoteStorage.initializeJDBCConn();
            streamingQuoteStorage.createDaysStreamingQuoteTable(
                    quoteTableDtFmt.format(Calendar.getInstance(timeZone).getTime()));
        }
        System.out.println("createInitialDayTables Exit");
    }

    private void closingDayRoundOffOperations() throws KiteException {
        System.out.println("closingDayRoundOffOperations Entry");
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
                            List<Holding> holdings = tradeOperations
                                    .getHoldings(kiteconnect).holdings;

                            for (int index = 0; index < orderList.size(); index++) {
                                if (orderList.get(index).status.equalsIgnoreCase("OPEN")) {
                                    tradeOperations.cancelOrder(kiteconnect, orderList.get(index));
                                }
                            }
                            for (int index = 0; index < holdings.size(); index++) {
                                tradeOperations.placeOrder(kiteconnect,
                                        holdings.get(index).instrumentToken, "SELL",
                                        holdings.get(index).quantity, streamingQuoteStorage);
                            }
                        } else {
                            System.out.println("in closingDayRoundOffOperations" + f++);
                            Thread.sleep(10 * seconds);
                            runnable = true;
                        }
                    }
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
        System.out.println("executeOrdersOnSignals entry");
        Thread t = new Thread(new Runnable() {
            private boolean runnable = true;

            @Override
            public void run() {
                try {
                    while (runnable) {
                        Date timeNow = Calendar.getInstance(timeZone).getTime();
                        if (timeNow.compareTo(timeStart) >= 0 && timeNow.compareTo(timeEnd) <= 0) {
                            System.out.println("in executeOrdersOnSignals" + e++);
                            List<Order> signalList = getOrderListToPlaceAsPerStrategySignals();
                            for (int index = 0; index < signalList.size(); index++)
                                tradeOperations.placeOrder(kiteconnect,
                                        signalList.get(index).symbol,
                                        signalList.get(index).transactionType,
                                        signalList.get(index).quantity, streamingQuoteStorage);
                            Thread.sleep(2 * seconds);
                        } else {
                            runnable = false;
                        }
                    }
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
        System.out.println("getOrderListToPlaceAsPerStrategySignals entry");
        return streamingQuoteStorage.getOrderListToPlace();
    }

    private void startLiveStreamOfSelectedInstruments() {
        System.out.println("startLiveStreamOfSelectedInstruments entry");
        Thread t = new Thread(new Runnable() {
            private boolean runnable = true;

            @Override
            public void run() {
                try {
                    while (runnable) {
                        Date timeNow = Calendar.getInstance(timeZone).getTime();
                        if (timeNow.compareTo(timeStart) >= 0 && timeNow.compareTo(timeEnd) <= 0) {
                            System.out.println("in startLiveStreamOfSelectedInstruments" + d++);

                            startStreamingQuote();
                            Thread.sleep(10 * seconds);
                        } else {
                            runnable = false;
                            stopStreamingQuote();
                        }
                    }
                } catch (InterruptedException e) {
                    System.out.println(e.getMessage());
                }
            }
        });
        t.start();
    }

    private void threadEnabledHistoricalDataFetch(List<Instrument> instrumentList) {
        for (int index = 0; index < instrumentList.size(); index++) {
            try {
                Instrument instrument = instrumentList.get(index);
                double priorityPoint = 0;
                ArrayList<StreamingQuoteModeQuote> quoteList = new ArrayList<StreamingQuoteModeQuote>();
                historicalData = tradeOperations.getHistoricalData(kiteconnect,
                        StreamingConfig.HIST_DATA_START_DATE, StreamingConfig.HIST_DATA_END_DATE,
                        "minute", Long.toString(instrument.getInstrument_token()));

                for (int count = 0; count < historicalData.dataArrayList.size(); count++) {

                    HistoricalData histData = historicalData.dataArrayList.get(count);
                    priorityPoint += (Double.valueOf(histData.volume) / 1000);

                    quote = new StreamingQuoteModeQuote(
                            dtTmFmt.format(
                                    dtTmFmt.parse(historicalData.dataArrayList.get(count).timeStamp)
                                            .getTime()),
                            Long.toString(instrument.getInstrument_token()), new Double(0),
                            new Long(0), new Double(0), Long.valueOf(histData.volume), new Long(0),
                            new Long(0), new Double(histData.open), new Double(histData.high),
                            new Double(histData.low), new Double(histData.close));
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
            } catch (ParseException e) {

                System.out.println(e.getMessage());
                continue;
            }
        }
    }

    private void startStreamForHistoricalInstrumentsData() {
        System.out.println("startStreamForHistoricalInstrumentsData entry");

        Thread t = new Thread(new Runnable() {
            private boolean runnable = true;

            @Override
            public void run() {
                while (runnable) {
                    try {
                        Date timeNow = Calendar.getInstance(timeZone).getTime();
                        if (timeNow.compareTo(histTimeStart) >= 0
                                && timeNow.compareTo(histTimeEnd) <= 0) {
                            System.out.println("in startStreamForHistoricalInstrumentsData" + a++);

                            if (firstTimeDayHistoryRun) {
                                try {
                                    instrumentList = tradeOperations
                                            .getInstrumentsForExchange(kiteconnect, "NSE");
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

                            /*
                             * int size = instrumentList.size() / 2;
                             * 
                             * for (int k = 0; k < 2; k++) { System.out.println(new
                             * Date().toString() + ">>>>>>>>>>>>>>>>>>>>>"); final List<Instrument>
                             * instrument = instrumentList.subList(size*k, size*(k+1)-1);
                             * System.out.println(new Date().toString() + ">>>outer loop>>> " + k +
                             * " " + instrument.size());
                             * 
                             * System.out.println(new Date().toString() + ">>>InThread>>>" +
                             * instrument.size()); Thread t = new Thread(new Runnable() {
                             * 
                             * @Override public void run() {
                             * 
                             * }
                             * 
                             * }); t.start();
                             */
                            List<Instrument> instrument = instrumentList;

                            threadEnabledHistoricalDataFetch(instrument);

                            quoteStreamingInstrumentsArr = streamingQuoteStorage
                                    .getTopPrioritizedTokenList(tokenCountForTrade);
                            try {
                                roundOfNonPerformingBoughtStocks(quoteStreamingInstrumentsArr,
                                        tradeOperations.getOrders(kiteconnect), kiteconnect);
                            } catch (KiteException e) {
                                System.out.println(e.getMessage());
                            }
                            StreamingConfig.HIST_DATA_START_DATE = todaysDate
                                    + tmFmt.format(new Date());
                            Thread.sleep(1800 * seconds);
                            StreamingConfig.HIST_DATA_END_DATE = todaysDate
                                    + tmFmt.format(new Date());
                        } else {
                            runnable = false;
                        }
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

    private void roundOfNonPerformingBoughtStocks(String[] quoteStreamingInstrumentsArr,
            List<Order> list, KiteConnect kiteconnect) throws KiteException {
        System.out.println("roundOfNonPerformingBoughtStocks Entry");
        for (int index = 0; index < list.size(); index++) {
            for (int count = 0; count < quoteStreamingInstrumentsArr.length; count++) {
                if (list.get(index).tradingSymbol
                        .equalsIgnoreCase(quoteStreamingInstrumentsArr[count])) {
                    if (list.get(index).status.equalsIgnoreCase("OPEN")) {
                        tradeOperations.cancelOrder(kiteconnect, list.get(index));
                    }
                }
            }
        }
        List<Holding> holdings = tradeOperations.getHoldings(kiteconnect).holdings;

        for (int index = 0; index < holdings.size(); index++) {
            for (int count = 0; count < quoteStreamingInstrumentsArr.length; count++) {
                if (holdings.get(index).instrumentToken
                        .equalsIgnoreCase(quoteStreamingInstrumentsArr[count])) {
                    tradeOperations.placeOrder(kiteconnect, holdings.get(index).instrumentToken,
                            "SELL", holdings.get(index).quantity, streamingQuoteStorage);
                }
            }
        }
    }

    private void applyStrategiesAndGenerateSignals() {
        System.out.println("checkForSignalsFromStrategies Entry");
        Thread t = new Thread(new Runnable() {
            private boolean runnable = true;

            @Override
            public void run() {
                try {
                    while (runnable) {
                        Date timeNow = Calendar.getInstance(timeZone).getTime();
                        if (timeNow.compareTo(timeStart) >= 0 && timeNow.compareTo(timeEnd) <= 0) {
                            System.out.println("in checkForSignalsFromStrategies" + b++);
                            applySmaStragegy();
                            Thread.sleep(10 * seconds);
                        } else {
                            runnable = false;
                        }
                    }
                } catch (InterruptedException e) {
                    System.out.println(e.getMessage());
                }
            }
        });
        t.start();
    }

    private void applySmaStragegy() {
        System.out.println("applySmaStragegy entry");

        ArrayList<Long> instrumentList = getInstrumentTokensList();
        if (null != instrumentList && instrumentList.size() > 0) {
            Map<Long, String> signalList = new HashMap<Long, String>();
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
                    signalList.put(instrumentList.get(i), "SELL");
                }
                if (sma11 > sma12 && sma12 > sma13 && sma21 > sma22 && sma22 > sma23
                        && sma31 > sma32 && sma32 > sma33) {
                    signalList.put(instrumentList.get(i), "BUY");
                }
            }
            streamingQuoteStorage.saveGeneratedSignals(signalList, instrumentList);
        }
        // try inplementing at data change trigger level, then only sma ema
        // value fetch and compare would be ok
    }

    private void startStreamingQuote() {
        System.out.println("startStreamingQuote Entry");
        tokenListForTick = getInstrumentTokensList();
        if (null != tokenListForTick && tokenListForTick.size() > 0) {
            try {
                if (liveStreamFirstRun) {
                    tickerSettingInitialization();
                    liveStreamFirstRun = false;
                }
                tickerProvider.connect();

            } catch (IOException | WebSocketException e) {
                e.printStackTrace();
            }
        }
    }

    private ArrayList<Long> getInstrumentTokensList() {
        System.out.println("getInstrumentTokensList Entry");
        if (null != quoteStreamingInstrumentsArr) {
            List<String> instrumentList = Arrays.asList(quoteStreamingInstrumentsArr);
            ArrayList<Long> instruments = new ArrayList<Long>();
            for (int i = 0; i < instrumentList.size(); i++)
                instruments.add(Long.parseLong(instrumentList.get(i)));
            return instruments;
        } else
            return new ArrayList<Long>();
    }

    private void stopStreamingQuote() {
        System.out.println("stopStreamingQuote Entry");
        tickerProvider.disconnect();

        if (streamingQuoteStorage != null) {
            streamingQuoteStorage.closeJDBCConn();
        }
    }
}
