package com.streamquote.dao;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;
import java.util.logging.Logger;

import com.streamquote.model.OHLCquote;
import com.streamquote.model.StreamingQuote;
import com.streamquote.model.StreamingQuoteModeQuote;
import com.streamquote.utils.StreamingConfig;
import com.trade.optimizer.models.Instrument;
import com.trade.optimizer.models.Order;
import com.trade.optimizer.models.Tick;

public class StreamingQuoteStorageImpl implements StreamingQuoteStorage {
    private final static Logger LOGGER = Logger
            .getLogger(StreamingQuoteStorageImpl.class.getName());

    private static final String JDBC_DRIVER = "com.mysql.jdbc.Driver";
    private static final String DB_URL = StreamingConfig.QUOTE_STREAMING_DB_URL;

    private static final String USER = StreamingConfig.QUOTE_STREAMING_DB_USER;
    private static final String PASS = StreamingConfig.QUOTE_STREAMING_DB_PWD;
    private DateFormat dtTmFmt = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

    private Connection conn = null;

    private static String quoteTable = null;

    public StreamingQuoteStorageImpl() {
    }

    @Override
    public void initializeJDBCConn() {
        dtTmFmt.setTimeZone(TimeZone.getTimeZone("IST"));
        try {
            LOGGER.info(
                    "StreamingQuoteStorageImpl.initializeJDBCConn(): creating JDBC connection for Streaming Quote...");

            Class.forName(JDBC_DRIVER);

            conn = DriverManager.getConnection(DB_URL, USER, PASS);
        } catch (ClassNotFoundException e) {
            LOGGER.info("StreamingQuoteStorageImpl.initializeJDBCConn(): ClassNotFoundException: "
                    + JDBC_DRIVER);
        } catch (SQLException e) {
            LOGGER.info(
                    "StreamingQuoteStorageImpl.initializeJDBCConn(): SQLException on getConnection");
        }
    }

    @Override
    public void closeJDBCConn() {
        if (conn != null) {
            try {
                LOGGER.info(
                        "StreamingQuoteStorageImpl.closeJDBCConn(): Closing JDBC connection for Streaming Quote...");
                conn.close();
            } catch (SQLException e) {
                LOGGER.info(
                        "StreamingQuoteStorageImpl.closeJDBCConn(): SQLException on conn close");
            }
        } else {
            LOGGER.info(
                    "StreamingQuoteStorageImpl.closeJDBCConn(): WARNING: DB connection already null");
        }
    }

    @Override
    public void createDaysStreamingQuoteTable(String date) throws SQLException {
        if (conn != null) {
            Statement stmt = conn.createStatement();
            quoteTable = StreamingConfig.getStreamingQuoteTbNameAppendFormat(date);
            String sql = "";
            try {
                sql = "CREATE TABLE " + quoteTable + " " + "(time timestamp, "
                        + " InstrumentToken varchar(32) , " + " LastTradedPrice DECIMAL(20,4) , "
                        + " LastTradedQty BIGINT , " + " AvgTradedPrice DECIMAL(20,4) , "
                        + " Volume BIGINT , " + " BuyQty BIGINT , " + " SellQty BIGINT , "
                        + " OpenPrice DECIMAL(20,4) , " + " HighPrice DECIMAL(20,4) , "
                        + " LowPrice DECIMAL(20,4) , " + " ClosePrice DECIMAL(20,4) , "
                        + " PRIMARY KEY (InstrumentToken, time)) "
                        + " ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARSET=utf8;";
                stmt.executeUpdate(sql);
            } catch (SQLException e) {
                LOGGER.info(
                        "StreamingQuoteStorageImpl.createDaysStreamingQuoteTable(): ERROR: SQLException on creating Table, cause: "
                                + e.getMessage());
            }
            try {
                sql = "CREATE TABLE " + quoteTable + "_hist " + "(time timestamp, "
                        + " InstrumentToken varchar(32) , " + " LastTradedPrice DECIMAL(20,4) , "
                        + " LastTradedQty BIGINT , " + " AvgTradedPrice DECIMAL(20,4) , "
                        + " Volume BIGINT , " + " BuyQty BIGINT , " + " SellQty BIGINT , "
                        + " OpenPrice DECIMAL(20,4) , " + " HighPrice DECIMAL(20,4) , "
                        + " LowPrice DECIMAL(20,4) , "
                        + " ClosePrice DECIMAL(20,4) ,TickType varchar(32) ,  "
                        + " PRIMARY KEY (InstrumentToken, time)) "
                        + " ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARSET=utf8;";
                stmt.executeUpdate(sql);
            } catch (SQLException e) {
                LOGGER.info(
                        "StreamingQuoteStorageImpl.createDaysStreamingQuoteTable(): ERROR: SQLException on creating Table, cause: "
                                + e.getMessage());
            }
            try {
                sql = "CREATE TABLE " + quoteTable + "_priority " + "(time timestamp, "
                        + " InstrumentToken varchar(32) , " + " PriorityPoint DECIMAL(20,4) , "
                        + " PRIMARY KEY (InstrumentToken,time)) "
                        + " ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARSET=utf8;";
                stmt.executeUpdate(sql);
            } catch (SQLException e) {
                LOGGER.info(
                        "StreamingQuoteStorageImpl.createDaysStreamingQuoteTable(): ERROR: SQLException on creating Table, cause: "
                                + e.getMessage());
            }
            try {
                sql = "CREATE TABLE " + quoteTable + "_Signal " + "(time timestamp, "
                        + " InstrumentToken varchar(32) , " + " Quantity varchar(32) , "
                        + " ProcessSignal varchar(32) , " + " Status varchar(32) , "
                        + " PRIMARY KEY (time, InstrumentToken, ProcessSignal)) "
                        + " ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARSET=utf8;";
                stmt.executeUpdate(sql);
            } catch (SQLException e) {
                LOGGER.info(
                        "StreamingQuoteStorageImpl.createDaysStreamingQuoteTable(): ERROR: SQLException on creating Table, cause: "
                                + e.getMessage());
            }
            try {
                sql = "CREATE TABLE " + quoteTable + "_instrumentDetails " + "(time timestamp, "
                        + " instrumentToken varchar(32) ,exchangeToken varchar(32) ,"
                        + "tradingsymbol varchar(32) ,name varchar(32) ,"
                        + "last_price varchar(32) ,tickSize varchar(32) ,"
                        + "expiry varchar(32) ,instrumentType varchar(32) ,"
                        + "segment varchar(32) ,exchange varchar(32) ,"
                        + "strike varchar(32) ,lotSize varchar(32) ,"
                        + " PRIMARY KEY (time,instrumentToken)) "
                        + " ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARSET=utf8;";
                stmt.executeUpdate(sql);

            } catch (SQLException e) {
                LOGGER.info(
                        "StreamingQuoteStorageImpl.createDaysStreamingQuoteTable(): ERROR: SQLException on creating Table, cause: "
                                + e.getMessage());
            }
        } else {
            LOGGER.info(
                    "StreamingQuoteStorageImpl.createDaysStreamingQuoteTable(): ERROR: DB conn is null !!!");
        }
    }

    @Override
    public void storeData(List<StreamingQuoteModeQuote> quoteList, String tickType) {
        if (conn != null) {

            try {
                String sql = "INSERT INTO " + quoteTable + "_hist "
                        + "(Time, InstrumentToken, LastTradedPrice, LastTradedQty, AvgTradedPrice, "
                        + "Volume, BuyQty, SellQty, OpenPrice, HighPrice, LowPrice, ClosePrice,TickType) "
                        + "values(?,?,?,?,?,?,?,?,?,?,?,?,?)";
                PreparedStatement prepStmt = conn.prepareStatement(sql);
                for (int index = 0; index < quoteList.size(); index++) {
                    try {
                        StreamingQuoteModeQuote quoteModeQuote = (StreamingQuoteModeQuote) quoteList
                                .get(index);

                        prepStmt.setTimestamp(1,
                                new Timestamp(dtTmFmt.parse(quoteModeQuote.getTime()).getTime()));

                        prepStmt.setString(2, quoteModeQuote.getInstrumentToken());
                        prepStmt.setDouble(3, quoteModeQuote.getLtp());
                        prepStmt.setLong(4, quoteModeQuote.getLastTradedQty());
                        prepStmt.setDouble(5, quoteModeQuote.getAvgTradedPrice());
                        prepStmt.setLong(6, quoteModeQuote.getVol());
                        prepStmt.setLong(7, quoteModeQuote.getBuyQty());
                        prepStmt.setLong(8, quoteModeQuote.getSellQty());
                        prepStmt.setDouble(9, quoteModeQuote.getOpenPrice());
                        prepStmt.setDouble(10, quoteModeQuote.getHighPrice());
                        prepStmt.setDouble(11, quoteModeQuote.getLowPrice());
                        prepStmt.setDouble(12, quoteModeQuote.getClosePrice());
                        prepStmt.setString(13, tickType);
                        prepStmt.executeUpdate();
                    } catch (ParseException e) {
                        LOGGER.info(e.getMessage());
                    }
                }
                try {
                    sql = "INSERT INTO " + quoteTable + "_priority "
                            + "(Time, InstrumentToken, PriorityPoint) " + "values(?,?,?)";
                    prepStmt = conn.prepareStatement(sql);

                    prepStmt.setTimestamp(1,
                            new Timestamp(dtTmFmt.parse(quoteList.get(0).getTime()).getTime()));
                    prepStmt.setString(2, quoteList.get(0).getInstrumentToken());
                    prepStmt.setDouble(3, quoteList.get(0).ltp);
                    prepStmt.executeUpdate();
                } catch (ParseException e) {
                    LOGGER.info(e.getMessage());
                }
                prepStmt.close();
            } catch (SQLException e) {
                LOGGER.info(e.getMessage());
            }
        } else {
            if (conn != null) {
                LOGGER.info("StreamingQuoteStorageImpl.storeData(): ERROR: DB conn is null !!!");
            } else {
                LOGGER.info(
                        "StreamingQuoteStorageImpl.storeData(): ERROR: quote is not of type StreamingQuoteModeQuote !!!");
            }
        }
    }

    @Override
    public void storeData(StreamingQuote quote) {
        if (conn != null && quote instanceof StreamingQuoteModeQuote) {
            StreamingQuoteModeQuote quoteModeQuote = (StreamingQuoteModeQuote) quote;

            try {
                String sql = "INSERT INTO " + quoteTable + ""
                        + "(Time, InstrumentToken, LastTradedPrice, LastTradedQty, AvgTradedPrice, "
                        + "Volume, BuyQty, SellQty, OpenPrice, HighPrice, LowPrice, ClosePrice) "
                        + "values(?,?,?,?,?,?,?,?,?,?,?,?)";
                PreparedStatement prepStmt = conn.prepareStatement(sql);

                prepStmt.setString(1, quoteModeQuote.getTime());
                prepStmt.setString(2, quoteModeQuote.getInstrumentToken());
                prepStmt.setDouble(3, quoteModeQuote.getLtp());
                prepStmt.setLong(4, quoteModeQuote.getLastTradedQty());
                prepStmt.setDouble(5, quoteModeQuote.getAvgTradedPrice());
                prepStmt.setLong(6, quoteModeQuote.getVol());
                prepStmt.setLong(7, quoteModeQuote.getBuyQty());
                prepStmt.setLong(8, quoteModeQuote.getSellQty());
                prepStmt.setDouble(9, quoteModeQuote.getOpenPrice());
                prepStmt.setDouble(10, quoteModeQuote.getHighPrice());
                prepStmt.setDouble(11, quoteModeQuote.getLowPrice());
                prepStmt.setDouble(12, quoteModeQuote.getClosePrice());

                prepStmt.executeUpdate();
                prepStmt.close();
            } catch (SQLException e) {
                LOGGER.info(
                        "StreamingQuoteStorageImpl.storeData(): ERROR: SQLException on Storing data to Table: "
                                + quote);
                LOGGER.info("StreamingQuoteStorageImpl.storeData(): [SQLException Cause]: "
                        + e.getMessage());
            }
        } else {
            if (conn != null) {
                LOGGER.info("StreamingQuoteStorageImpl.storeData(): ERROR: DB conn is null !!!");
            } else {
                LOGGER.info(
                        "StreamingQuoteStorageImpl.storeData(): ERROR: quote is not of type StreamingQuoteModeQuote !!!");
            }
        }
    }

    @Override
    public void storeData(ArrayList<Tick> ticks) {
        if (conn != null) {

            try {
                String sql = "INSERT INTO " + quoteTable + ""
                        + "(Time, InstrumentToken, LastTradedPrice, LastTradedQty, AvgTradedPrice, "
                        + "Volume, BuyQty, SellQty, OpenPrice, HighPrice, LowPrice, ClosePrice) "
                        + "values(?,?,?,?,?,?,?,?,?,?,?,?)";
                PreparedStatement prepStmt = conn.prepareStatement(sql);
                for (int index = 0; index < ticks.size(); index++) {
                    Tick tick = ticks.get(index);

                    prepStmt.setTimestamp(1, new Timestamp(new Date().getTime()));
                    prepStmt.setString(2, String.valueOf(tick.getToken()));
                    prepStmt.setDouble(3, tick.getLastTradedPrice());
                    prepStmt.setLong(4, (long) tick.getLastTradedQuantity());
                    prepStmt.setDouble(5, tick.getAverageTradePrice());
                    prepStmt.setLong(6, (long) tick.getVolumeTradedToday());
                    prepStmt.setLong(7, (long) tick.getTotalBuyQuantity());
                    prepStmt.setLong(8, (long) tick.getTotalSellQuantity());
                    prepStmt.setDouble(9, tick.getOpenPrice());
                    prepStmt.setDouble(10, tick.getHighPrice());
                    prepStmt.setDouble(11, tick.getLowPrice());
                    prepStmt.setDouble(12, tick.getClosePrice());

                    prepStmt.executeUpdate();
                }                
                prepStmt.close();
                for (int index = 0; index < ticks.size(); index++) {
                    calculateAndStoreStrategySignalParameters(String.valueOf(ticks.get(index).getToken()), null, null);
                }
            } catch (SQLException e) {
                LOGGER.info(
                        "StreamingQuoteStorageImpl.storeData(): ERROR: SQLException on Storing data to Table: "
                                + ticks);
                LOGGER.info("StreamingQuoteStorageImpl.storeData(): [SQLException Cause]: "
                        + e.getMessage());
            }
        } else {
            if (conn != null) {
                LOGGER.info("StreamingQuoteStorageImpl.storeData(): ERROR: DB conn is null !!!");
            } else {
                LOGGER.info(
                        "StreamingQuoteStorageImpl.storeData(): ERROR: quote is not of type StreamingQuoteModeQuote !!!");
            }
        }
    }

    @Override
    public OHLCquote getOHLCDataByTimeRange(String instrumentToken, String prevTime,
            String currTime) {
        OHLCquote ohlcMap = null;

        if (conn != null) {
            try {
                Statement stmt = conn.createStatement();

                String openSql = "SELECT LastTradedPrice FROM " + quoteTable + " WHERE Time >= '"
                        + prevTime + "' AND Time <= '" + currTime + "' AND InstrumentToken = '"
                        + instrumentToken + "' ORDER BY Time ASC LIMIT 1";
                ResultSet openRs = stmt.executeQuery(openSql);
                openRs.next();
                Double openQuote = openRs.getDouble("LastTradedPrice");

                String highSql = "SELECT MAX(LastTradedPrice) FROM " + quoteTable
                        + " WHERE Time >= '" + prevTime + "' AND Time <= '" + currTime
                        + "' AND InstrumentToken = '" + instrumentToken + "'";
                ResultSet highRs = stmt.executeQuery(highSql);
                highRs.next();
                Double highQuote = highRs.getDouble(1);

                String lowSql = "SELECT MIN(LastTradedPrice) FROM " + quoteTable
                        + " WHERE Time >= '" + prevTime + "' AND Time <= '" + currTime
                        + "' AND InstrumentToken = '" + instrumentToken + "'";
                ResultSet lowRs = stmt.executeQuery(lowSql);
                lowRs.next();
                Double lowQuote = lowRs.getDouble(1);

                String closeSql = "SELECT LastTradedPrice FROM " + quoteTable + " WHERE Time >= '"
                        + prevTime + "' AND Time <= '" + currTime + "' AND InstrumentToken = '"
                        + instrumentToken + "' ORDER BY Time DESC LIMIT 1";
                ResultSet closeRs = stmt.executeQuery(closeSql);
                closeRs.next();
                Double closeQuote = closeRs.getDouble("LastTradedPrice");

                String volSql = "SELECT Volume FROM " + quoteTable + " WHERE Time >= '" + prevTime
                        + "' AND Time <= '" + currTime + "' AND InstrumentToken = '"
                        + instrumentToken + "' ORDER BY Time DESC LIMIT 1";
                ResultSet volRs = stmt.executeQuery(volSql);
                volRs.next();
                Long volQuote = volRs.getLong(1);

                ohlcMap = new OHLCquote(openQuote, highQuote, lowQuote, closeQuote, volQuote);

                stmt.close();
            } catch (SQLException e) {
                ohlcMap = null;
                LOGGER.info(
                        "StreamingQuoteStorageImpl.getOHLCDataByTimeRange(): ERROR: SQLException on fetching data from Table, cause: "
                                + e.getMessage());
            }
        } else {
            ohlcMap = null;
            LOGGER.info(
                    "StreamingQuoteStorageImpl.getOHLCDataByTimeRange(): ERROR: DB conn is null !!!");
        }

        return ohlcMap;
    }

    // @Override
    // public List<StreamingQuote> getQuoteListByTimeRange(String
    // instrumentToken, String prevTime, String currTime) {
    // List<StreamingQuote> streamingQuoteList = new
    // ArrayList<StreamingQuote>();
    //
    // if (conn != null) {
    // try {
    // Statement stmt = conn.createStatement();
    //
    // String openSql = "SELECT * FROM " + quoteTable + " WHERE Time >= '" +
    // prevTime + "' AND Time <= '"
    // + currTime + "' AND InstrumentToken = '" + instrumentToken + "'";
    // ResultSet openRs = stmt.executeQuery(openSql);
    // while (openRs.next()) {
    // String time = openRs.getString("Time");
    // String instrument_Token = openRs.getString("InstrumentToken");
    // Double lastTradedPrice = openRs.getDouble("LastTradedPrice");
    // Long lastTradedQty = openRs.getLong("LastTradedQty");
    // Double avgTradedPrice = openRs.getDouble("AvgTradedPrice");
    // Long volume = openRs.getLong("Volume");
    // Long buyQty = openRs.getLong("BuyQty");
    // Long sellQty = openRs.getLong("SellQty");
    // Double openPrice = openRs.getDouble("OpenPrice");
    // Double highPrice = openRs.getDouble("HighPrice");
    // Double lowPrice = openRs.getDouble("LowPrice");
    // Double closePrice = openRs.getDouble("ClosePrice");
    //
    // StreamingQuote streamingQuote = new StreamingQuoteModeQuote(time,
    // instrument_Token, lastTradedPrice,
    // lastTradedQty, avgTradedPrice, volume, buyQty, sellQty, openPrice,
    // highPrice, lowPrice,
    // closePrice);
    // streamingQuoteList.add(streamingQuote);
    // }
    //
    // stmt.close();
    // } catch (SQLException e) {
    // streamingQuoteList = null;
    // LOGGER.info(
    // "StreamingQuoteStorageImpl.getQuoteByTimeRange(): ERROR: SQLException on
    // fetching data from Table, cause: "
    // + e.getMessage());
    // }
    // } else {
    // streamingQuoteList = null;
    // LOGGER.info("StreamingQuoteStorageImpl.getQuoteByTimeRange():
    // ERROR: DB conn is null !!!");
    // }
    //
    // return streamingQuoteList;
    // }

    @Override
    public ArrayList<Long> getTopPrioritizedTokenList(int i) {
        ArrayList<Long> instrumentList = new ArrayList<Long>();
        if (conn != null) {
            try {
                Statement stmt = conn.createStatement();
                String openSql = "SELECT InstrumentToken FROM " + quoteTable
                        + "_priority ORDER BY Time,PriorityPoint DESC LIMIT " + i + "";
                ResultSet openRs = stmt.executeQuery(openSql);
                while (openRs.next()) {
                    instrumentList.add(Long.valueOf(openRs.getString(1)));
                }
            } catch (SQLException e) {
                LOGGER.info(
                        "StreamingQuoteStorageImpl.getTopPrioritizedTokenList(): ERROR: SQLException on fetching data from Table, cause: "
                                + e.getMessage());
            }
        } else {
            LOGGER.info(
                    "StreamingQuoteStorageImpl.getTopPrioritizedTokenList(): ERROR: DB conn is null !!!");
        }
        return instrumentList;
    }

    @Override
    public List<Order> getOrderListToPlace() {
        List<Order> orders = new ArrayList<Order>();
        if (conn != null) {
            try {
                Statement stmt = conn.createStatement();
                String openSql = "SELECT InstrumentToken,ProcessSignal,Quantity,Time FROM "
                        + quoteTable + "_Signal where status ='active'";
                ResultSet openRs = stmt.executeQuery(openSql);

                while (openRs.next()) {
                    Order order = new Order();
                    order.symbol = String.valueOf(openRs.getString(1));
                    order.transactionType = String.valueOf(openRs.getString(2));
                    order.quantity = String.valueOf(openRs.getString(3));

                    orders.add(order);

                    stmt = conn.createStatement();
                    openSql = "update " + quoteTable
                            + "_Signal set status ='orderPlaced' where InstrumentToken= "
                            + openRs.getString(1) + " and ProcessSignal= '" + openRs.getString(2)
                            + "' and Time= " + openRs.getTimestamp(4) + "";
                    stmt.executeUpdate(openSql);

                }
                stmt.close();
            } catch (SQLException e) {
                LOGGER.info(
                        "StreamingQuoteStorageImpl.getOrderListToPlace(): ERROR: SQLException on fetching data from Table, cause: "
                                + e.getMessage());
            }
        } else {
            LOGGER.info(
                    "StreamingQuoteStorageImpl.getOrderListToPlace(): ERROR: DB conn is null !!!");
        }
        return orders;
    }

    @Override
    public void saveInstrumentDetails(List<Instrument> instrumentList, Timestamp time) {

        if (conn != null) {

            try {
                String sql = "INSERT INTO " + quoteTable + "_instrumentDetails "
                        + "(time,instrumentToken,exchangeToken,tradingsymbol,name,last_price,"
                        + "tickSize,expiry,instrumentType,segment,exchange,strike,lotSize) "
                        + "values(?,?,?,?,?,?,?,?,?,?,?,?,?)";
                PreparedStatement prepStmt = conn.prepareStatement(sql);
                for (int index = 0; index < instrumentList.size(); index++) {
                    Instrument instrument = instrumentList.get(index);

                    prepStmt.setTimestamp(1, time);
                    prepStmt.setString(2, String.valueOf(instrument.getInstrument_token()));
                    prepStmt.setString(3, String.valueOf(instrument.getExchange_token()));
                    prepStmt.setString(4, instrument.getTradingsymbol());
                    prepStmt.setString(5, instrument.getName());
                    prepStmt.setString(6, String.valueOf(instrument.getLast_price()));
                    prepStmt.setString(7, String.valueOf(instrument.getTick_size()));
                    prepStmt.setString(8, instrument.getExpiry());
                    prepStmt.setString(9, instrument.getInstrument_type());
                    prepStmt.setString(10, instrument.getSegment());
                    prepStmt.setString(11, instrument.getExchange());
                    prepStmt.setString(12, instrument.getStrike());
                    prepStmt.setString(13, String.valueOf(instrument.getLot_size()));
                    prepStmt.executeUpdate();
                }
                prepStmt.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        } else {
            if (conn != null) {
                LOGGER.info(
                        "StreamingQuoteStorageImpl.saveInstrumentDetails(): ERROR: DB conn is null !!!");
            } else {
                LOGGER.info(
                        "StreamingQuoteStorageImpl.saveInstrumentDetails(): ERROR: quote is not of type StreamingQuoteModeQuote !!!");
            }
        }

    }

    @Override
    public String[] getInstrumentDetailsOnTokenId(String instrumentToken) {
        String[] param = new String[3];
        if (conn != null) {
            try {
                Statement stmt = conn.createStatement();

                String openSql = "SELECT lotSize,tradingsymbol,exchange FROM " + quoteTable
                        + "_instrumentDetails WHERE instrumentToken='" + instrumentToken + "'";
                ResultSet openRs = stmt.executeQuery(openSql);
                while (openRs.next()) {
                    param[0] = openRs.getString(1);
                    param[1] = openRs.getString(2);
                    param[2] = openRs.getString(3);
                }
                stmt.close();
            } catch (SQLException e) {
                LOGGER.info(
                        "StreamingQuoteStorageImpl.getInstrumentDetailsOnTokenId(): ERROR: SQLException on fetching data from Table, cause: "
                                + e.getMessage());
            }
        } else {
            LOGGER.info(
                    "StreamingQuoteStorageImpl.getInstrumentDetailsOnTokenId(): ERROR: DB conn is null !!!");
        }

        return param;
    }

    @Override
    public List<StreamingQuoteModeQuote> getProcessableQuoteDataOnTokenId(String instrumentToken,
            int count) {
        List<StreamingQuoteModeQuote> streamingQuoteList = new ArrayList<StreamingQuoteModeQuote>();
        if (conn != null) {
            try {
                Statement stmt = conn.createStatement();

                String openSql = "SELECT * FROM " + quoteTable + " WHERE InstrumentToken = '"
                        + instrumentToken + "' order by Time desc limit " + count;
                ResultSet openRs = stmt.executeQuery(openSql);
                while (openRs.next()) {
                    Timestamp time = openRs.getTimestamp("Time");
                    String instrument_Token = openRs.getString("InstrumentToken");
                    Double lastTradedPrice = openRs.getDouble("LastTradedPrice");
                    Long lastTradedQty = openRs.getLong("LastTradedQty");
                    Double avgTradedPrice = openRs.getDouble("AvgTradedPrice");
                    Long volume = openRs.getLong("Volume");
                    Long buyQty = openRs.getLong("BuyQty");
                    Long sellQty = openRs.getLong("SellQty");
                    Double openPrice = openRs.getDouble("OpenPrice");
                    Double highPrice = openRs.getDouble("HighPrice");
                    Double lowPrice = openRs.getDouble("LowPrice");
                    Double closePrice = openRs.getDouble("ClosePrice");

                    StreamingQuoteModeQuote streamingQuote = new StreamingQuoteModeQuote(
                            time.toString(), instrument_Token, lastTradedPrice, lastTradedQty,
                            avgTradedPrice, volume, buyQty, sellQty, openPrice, highPrice, lowPrice,
                            closePrice);
                    streamingQuoteList.add(streamingQuote);
                }

                stmt.close();
            } catch (SQLException e) {
                streamingQuoteList = null;
                LOGGER.info(
                        "StreamingQuoteStorageImpl.getProcessableQuoteDataOnTokenId(): ERROR: SQLException on fetching data from Table, cause: "
                                + e.getMessage());
            }
        } else {
            streamingQuoteList = null;
            LOGGER.info(
                    "StreamingQuoteStorageImpl.getProcessableQuoteDataOnTokenId(): ERROR: DB conn is null !!!");
        }

        return streamingQuoteList;

    }

    @Override
    public void saveGeneratedSignals(Map<Long, String> signalList, List<Long> instrumentList) {

        if (conn != null) {

            try {
                String sql = "INSERT INTO " + quoteTable + "_signal "
                        + "(time,instrumentToken,quantity,processSignal,status) "
                        + "values(?,?,?,?,?)";
                PreparedStatement prepStmt = conn.prepareStatement(sql);
                for (int index = 0; index < instrumentList.size(); index++) {

                    if (updateOldSignalInSignalTable(instrumentList.get(index).toString(),
                            signalList.get(instrumentList.get(index)))) {
                        prepStmt.setTimestamp(1, new Timestamp(new Date().getTime()));
                        prepStmt.setString(2, instrumentList.get(index).toString());
                        prepStmt.setString(3, "0");
                        prepStmt.setString(4, signalList.get(instrumentList.get(index)));
                        prepStmt.setString(5, "active");
                        prepStmt.executeUpdate();
                    }
                }
                prepStmt.close();
            } catch (SQLException e) {
                LOGGER.info(e.getMessage());
            }
        } else {
            if (conn != null) {
                LOGGER.info(
                        "StreamingQuoteStorageImpl.saveGeneratedSignals(): ERROR: DB conn is null !!!");
            } else {
                LOGGER.info(
                        "StreamingQuoteStorageImpl.saveGeneratedSignals(): ERROR: quote is not of type StreamingQuoteModeQuote !!!");
            }
        }

    }

    private boolean updateOldSignalInSignalTable(String instrument, String processSignal) {
        if (conn != null) {
            try {
                Statement stmt = conn.createStatement();
                String openSql = "SELECT Time FROM " + quoteTable
                        + "_Signal where status ='active' and InstrumentToken= '" + instrument
                        + "' and processSignal='" + processSignal + "'";
                ResultSet openRs = stmt.executeQuery(openSql);
                if (openRs.first())
                    return false;

                openSql = "SELECT Time FROM " + quoteTable
                        + "_Signal where status ='active' and InstrumentToken= '" + instrument
                        + "'";
                openRs = stmt.executeQuery(openSql);

                while (openRs.next()) {
                    stmt = conn.createStatement();
                    openSql = "update " + quoteTable
                            + "_Signal set status ='timeOut' where InstrumentToken= '" + instrument
                            + "' and Time=" + openRs.getTimestamp(1) + "";
                    stmt.executeUpdate(openSql);
                    openRs.next();
                }
                stmt.close();
            } catch (SQLException e) {
                LOGGER.info(
                        "StreamingQuoteStorageImpl.updateOldSignalInSignalTable(): ERROR: SQLException on fetching data from Table, cause: "
                                + e.getMessage());
            }
        } else

        {
            LOGGER.info(
                    "StreamingQuoteStorageImpl.updateOldSignalInSignalTable(): ERROR: DB conn is null !!!");
        }
        return true;
    }
   
    private void calculateAndStoreStrategySignalParameters(String instrumentToken, String startTime, String endTime) {
        List<Order> orders = new ArrayList<Order>();
        if (conn != null) {
            try {
                Statement stmt = conn.createStatement();
                String openSql = "SELECT * FROM "
                        + quoteTable + "_SignalParams where InstrumentToken ='"+instrumentToken+"' ORDER BY Time DESC LIMIT 26 ";
                ResultSet openRs = stmt.executeQuery(openSql);

                openSql = "SELECT * FROM "
                        + quoteTable + " where InstrumentToken ='"+instrumentToken+"' and time between "+startTime+" and "+endTime+" ORDER BY Time DESC ";
                ResultSet openRs2 = stmt.executeQuery(openSql);
                
                while (openRs.next()) {
                    Order order = new Order();
                    order.symbol = String.valueOf(openRs.getString(1));
                    order.transactionType = String.valueOf(openRs.getString(2));
                    order.quantity = String.valueOf(openRs.getString(3));

                    orders.add(order);

                    stmt = conn.createStatement();
                    openSql = "update " + quoteTable
                            + "_Signal set status ='orderPlaced' where InstrumentToken= "
                            + openRs.getString(1) + " and ProcessSignal= '" + openRs.getString(2)
                            + "' and Time= " + openRs.getTimestamp(4) + "";
                    stmt.executeUpdate(openSql);

                }
                stmt.close();
            } catch (SQLException e) {
                LOGGER.info(
                        "StreamingQuoteStorageImpl.calculateAndStoreStrategySignalParameters(): ERROR: SQLException on fetching data from Table, cause: "
                                + e.getMessage());
            }
        } else {
            LOGGER.info(
                    "StreamingQuoteStorageImpl.calculateAndStoreStrategySignalParameters(): ERROR: DB conn is null !!!");
        }
        }

}
