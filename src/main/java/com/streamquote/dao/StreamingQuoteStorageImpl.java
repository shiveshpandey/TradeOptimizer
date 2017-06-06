package com.streamquote.dao;

import java.sql.Connection;
import java.sql.Date;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Time;
import java.util.ArrayList;
import java.util.List;

import com.streamquote.model.OHLCquote;
import com.streamquote.model.StreamingQuote;
import com.streamquote.model.StreamingQuoteModeQuote;
import com.streamquote.utils.StreamingConfig;
import com.trade.optimizer.models.Instrument;
import com.trade.optimizer.models.Order;

public class StreamingQuoteStorageImpl implements StreamingQuoteStorage {

    private static final String JDBC_DRIVER = "com.mysql.jdbc.Driver";
    private static final String DB_URL = StreamingConfig.QUOTE_STREAMING_DB_URL;

    private static final String USER = StreamingConfig.QUOTE_STREAMING_DB_USER;
    private static final String PASS = StreamingConfig.QUOTE_STREAMING_DB_PWD;

    private Connection conn = null;

    private static String quoteTable = null;

    public StreamingQuoteStorageImpl() {
    }

    @Override
    public void initializeJDBCConn() {
        try {
            System.out.println(
                    "StreamingQuoteStorageImpl.initializeJDBCConn(): creating JDBC connection for Streaming Quote...");

            Class.forName(JDBC_DRIVER);

            conn = DriverManager.getConnection(DB_URL, USER, PASS);
        } catch (ClassNotFoundException e) {
            System.out.println(
                    "StreamingQuoteStorageImpl.initializeJDBCConn(): ClassNotFoundException: "
                            + JDBC_DRIVER);
            e.printStackTrace();
        } catch (SQLException e) {
            System.out.println(
                    "StreamingQuoteStorageImpl.initializeJDBCConn(): SQLException on getConnection");
            e.printStackTrace();
        }
    }

    @Override
    public void closeJDBCConn() {
        if (conn != null) {
            try {
                System.out.println(
                        "StreamingQuoteStorageImpl.closeJDBCConn(): Closing JDBC connection for Streaming Quote...");
                conn.close();
            } catch (SQLException e) {
                System.out.println(
                        "StreamingQuoteStorageImpl.closeJDBCConn(): SQLException on conn close");
                e.printStackTrace();
            }
        } else {
            System.out.println(
                    "StreamingQuoteStorageImpl.closeJDBCConn(): WARNING: DB connection already null");
        }
    }

    @Override
    public void createDaysStreamingQuoteTable(String date) {
        if (conn != null) {
            Statement stmt;
            try {
                stmt = conn.createStatement();
                quoteTable = StreamingConfig.getStreamingQuoteTbNameAppendFormat(date);
                String sql = "CREATE TABLE " + quoteTable + " " + "(time varchar(32) NOT NULL, "
                        + " InstrumentToken varchar(32) NOT NULL, "
                        + " LastTradedPrice DECIMAL(20,4) NOT NULL, "
                        + " LastTradedQty BIGINT NOT NULL, "
                        + " AvgTradedPrice DECIMAL(20,4) NOT NULL, " + " Volume BIGINT NOT NULL, "
                        + " BuyQty BIGINT NOT NULL, " + " SellQty BIGINT NOT NULL, "
                        + " OpenPrice DECIMAL(20,4) NOT NULL, "
                        + " HighPrice DECIMAL(20,4) NOT NULL, "
                        + " LowPrice DECIMAL(20,4) NOT NULL, "
                        + " ClosePrice DECIMAL(20,4) NOT NULL, "
                        + " PRIMARY KEY (InstrumentToken, time)) "
                        + " ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARSET=utf8;";
                stmt.executeUpdate(sql);
                sql = "CREATE TABLE " + quoteTable + "_hist " + "(time varchar(32) NOT NULL, "
                        + " InstrumentToken varchar(32) NOT NULL, "
                        + " LastTradedPrice DECIMAL(20,4) NOT NULL, "
                        + " LastTradedQty BIGINT NOT NULL, "
                        + " AvgTradedPrice DECIMAL(20,4) NOT NULL, " + " Volume BIGINT NOT NULL, "
                        + " BuyQty BIGINT NOT NULL, " + " SellQty BIGINT NOT NULL, "
                        + " OpenPrice DECIMAL(20,4) NOT NULL, "
                        + " HighPrice DECIMAL(20,4) NOT NULL, "
                        + " LowPrice DECIMAL(20,4) NOT NULL, "
                        + " ClosePrice DECIMAL(20,4) NOT NULL,TickType varchar(32) NOT NULL,  "
                        + " PRIMARY KEY (InstrumentToken, time)) "
                        + " ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARSET=utf8;";
                stmt.executeUpdate(sql);

                sql = "CREATE TABLE " + quoteTable + "_priority " + "(time varchar(32) NOT NULL, "
                        + " InstrumentToken varchar(32) NOT NULL, "
                        + " PriorityPoint DECIMAL(20,4) NOT NULL, "
                        + " PRIMARY KEY (InstrumentToken)) "
                        + " ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARSET=utf8;";
                stmt.executeUpdate(sql);

                sql = "CREATE TABLE " + quoteTable + "_Signal " + "(time varchar(32) NOT NULL, "
                        + " InstrumentToken varchar(32) NOT NULL, "
                        + " Quantity varchar(32) NOT NULL, "
                        + " ProcessSignal varchar(32) NOT NULL, " + " Status varchar(32) NOT NULL, "
                        + " PRIMARY KEY (time, InstrumentToken, ProcessSignal)) "
                        + " ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARSET=utf8;";
                stmt.executeUpdate(sql);

                sql = "CREATE TABLE " + quoteTable + "_instrumentDetails " + "(time varchar(32) NOT NULL, "
                        + " instrumentToken varchar(32) NOT NULL,exchangeToken varchar(32) NOT NULL,"
                        + "tradingsymbol varchar(32) NOT NULL,name varchar(32) NOT NULL,"
                        + "last_price varchar(32) NOT NULL,tickSize varchar(32) NOT NULL,"
                        + "expiry varchar(32) NOT NULL,instrumentType varchar(32) NOT NULL,"
                        + "segment varchar(32) NOT NULL,exchange varchar(32) NOT NULL,"
                        + "strike varchar(32) NOT NULL,lotSize varchar(32) NOT NULL,"
                        + " PRIMARY KEY (time,instrumentToken)) "
                        + " ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARSET=utf8;";
                stmt.executeUpdate(sql);

                System.out.println(
                        "StreamingQuoteStorageImpl.createDaysStreamingQuoteTable(): SQL table for Streaming quote created, table name: ["
                                + quoteTable + "]");
            } catch (SQLException e) {
                System.out.println(
                        "StreamingQuoteStorageImpl.createDaysStreamingQuoteTable(): ERROR: SQLException on creating Table, cause: "
                                + e.getMessage());
            }
        } else {
            System.out.println(
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
                    StreamingQuoteModeQuote quoteModeQuote = (StreamingQuoteModeQuote) quoteList
                            .get(index);

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
                    prepStmt.setString(13, tickType);
                    prepStmt.executeUpdate();
                }
                sql = "INSERT INTO " + quoteTable + "_priority "
                        + "(Time, InstrumentToken, PriorityPoint) "
                        + "values(?,?,?)";
                prepStmt = conn.prepareStatement(sql);

                prepStmt.setString(1, quoteList.get(0).getTime());
                prepStmt.setString(2, quoteList.get(0).getInstrumentToken());
                prepStmt.setDouble(3, quoteList.get(0).ltp);

                prepStmt.executeUpdate();
                prepStmt.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        } else {
            if (conn != null) {
                System.out.println(
                        "StreamingQuoteStorageImpl.storeData(): ERROR: DB conn is null !!!");
            } else {
                System.out.println(
                        "StreamingQuoteStorageImpl.storeData(): ERROR: quote is not of type StreamingQuoteModeQuote !!!");
            }
        }
    }

    @Override
    public void storeData(StreamingQuote quote) {
        if (conn != null && quote instanceof StreamingQuoteModeQuote) {
            StreamingQuoteModeQuote quoteModeQuote = (StreamingQuoteModeQuote) quote;

            try {
                String sql = "INSERT INTO " + quoteTable + " "
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
                System.out.println(
                        "StreamingQuoteStorageImpl.storeData(): ERROR: SQLException on Storing data to Table: "
                                + quote);
                System.out.println("StreamingQuoteStorageImpl.storeData(): [SQLException Cause]: "
                        + e.getMessage());
            }
        } else {
            if (conn != null) {
                System.out.println(
                        "StreamingQuoteStorageImpl.storeData(): ERROR: DB conn is null !!!");
            } else {
                System.out.println(
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
                System.out.println(
                        "StreamingQuoteStorageImpl.getOHLCDataByTimeRange(): ERROR: SQLException on fetching data from Table, cause: "
                                + e.getMessage());
            }
        } else {
            ohlcMap = null;
            System.out.println(
                    "StreamingQuoteStorageImpl.getOHLCDataByTimeRange(): ERROR: DB conn is null !!!");
        }

        return ohlcMap;
    }

    @Override
    public List<StreamingQuote> getQuoteListByTimeRange(String instrumentToken, String prevTime,
            String currTime) {
        List<StreamingQuote> streamingQuoteList = new ArrayList<StreamingQuote>();

        if (conn != null) {
            try {
                Statement stmt = conn.createStatement();

                String openSql = "SELECT * FROM " + quoteTable + " WHERE Time >= '" + prevTime
                        + "' AND Time <= '" + currTime + "' AND InstrumentToken = '"
                        + instrumentToken + "'";
                ResultSet openRs = stmt.executeQuery(openSql);
                while (openRs.next()) {
                    String time = openRs.getString("Time");
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

                    StreamingQuote streamingQuote = new StreamingQuoteModeQuote(time,
                            instrument_Token, lastTradedPrice, lastTradedQty, avgTradedPrice,
                            volume, buyQty, sellQty, openPrice, highPrice, lowPrice, closePrice);
                    streamingQuoteList.add(streamingQuote);
                }

                stmt.close();
            } catch (SQLException e) {
                streamingQuoteList = null;
                System.out.println(
                        "StreamingQuoteStorageImpl.getQuoteByTimeRange(): ERROR: SQLException on fetching data from Table, cause: "
                                + e.getMessage());
            }
        } else {
            streamingQuoteList = null;
            System.out.println(
                    "StreamingQuoteStorageImpl.getQuoteByTimeRange(): ERROR: DB conn is null !!!");
        }

        return streamingQuoteList;
    }

    @Override
    public String[] getTopPrioritizedTokenList(int i) {
        String[] instrumentList = null;
        if (conn != null) {
            try {
                instrumentList = new String[i];
                Statement stmt = conn.createStatement();
                String openSql = "SELECT InstrumentToken FROM " + quoteTable
                        + " ORDER BY PriorityPoint DESC LIMIT " + i + "";
                ResultSet openRs = stmt.executeQuery(openSql);
                for (int index = 0; index < openRs.getFetchSize(); index++) {
                    openRs.next();
                    instrumentList[index] = String.valueOf(openRs.getLong(1));
                }
            } catch (SQLException e) {
                System.out.println(
                        "StreamingQuoteStorageImpl.getTopPrioritizedTokenList(): ERROR: SQLException on fetching data from Table, cause: "
                                + e.getMessage());
            }
        } else {
            System.out.println(
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

                for (int index = 0; index < openRs.getFetchSize(); index++) {
                    openRs.next();
                    Order order = new Order();
                    order.symbol = String.valueOf(openRs.getString(1));
                    order.transactionType = String.valueOf(openRs.getString(2));
                    order.quantity = String.valueOf(openRs.getString(3));

                    orders.add(order);

                    stmt = conn.createStatement();
                    openSql = "update " + quoteTable
                            + "_Signal set status ='orderPlaced' where InstrumentToken= "
                            + openRs.getString(1) + " and ProcessSignal= " + openRs.getString(2)
                            + " and Time= " + openRs.getString(3) + "";
                    stmt.executeUpdate(openSql);

                }
                stmt.close();
            } catch (SQLException e) {
                System.out.println(
                        "StreamingQuoteStorageImpl.getTopPrioritizedTokenList(): ERROR: SQLException on fetching data from Table, cause: "
                                + e.getMessage());
            }
        } else {
            System.out.println(
                    "StreamingQuoteStorageImpl.getTopPrioritizedTokenList(): ERROR: DB conn is null !!!");
        }
        return orders;
    }

    @Override
    public void saveInstrumentDetails(List<Instrument> instrumentList, String time) {

        if (conn != null) {

            try {
                String sql = "INSERT INTO " + quoteTable + "_instrumentDetails "
                        + "(time,instrumentToken,exchangeToken,tradingsymbol,name,last_price,"
                        + "tickSize,expiry,instrumentType,segment,exchange,strike,lotSize) "
                        + "values(?,?,?,?,?,?,?,?,?,?,?,?,?)";
                PreparedStatement prepStmt = conn.prepareStatement(sql);
                for (int index = 0; index < instrumentList.size(); index++) {
                    Instrument instrument = instrumentList.get(index);

                    prepStmt.setString(1, time);
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
                System.out.println(
                        "StreamingQuoteStorageImpl.storeData(): ERROR: DB conn is null !!!");
            } else {
                System.out.println(
                        "StreamingQuoteStorageImpl.storeData(): ERROR: quote is not of type StreamingQuoteModeQuote !!!");
            }
        }

    }

    @Override
    public String[] getInstrumentDetailsOnTokenId(String instrumentToken) {
        String[] param = new String[3];
        if (conn != null) {
            try {
                Statement stmt = conn.createStatement();

                String openSql = "SELECT lotSize,tradingsymbol,,exchange FROM " + quoteTable
                        + "_instrumentDetails WHERE instrumentToken='" + instrumentToken + "'";
                ResultSet openRs = stmt.executeQuery(openSql);
                while (openRs.next()) {
                    param[0] = openRs.getString(0);
                    param[1] = openRs.getString(1);
                    param[2] = openRs.getString(2);
                }
                stmt.close();
            } catch (SQLException e) {
                System.out.println(
                        "StreamingQuoteStorageImpl.getQuoteByTimeRange(): ERROR: SQLException on fetching data from Table, cause: "
                                + e.getMessage());
            }
        } else {
            System.out.println(
                    "StreamingQuoteStorageImpl.getQuoteByTimeRange(): ERROR: DB conn is null !!!");
        }

        return param;
    }
}
