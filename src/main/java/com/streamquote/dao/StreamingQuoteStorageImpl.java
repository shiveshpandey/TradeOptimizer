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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;
import java.util.logging.Logger;

import com.streamquote.model.StreamingQuoteModeQuote;
import com.streamquote.utils.StreamingConfig;
import com.trade.optimizer.models.Instrument;
import com.trade.optimizer.models.Order;
import com.trade.optimizer.models.Tick;
import com.trade.optimizer.signal.parameter.MACDSignalParam;
import com.trade.optimizer.signal.parameter.PSarSignalParam;
import com.trade.optimizer.signal.parameter.RSISignalParam;

public class StreamingQuoteStorageImpl implements StreamingQuoteStorage {
	private final static Logger LOGGER = Logger.getLogger(StreamingQuoteStorageImpl.class.getName());

	private static final String JDBC_DRIVER = "com.mysql.jdbc.Driver";
	private static final String DB_URL = StreamingConfig.QUOTE_STREAMING_DB_URL;

	private static final String USER = StreamingConfig.QUOTE_STREAMING_DB_USER;
	private static final String PASS = StreamingConfig.QUOTE_STREAMING_DB_PWD;
	private DateFormat dtTmFmt = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
	private DateFormat dtTmFmtNoSeconds = new SimpleDateFormat("yyyy-MM-dd HH:mm");
	private Connection conn = null;

	private static String quoteTable = null;

	@Override
	public void initializeJDBCConn() {
		dtTmFmt.setTimeZone(TimeZone.getTimeZone("IST"));
		dtTmFmtNoSeconds.setTimeZone(TimeZone.getTimeZone("IST"));
		try {
			LOGGER.info(
					"StreamingQuoteStorageImpl.initializeJDBCConn(): creating JDBC connection for Streaming Quote...");

			Class.forName(JDBC_DRIVER);

			conn = DriverManager.getConnection(DB_URL, USER, PASS);
		} catch (ClassNotFoundException e) {
			LOGGER.info("StreamingQuoteStorageImpl.initializeJDBCConn(): ClassNotFoundException: " + JDBC_DRIVER);
		} catch (SQLException e) {
			LOGGER.info("StreamingQuoteStorageImpl.initializeJDBCConn(): SQLException on getConnection");
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
				LOGGER.info("StreamingQuoteStorageImpl.closeJDBCConn(): SQLException on conn close");
			}
		} else {
			LOGGER.info("StreamingQuoteStorageImpl.closeJDBCConn(): WARNING: DB connection already null");
		}
	}

	@Override
	public void createDaysStreamingQuoteTable(String date) throws SQLException {
		if (conn != null) {
			Statement stmt = conn.createStatement();
			quoteTable = StreamingConfig.getStreamingQuoteTbNameAppendFormat(date);
			String sql;
			try {
				sql = "CREATE TABLE " + quoteTable + " " + "(time timestamp, " + " InstrumentToken varchar(32) , "
						+ " LastTradedPrice DECIMAL(20,4) , " + " LastTradedQty BIGINT , "
						+ " AvgTradedPrice DECIMAL(20,4) , " + " Volume BIGINT , " + " BuyQty BIGINT , "
						+ " SellQty BIGINT , " + " OpenPrice DECIMAL(20,4) , " + " HighPrice DECIMAL(20,4) , "
						+ " LowPrice DECIMAL(20,4) , "
						+ " ClosePrice DECIMAL(20,4) ,timestampGrp timestamp,usedForSignal varchar(32), "
						+ " PRIMARY KEY (InstrumentToken, time)) "
						+ " ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARSET=utf8;";
				stmt.executeUpdate(sql);
			} catch (SQLException e) {
				LOGGER.info(
						"StreamingQuoteStorageImpl.createDaysStreamingQuoteTable(): ERROR: SQLException on creating Table, cause: "
								+ e.getMessage());
			}
			try {
				sql = "CREATE TABLE " + quoteTable + "_hist " + "(time timestamp, " + " InstrumentToken varchar(32) , "
						+ " LastTradedPrice DECIMAL(20,4) , " + " LastTradedQty BIGINT , "
						+ " AvgTradedPrice DECIMAL(20,4) , " + " Volume BIGINT , " + " BuyQty BIGINT , "
						+ " SellQty BIGINT , " + " OpenPrice DECIMAL(20,4) , " + " HighPrice DECIMAL(20,4) , "
						+ " LowPrice DECIMAL(20,4) , " + " ClosePrice DECIMAL(20,4) ,TickType varchar(32) ,  "
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
						+ "segment varchar(32) ,exchange varchar(32) ," + "strike varchar(32) ,lotSize varchar(32) ,"
						+ " PRIMARY KEY (time,instrumentToken)) "
						+ " ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARSET=utf8;";
				stmt.executeUpdate(sql);

			} catch (SQLException e) {
				LOGGER.info(
						"StreamingQuoteStorageImpl.createDaysStreamingQuoteTable(): ERROR: SQLException on creating Table, cause: "
								+ e.getMessage());
			}
			try {
				sql = "CREATE TABLE " + quoteTable + "_signalParam " + "(time timestamp, "
						+ " instrumentToken varchar(32) ,high DECIMAL(20,4) ,low DECIMAL(20,4) ,close DECIMAL(20,4) ,pSar DECIMAL(20,4) ,"
						+ "eP DECIMAL(20,4) ,eP_pSar DECIMAL(20,4) ,accFactor DECIMAL(20,4) ,eP_pSarXaccFactor DECIMAL(20,4) ,"
						+ "trend DECIMAL(20,4) ,upMove DECIMAL(20,4) ,downMove DECIMAL(20,4) ,avgUpMove DECIMAL(20,4) ,"
						+ "avgDownMove DECIMAL(20,4) ,relativeStrength DECIMAL(20,4) ,RSI DECIMAL(20,4) ,fastEma DECIMAL(20,4) ,"
						+ "slowEma DECIMAL(20,4) ,difference DECIMAL(20,4) ,strategySignal  DECIMAL(20,4) ,"
						+ " PRIMARY KEY (time,instrumentToken)) "
						+ " ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARSET=utf8;";

				stmt.executeUpdate(sql);

			} catch (SQLException e) {
				LOGGER.info(
						"StreamingQuoteStorageImpl.createDaysStreamingQuoteTable(): ERROR: SQLException on creating Table, cause: "
								+ e.getMessage());
			}
		} else {
			LOGGER.info("StreamingQuoteStorageImpl.createDaysStreamingQuoteTable(): ERROR: DB conn is null !!!");
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
						StreamingQuoteModeQuote quoteModeQuote = (StreamingQuoteModeQuote) quoteList.get(index);

						prepStmt.setTimestamp(1, new Timestamp(dtTmFmt.parse(quoteModeQuote.getTime()).getTime()));

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
					sql = "INSERT INTO " + quoteTable + "_priority " + "(Time, InstrumentToken, PriorityPoint) "
							+ "values(?,?,?)";
					prepStmt = conn.prepareStatement(sql);

					prepStmt.setTimestamp(1,
							new Timestamp(dtTmFmt.parse(quoteList.get(quoteList.size() - 1).getTime()).getTime()));
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
	//
	// @Override
	// public void storeData(StreamingQuote quote) {
	// if (conn != null && quote instanceof StreamingQuoteModeQuote) {
	// StreamingQuoteModeQuote quoteModeQuote = (StreamingQuoteModeQuote) quote;
	//
	// try {
	// String sql = "INSERT INTO " + quoteTable + ""
	// + "(Time, InstrumentToken, LastTradedPrice, LastTradedQty,
	// AvgTradedPrice, "
	// + "Volume, BuyQty, SellQty, OpenPrice, HighPrice, LowPrice, ClosePrice) "
	// + "values(?,?,?,?,?,?,?,?,?,?,?,?)";
	// PreparedStatement prepStmt = conn.prepareStatement(sql);
	//
	// prepStmt.setString(1, quoteModeQuote.getTime());
	// prepStmt.setString(2, quoteModeQuote.getInstrumentToken());
	// prepStmt.setDouble(3, quoteModeQuote.getLtp());
	// prepStmt.setLong(4, quoteModeQuote.getLastTradedQty());
	// prepStmt.setDouble(5, quoteModeQuote.getAvgTradedPrice());
	// prepStmt.setLong(6, quoteModeQuote.getVol());
	// prepStmt.setLong(7, quoteModeQuote.getBuyQty());
	// prepStmt.setLong(8, quoteModeQuote.getSellQty());
	// prepStmt.setDouble(9, quoteModeQuote.getOpenPrice());
	// prepStmt.setDouble(10, quoteModeQuote.getHighPrice());
	// prepStmt.setDouble(11, quoteModeQuote.getLowPrice());
	// prepStmt.setDouble(12, quoteModeQuote.getClosePrice());
	//
	// prepStmt.executeUpdate();
	// prepStmt.close();
	// } catch (SQLException e) {
	// LOGGER.info("StreamingQuoteStorageImpl.storeData(): ERROR: SQLException
	// on Storing data to Table: "
	// + quote);
	// LOGGER.info("StreamingQuoteStorageImpl.storeData(): [SQLException Cause]:
	// " + e.getMessage());
	// }
	// } else {
	// if (conn != null) {
	// LOGGER.info("StreamingQuoteStorageImpl.storeData(): ERROR: DB conn is
	// null !!!");
	// } else {
	// LOGGER.info(
	// "StreamingQuoteStorageImpl.storeData(): ERROR: quote is not of type
	// StreamingQuoteModeQuote !!!");
	// }
	// }
	// }

	@Override
	public void storeData(ArrayList<Tick> ticks) {
		if (conn != null) {

			try {
				String sql = "INSERT INTO " + quoteTable + ""
						+ "(Time, InstrumentToken, LastTradedPrice, LastTradedQty, AvgTradedPrice, "
						+ "Volume, BuyQty, SellQty, OpenPrice, HighPrice, LowPrice, ClosePrice,timestampGrp,usedForSignal) "
						+ "values(?,?,?,?,?,?,?,?,?,?,?,?,?,?)";
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
					prepStmt.setTimestamp(13, new Timestamp(dtTmFmtNoSeconds.parse(new Date().toString()).getTime()));
					prepStmt.setString(14, "");
					prepStmt.executeUpdate();
				}
				prepStmt.close();

			} catch (SQLException | ParseException e) {
				LOGGER.info("StreamingQuoteStorageImpl.storeData(): ERROR: SQLException on Storing data to Table: "
						+ ticks);
				LOGGER.info("StreamingQuoteStorageImpl.storeData(): [SQLException Cause]: " + e.getMessage());
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
	//
	// @Override
	// public OHLCquote getOHLCDataByTimeRange(String instrumentToken, String
	// prevTime, String currTime) {
	// OHLCquote ohlcMap = null;
	//
	// if (conn != null) {
	// try {
	// Statement stmt = conn.createStatement();
	//
	// String openSql = "SELECT LastTradedPrice FROM " + quoteTable + " WHERE
	// Time >= '" + prevTime
	// + "' AND Time <= '" + currTime + "' AND InstrumentToken = '" +
	// instrumentToken
	// + "' ORDER BY Time ASC LIMIT 1";
	// ResultSet openRs = stmt.executeQuery(openSql);
	// openRs.next();
	// Double openQuote = openRs.getDouble("LastTradedPrice");
	//
	// String highSql = "SELECT MAX(LastTradedPrice) FROM " + quoteTable + "
	// WHERE Time >= '" + prevTime
	// + "' AND Time <= '" + currTime + "' AND InstrumentToken = '" +
	// instrumentToken + "'";
	// ResultSet highRs = stmt.executeQuery(highSql);
	// highRs.next();
	// Double highQuote = highRs.getDouble(1);
	//
	// String lowSql = "SELECT MIN(LastTradedPrice) FROM " + quoteTable + "
	// WHERE Time >= '" + prevTime
	// + "' AND Time <= '" + currTime + "' AND InstrumentToken = '" +
	// instrumentToken + "'";
	// ResultSet lowRs = stmt.executeQuery(lowSql);
	// lowRs.next();
	// Double lowQuote = lowRs.getDouble(1);
	//
	// String closeSql = "SELECT LastTradedPrice FROM " + quoteTable + " WHERE
	// Time >= '" + prevTime
	// + "' AND Time <= '" + currTime + "' AND InstrumentToken = '" +
	// instrumentToken
	// + "' ORDER BY Time DESC LIMIT 1";
	// ResultSet closeRs = stmt.executeQuery(closeSql);
	// closeRs.next();
	// Double closeQuote = closeRs.getDouble("LastTradedPrice");
	//
	// String volSql = "SELECT Volume FROM " + quoteTable + " WHERE Time >= '" +
	// prevTime + "' AND Time <= '"
	// + currTime + "' AND InstrumentToken = '" + instrumentToken + "' ORDER BY
	// Time DESC LIMIT 1";
	// ResultSet volRs = stmt.executeQuery(volSql);
	// volRs.next();
	// Long volQuote = volRs.getLong(1);
	//
	// ohlcMap = new OHLCquote(openQuote, highQuote, lowQuote, closeQuote,
	// volQuote);
	//
	// stmt.close();
	// } catch (SQLException e) {
	// ohlcMap = null;
	// LOGGER.info(
	// "StreamingQuoteStorageImpl.getOHLCDataByTimeRange(): ERROR: SQLException
	// on fetching data from Table, cause: "
	// + e.getMessage());
	// }
	// } else {
	// ohlcMap = null;
	// LOGGER.info("StreamingQuoteStorageImpl.getOHLCDataByTimeRange(): ERROR:
	// DB conn is null !!!");
	// }
	//
	// return ohlcMap;
	// }

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
			LOGGER.info("StreamingQuoteStorageImpl.getTopPrioritizedTokenList(): ERROR: DB conn is null !!!");
		}
		return instrumentList;
	}

	@Override
	public List<Order> getOrderListToPlace() {
		List<Order> orders = new ArrayList<Order>();
		if (conn != null) {
			try {
				Statement stmt = conn.createStatement();
				String openSql = "SELECT InstrumentToken,ProcessSignal,Quantity,Time FROM " + quoteTable
						+ "_Signal where status ='active'";
				ResultSet openRs = stmt.executeQuery(openSql);

				while (openRs.next()) {
					Order order = new Order();
					order.symbol = String.valueOf(openRs.getString(1));
					order.transactionType = String.valueOf(openRs.getString(2));
					order.quantity = String.valueOf(openRs.getString(3));

					orders.add(order);

					stmt = conn.createStatement();
					openSql = "update " + quoteTable + "_Signal set status ='orderPlaced' where InstrumentToken= "
							+ openRs.getString(1) + " and ProcessSignal= '" + openRs.getString(2) + "' and Time= "
							+ openRs.getTimestamp(4) + "";
					stmt.executeUpdate(openSql);

				}
				stmt.close();
			} catch (SQLException e) {
				LOGGER.info(
						"StreamingQuoteStorageImpl.getOrderListToPlace(): ERROR: SQLException on fetching data from Table, cause: "
								+ e.getMessage());
			}
		} else {
			LOGGER.info("StreamingQuoteStorageImpl.getOrderListToPlace(): ERROR: DB conn is null !!!");
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
				LOGGER.info(e.getMessage());
			}
		} else {
			if (conn != null) {
				LOGGER.info("StreamingQuoteStorageImpl.saveInstrumentDetails(): ERROR: DB conn is null !!!");
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
			LOGGER.info("StreamingQuoteStorageImpl.getInstrumentDetailsOnTokenId(): ERROR: DB conn is null !!!");
		}

		return param;
	}

	// @Override
	// public List<StreamingQuoteModeQuote>
	// getProcessableQuoteDataOnTokenId(String instrumentToken, int count) {
	// List<StreamingQuoteModeQuote> streamingQuoteList = new
	// ArrayList<StreamingQuoteModeQuote>();
	// if (conn != null) {
	// try {
	// Statement stmt = conn.createStatement();
	//
	// String openSql = "SELECT * FROM " + quoteTable + " WHERE InstrumentToken
	// = '" + instrumentToken
	// + "' order by Time desc limit " + count;
	// ResultSet openRs = stmt.executeQuery(openSql);
	// while (openRs.next()) {
	// Timestamp time = openRs.getTimestamp("Time");
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
	// StreamingQuoteModeQuote streamingQuote = new
	// StreamingQuoteModeQuote(time.toString(),
	// instrument_Token, lastTradedPrice, lastTradedQty, avgTradedPrice, volume,
	// buyQty, sellQty,
	// openPrice, highPrice, lowPrice, closePrice);
	// streamingQuoteList.add(streamingQuote);
	// }
	//
	// stmt.close();
	// } catch (SQLException e) {
	// streamingQuoteList = null;
	// LOGGER.info(
	// "StreamingQuoteStorageImpl.getProcessableQuoteDataOnTokenId(): ERROR:
	// SQLException on fetching data from Table, cause: "
	// + e.getMessage());
	// }
	// } else {
	// streamingQuoteList = null;
	// LOGGER.info("StreamingQuoteStorageImpl.getProcessableQuoteDataOnTokenId():
	// ERROR: DB conn is null !!!");
	// }
	//
	// return streamingQuoteList;
	//
	// }

	@Override
	public void saveGeneratedSignals(Map<Long, String> signalList, List<Long> instrumentList) {

		if (conn != null) {

			try {
				String sql = "INSERT INTO " + quoteTable + "_signal "
						+ "(time,instrumentToken,quantity,processSignal,status) " + "values(?,?,?,?,?)";
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
				LOGGER.info("StreamingQuoteStorageImpl.saveGeneratedSignals(): ERROR: DB conn is null !!!");
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
						+ "_Signal where status ='active' and InstrumentToken= '" + instrument + "' and processSignal='"
						+ processSignal + "'";
				ResultSet openRs = stmt.executeQuery(openSql);
				if (openRs.next())
					return false;

				openSql = "SELECT Time FROM " + quoteTable + "_Signal where status ='active' and InstrumentToken= '"
						+ instrument + "'";
				openRs = stmt.executeQuery(openSql);

				while (openRs.next()) {
					stmt = conn.createStatement();
					openSql = "update " + quoteTable + "_Signal set status ='timeOut' where InstrumentToken= '"
							+ instrument + "' and Time=" + openRs.getTimestamp(1) + "";
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
			LOGGER.info("StreamingQuoteStorageImpl.updateOldSignalInSignalTable(): ERROR: DB conn is null !!!");
		}
		return true;
	}

	@Override
	public void calculateAndStoreStrategySignalParameters(String instrumentToken, String endingTimeToMatch) {

		if (conn != null) {
			try {
				Timestamp endTime = new Timestamp(dtTmFmtNoSeconds.parse(endingTimeToMatch).getTime());

				Statement stmt = conn.createStatement();
				String openSql = "SELECT distinct timestampGrp FROM " + quoteTable + " where InstrumentToken ='"
						+ instrumentToken + "' and (usedForSignal = '' or usedForSignal = 'null') and timestampGrp<"
						+ endTime + " ORDER BY Time ASC ";
				ResultSet timeStampRs = stmt.executeQuery(openSql);
				openSql = "update " + quoteTable + " set usedForSignal ='used' where InstrumentToken= '"
						+ instrumentToken + "' and timestampGrp in (SELECT distinct timestampGrp FROM " + quoteTable
						+ " where InstrumentToken ='" + instrumentToken
						+ "' and (usedForSignal = '' or usedForSignal = 'null') and timestampgrp<" + endTime + ")";
				stmt.executeUpdate(openSql);

				while (timeStampRs.next()) {
					Double low = null;
					Double high = null;
					Double close = null;

					openSql = "SELECT * FROM " + quoteTable + " where InstrumentToken ='" + instrumentToken
							+ "' and timestampGrp =" + timeStampRs.getTimestamp("timestampGrp")
							+ " ORDER BY Time DESC ";
					ResultSet openRs1 = stmt.executeQuery(openSql);
					boolean firstRecord = true;
					while (openRs1.next()) {
						if (null == low || openRs1.getDouble("low") < low)
							low = openRs1.getDouble("low");
						if (null == high || openRs1.getDouble("high") > high)
							high = openRs1.getDouble("high");
						if (firstRecord) {
							close = openRs1.getDouble("close");
							firstRecord = false;
						}
					}

					openSql = "SELECT * FROM " + quoteTable + "_SignalParams where InstrumentToken ='" + instrumentToken
							+ "' ORDER BY Time DESC LIMIT 1 ";
					ResultSet openRs2 = stmt.executeQuery(openSql);

					PSarSignalParam p1 = null;
					RSISignalParam r1 = null;
					MACDSignalParam m1 = null;

					if (openRs2.next()) {
						if (null != openRs2.getString(19) && !"".equalsIgnoreCase(openRs2.getString(19))) {
							this.whenPsarRsiMacdAll3sPreviousSignalsAvailable(openRs2, low, high, close, p1, r1, m1);

						} else if (null != openRs2.getString(15) && !"".equalsIgnoreCase(openRs2.getString(15))) {
							this.whenPsarRsiPreviousSignalsAvailableButNotMacd(stmt, instrumentToken, low, high, close,
									p1, r1, m1);

						} else if (null != openRs2.getString(4) && !"".equalsIgnoreCase(openRs2.getString(4))
								&& null != openRs2.getString(5) && !"".equalsIgnoreCase(openRs2.getString(5))
								&& null != openRs2.getString(8) && !"".equalsIgnoreCase(openRs2.getString(8))) {
							this.whenPsarPreviousSignalsAvailableButNotRsiAndMacd(stmt, instrumentToken, low, high,
									close, p1, r1, m1);
						}

					} else {
						this.whenPsarRsiMacdAll3sPreviousSignalsNOTAvailable(stmt, instrumentToken, low, high, close,
								p1, r1, m1);
					}
					String sql = "INSERT INTO " + quoteTable + "_signalParam "
							+ "(Time, InstrumentToken, high,low,close,pSar,eP,eP_pSar,accFactor,eP_pSarXaccFactor,trend,"
							+ "upMove,downMove,avgUpMove,avgDownMove,relativeStrength,RSI,fastEma,slowEma,difference,strategySignal,timestampGrp) "
							+ "values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)";
					PreparedStatement prepStmt = conn.prepareStatement(sql);

					prepStmt.setTimestamp(1, new Timestamp(new Date().getTime()));
					prepStmt.setTimestamp(22, timeStampRs.getTimestamp("timestampGrp"));
					prepStmt.setString(2, instrumentToken);
					prepStmt.setDouble(3, p1.getHigh());
					prepStmt.setDouble(4, p1.getLow());
					prepStmt.setDouble(5, close);
					prepStmt.setDouble(6, p1.getpSar());
					prepStmt.setDouble(7, p1.geteP());
					prepStmt.setDouble(8, p1.geteP_pSar());
					prepStmt.setDouble(9, p1.getAccFactor());
					prepStmt.setDouble(10, p1.geteP_pSarXaccFactor());
					prepStmt.setInt(11, p1.getTrend());
					prepStmt.setDouble(12, r1.getUpMove());
					prepStmt.setDouble(13, r1.getDownMove());
					prepStmt.setDouble(14, r1.getAvgUpMove());
					prepStmt.setDouble(15, r1.getAvgDownMove());
					prepStmt.setDouble(16, r1.getRelativeStrength());
					prepStmt.setDouble(17, r1.getRSI());
					prepStmt.setDouble(18, m1.getFastEma());
					prepStmt.setDouble(19, m1.getSlowEma());
					prepStmt.setDouble(20, m1.getDifference());
					prepStmt.setDouble(21, m1.getSignal());
					prepStmt.executeUpdate();

					prepStmt.close();
				}
			} catch (SQLException | ParseException e) {
				LOGGER.info(
						"StreamingQuoteStorageImpl.calculateAndStoreStrategySignalParameters(): ERROR: SQLException on fetching data from Table, cause: "
								+ e.getMessage());
			}
		} else {
			LOGGER.info(
					"StreamingQuoteStorageImpl.calculateAndStoreStrategySignalParameters(): ERROR: DB conn is null !!!");
		}
	}

	private void whenPsarRsiMacdAll3sPreviousSignalsNOTAvailable(Statement stmt, String instrumentToken, Double low,
			Double high, Double close, PSarSignalParam p1, RSISignalParam r1, MACDSignalParam m1) throws SQLException {

		String openSql = "SELECT * FROM " + quoteTable + "_SignalParams where InstrumentToken ='" + instrumentToken
				+ "' ORDER BY Time DESC LIMIT 35 ";
		ResultSet openRs3 = stmt.executeQuery(openSql);
		List<MACDSignalParam> macdSignalParamList = new ArrayList<MACDSignalParam>();
		List<RSISignalParam> rsiSignalParamList = new ArrayList<RSISignalParam>();
		MACDSignalParam macdSignalParam;
		RSISignalParam rsiSignalParam;
		boolean firstLoop = true;
		while (openRs3.next()) {
			if (firstLoop) {
				p1 = new PSarSignalParam(high, low);
				firstLoop = false;
			}
			macdSignalParam = new MACDSignalParam();
			rsiSignalParam = new RSISignalParam();

			macdSignalParam.setClose(close);
			macdSignalParam.setDifference(openRs3.getDouble("difference"));
			macdSignalParam.setFastEma(openRs3.getDouble("fastEma"));
			macdSignalParam.setSlowEma(openRs3.getDouble("slowEma"));
			macdSignalParam.setSignal(openRs3.getDouble("strategySignal"));
			macdSignalParamList.add(macdSignalParam);

			rsiSignalParam.setClose(close);
			rsiSignalParam.setDownMove(openRs3.getDouble("downMove"));
			rsiSignalParam.setUpMove(openRs3.getDouble("upMove"));
			rsiSignalParam.setAvgDownMove(openRs3.getDouble("avgDownMove"));
			rsiSignalParam.setAvgDownMove(openRs3.getDouble("avgDownMove"));
			rsiSignalParam.setRelativeStrength(openRs3.getDouble("relativeStrength"));
			rsiSignalParam.setRSI(openRs3.getDouble("rSI"));
			rsiSignalParamList.add(rsiSignalParam);
		}
		r1 = new RSISignalParam(rsiSignalParamList);
		m1 = new MACDSignalParam(macdSignalParamList);

	}

	private void whenPsarPreviousSignalsAvailableButNotRsiAndMacd(Statement stmt, String instrumentToken, Double low,
			Double high, Double close, PSarSignalParam p1, RSISignalParam r1, MACDSignalParam m1) throws SQLException {

		String openSql = "SELECT * FROM " + quoteTable + "_SignalParams where InstrumentToken ='" + instrumentToken
				+ "' ORDER BY Time DESC LIMIT 35 ";
		ResultSet openRs3 = stmt.executeQuery(openSql);
		List<MACDSignalParam> macdSignalParamList = new ArrayList<MACDSignalParam>();
		List<RSISignalParam> rsiSignalParamList = new ArrayList<RSISignalParam>();
		MACDSignalParam macdSignalParam;
		RSISignalParam rsiSignalParam;
		boolean firstLoop = true;
		while (openRs3.next()) {
			if (firstLoop) {
				p1 = new PSarSignalParam(high, low, openRs3.getDouble("pSar"), openRs3.getDouble("eP"),
						openRs3.getDouble("eP_pSar"), openRs3.getDouble("accFactor"),
						openRs3.getDouble("eP_pSarXaccFactor"), openRs3.getInt("trend"));
				firstLoop = false;
			}
			macdSignalParam = new MACDSignalParam();
			rsiSignalParam = new RSISignalParam();

			macdSignalParam.setClose(close);
			macdSignalParam.setDifference(openRs3.getDouble("difference"));
			macdSignalParam.setFastEma(openRs3.getDouble("fastEma"));
			macdSignalParam.setSlowEma(openRs3.getDouble("slowEma"));
			macdSignalParam.setSignal(openRs3.getDouble("strategySignal"));
			macdSignalParamList.add(macdSignalParam);

			rsiSignalParam.setClose(close);
			rsiSignalParam.setDownMove(openRs3.getDouble("downMove"));
			rsiSignalParam.setUpMove(openRs3.getDouble("upMove"));
			rsiSignalParam.setAvgDownMove(openRs3.getDouble("avgDownMove"));
			rsiSignalParam.setAvgDownMove(openRs3.getDouble("avgDownMove"));
			rsiSignalParam.setRelativeStrength(openRs3.getDouble("relativeStrength"));
			rsiSignalParam.setRSI(openRs3.getDouble("rSI"));
			rsiSignalParamList.add(rsiSignalParam);
		}
		r1 = new RSISignalParam(rsiSignalParamList);
		m1 = new MACDSignalParam(macdSignalParamList);
	}

	private void whenPsarRsiPreviousSignalsAvailableButNotMacd(Statement stmt, String instrumentToken, Double low,
			Double high, Double close, PSarSignalParam p1, RSISignalParam r1, MACDSignalParam m1) throws SQLException {
		String openSql = "SELECT * FROM " + quoteTable + "_SignalParams where InstrumentToken ='" + instrumentToken
				+ "' ORDER BY Time DESC LIMIT 35 ";

		ResultSet openRs3 = stmt.executeQuery(openSql);
		List<MACDSignalParam> macdSignalParamList = new ArrayList<MACDSignalParam>();
		MACDSignalParam macdSignalParam;
		boolean firstLoop = true;

		while (openRs3.next()) {
			if (firstLoop) {
				p1 = new PSarSignalParam(high, low, openRs3.getDouble("pSar"), openRs3.getDouble("eP"),
						openRs3.getDouble("eP_pSar"), openRs3.getDouble("accFactor"),
						openRs3.getDouble("eP_pSarXaccFactor"), openRs3.getInt("trend"));
				r1 = new RSISignalParam(openRs3.getDouble("close"), close, openRs3.getDouble("upMove"),
						openRs3.getDouble("downMove"), openRs3.getDouble("avgUpMove"), openRs3.getDouble("avgDownMove"),
						openRs3.getDouble("relativeStrength"), openRs3.getDouble("rSI"));
				firstLoop = false;
			}
			macdSignalParam = new MACDSignalParam();
			macdSignalParam.setClose(close);
			macdSignalParam.setDifference(openRs3.getDouble("difference"));
			macdSignalParam.setFastEma(openRs3.getDouble("fastEma"));
			macdSignalParam.setSlowEma(openRs3.getDouble("slowEma"));
			macdSignalParam.setSignal(openRs3.getDouble("strategySignal"));
			macdSignalParamList.add(macdSignalParam);
		}
		m1 = new MACDSignalParam(macdSignalParamList);
	}

	private void whenPsarRsiMacdAll3sPreviousSignalsAvailable(ResultSet openRs2, Double low, Double high, Double close,
			PSarSignalParam p1, RSISignalParam r1, MACDSignalParam m1) throws SQLException {

		p1 = new PSarSignalParam(high, low, openRs2.getDouble("pSar"), openRs2.getDouble("eP"),
				openRs2.getDouble("eP_pSar"), openRs2.getDouble("accFactor"), openRs2.getDouble("eP_pSarXaccFactor"),
				openRs2.getInt("trend"));
		r1 = new RSISignalParam(openRs2.getDouble("close"), close, openRs2.getDouble("upMove"),
				openRs2.getDouble("downMove"), openRs2.getDouble("avgUpMove"), openRs2.getDouble("avgDownMove"),
				openRs2.getDouble("relativeStrength"), openRs2.getDouble("rSI"));
		m1 = new MACDSignalParam(close, openRs2.getDouble("fastEma"), openRs2.getDouble("slowEma"),
				openRs2.getDouble("strategySignal"));

	}

	@Override
	public ArrayList<Long> getInstrumentTokenIdsFromSymbols(Map<String, Double> stocksSymbolArray) {

		ArrayList<Long> instrumentList = new ArrayList<Long>();
		ArrayList<String> tradingSymbols = new ArrayList<String>();
		if (conn != null && stocksSymbolArray != null && stocksSymbolArray.size() > 0) {
			try {
				Statement stmt = conn.createStatement();
				String openSql = "SELECT InstrumentToken, tradingsymbol FROM " + quoteTable
						+ "_instrumentdetails where tradingsymbol in (";

				Object[] symbolKeys = stocksSymbolArray.keySet().toArray();
				for (int i = 0; i < symbolKeys.length; i++) {
					openSql = openSql + "'" + (String) symbolKeys[i] + "',";
				}
				openSql = openSql.substring(0, openSql.length() - 2) + ")";
				ResultSet openRs = stmt.executeQuery(openSql);

				while (openRs.next()) {
					instrumentList.add(openRs.getLong(1));
					tradingSymbols.add(openRs.getString(2));
				}

				openSql = "INSERT INTO " + quoteTable + "_priority " + "(Time, InstrumentToken, PriorityPoint) "
						+ "values(?,?,?)";
				PreparedStatement prepStmt = conn.prepareStatement(openSql);
				for (int index = 0; index < instrumentList.size(); index++) {

					prepStmt.setTimestamp(1, new Timestamp(new Date().getTime()));
					prepStmt.setString(2, Long.toString(instrumentList.get(index)));
					prepStmt.setDouble(3, stocksSymbolArray.get(tradingSymbols.get(index)));
					prepStmt.executeUpdate();
				}
				stmt.close();
			} catch (SQLException e) {
				LOGGER.info(
						"StreamingQuoteStorageImpl.getInstrumentTokenIdsFromSymbols(): ERROR: SQLException on fetching data from Table, cause: "
								+ e.getMessage());
			}
		} else {
			LOGGER.info("StreamingQuoteStorageImpl.getInstrumentTokenIdsFromSymbols(): ERROR: DB conn is null !!!");
		}

		return instrumentList;

	}

	@Override
	public Map<Long, String> calculateSignalsFromStrategyParams(ArrayList<Long> instrumentList) {
		Map<Long, String> signalList = new HashMap<Long, String>();

		if (conn != null && instrumentList != null && instrumentList.size() > 0) {
			Statement stmt;

			try {
				stmt = conn.createStatement();

				String openSql;
				for (int count = 0; count < instrumentList.size(); count++) {
					openSql = "SELECT trend,rSI,strategySignal FROM " + quoteTable
							+ "_SignalParams where InstrumentToken ='" + instrumentList.get(count)
							+ "' ORDER BY Time DESC LIMIT 1 ";

					ResultSet openRs3 = stmt.executeQuery(openSql);

					if (openRs3.next()) {
						if (openRs3.getInt("trend") == 2 && openRs3.getDouble("rSI") < 70
								&& openRs3.getDouble("rSI") > 30 && openRs3.getDouble("strategySignal") > 0) {
							signalList.put(instrumentList.get(count), "BUY");
						} else if (openRs3.getInt("trend") == 0 && openRs3.getDouble("rSI") > 70
								&& openRs3.getDouble("rSI") < 30 && openRs3.getDouble("strategySignal") < 0) {
							signalList.put(instrumentList.get(count), "SELL");
						}
					}
				}
			} catch (SQLException e) {
				LOGGER.info(
						"StreamingQuoteStorageImpl.calculateSignalsFromStrategyParams(): ERROR: SQLException on fetching data from Table, cause: "
								+ e.getMessage());
			}
		} else {
			LOGGER.info("StreamingQuoteStorageImpl.calculateSignalsFromStrategyParams(): ERROR: DB conn is null !!!");
		}
		return signalList;
	}
}
