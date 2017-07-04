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
import com.trade.optimizer.signal.parameter.SignalContainer;

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
				sql = "CREATE TABLE " + quoteTable
						+ " (ID int NOT NULL AUTO_INCREMENT,time timestamp, InstrumentToken varchar(32),"
						+ " LastTradedPrice DECIMAL(20,4) , LastTradedQty BIGINT , AvgTradedPrice DECIMAL(20,4) , Volume BIGINT,"
						+ " BuyQty BIGINT , SellQty BIGINT , OpenPrice DECIMAL(20,4) ,  HighPrice DECIMAL(20,4) , LowPrice DECIMAL(20,4),"
						+ " ClosePrice DECIMAL(20,4) ,timestampGrp timestamp,usedForSignal varchar(32),PRIMARY KEY (ID),"
						+ "CONSTRAINT UC_quoteTable UNIQUE (time,InstrumentToken,LastTradedPrice,LastTradedQty))"
						+ " ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARSET=utf8;";
				stmt.executeUpdate(sql);
			} catch (SQLException e) {
				LOGGER.info(
						"StreamingQuoteStorageImpl.createDaysStreamingQuoteTable(): ERROR: SQLException on creating Table, cause: "
								+ e.getMessage());
			}
			try {
				sql = "CREATE TABLE " + quoteTable
						+ "_hist (ID int NOT NULL AUTO_INCREMENT,time timestamp,InstrumentToken varchar(32),"
						+ " LastTradedPrice DECIMAL(20,4), LastTradedQty BIGINT , AvgTradedPrice DECIMAL(20,4) , Volume BIGINT , "
						+ " BuyQty BIGINT, SellQty BIGINT, OpenPrice DECIMAL(20,4), HighPrice DECIMAL(20,4), LowPrice DECIMAL(20,4), "
						+ " ClosePrice DECIMAL(20,4), TickType varchar(32), PRIMARY KEY (ID),CONSTRAINT UC_hist UNIQUE (time,InstrumentToken)) "
						+ " ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARSET=utf8;";
				stmt.executeUpdate(sql);
			} catch (SQLException e) {
				LOGGER.info(
						"StreamingQuoteStorageImpl.createDaysStreamingQuoteTable(): ERROR: SQLException on creating Table, cause: "
								+ e.getMessage());
			}
			try {
				sql = "CREATE TABLE " + quoteTable
						+ "_priority (ID int NOT NULL AUTO_INCREMENT,time timestamp, InstrumentToken varchar(32), PriorityPoint DECIMAL(20,4)  "
						+ ",PRIMARY KEY (ID),CONSTRAINT UC_priority UNIQUE (time,InstrumentToken,PriorityPoint)) ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARSET=utf8;";
				stmt.executeUpdate(sql);
			} catch (SQLException e) {
				LOGGER.info(
						"StreamingQuoteStorageImpl.createDaysStreamingQuoteTable(): ERROR: SQLException on creating Table, cause: "
								+ e.getMessage());
			}
			try {
				sql = "CREATE TABLE " + quoteTable
						+ "_Signal (ID int NOT NULL AUTO_INCREMENT,time timestamp, InstrumentToken varchar(32) , "
						+ " Quantity varchar(32) , ProcessSignal varchar(32) , Status varchar(32) ,PRIMARY KEY (ID),"
						+ "TradePrice DECIMAL(20,4), CONSTRAINT UC_signal UNIQUE (time,InstrumentToken,ProcessSignal,Status)) "
						+ " ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARSET=utf8;";
				stmt.executeUpdate(sql);
			} catch (SQLException e) {
				LOGGER.info(
						"StreamingQuoteStorageImpl.createDaysStreamingQuoteTable(): ERROR: SQLException on creating Table, cause: "
								+ e.getMessage());
			}
			try {
				sql = "CREATE TABLE " + quoteTable + "_instrumentDetails "
						+ "(ID int NOT NULL AUTO_INCREMENT,time timestamp, instrumentToken varchar(32) ,exchangeToken varchar(32) ,"
						+ "tradingsymbol varchar(32) ,name varchar(32) , last_price varchar(32) ,tickSize varchar(32), expiry varchar(32),"
						+ "instrumentType varchar(32), segment varchar(32) ,exchange varchar(32), strike varchar(32) ,lotSize varchar(32) ,"
						+ " PRIMARY KEY (ID),CONSTRAINT UC_instrumentDetails UNIQUE (instrumentToken,tradingsymbol)) ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARSET=utf8;";
				stmt.executeUpdate(sql);

			} catch (SQLException e) {
				LOGGER.info(
						"StreamingQuoteStorageImpl.createDaysStreamingQuoteTable(): ERROR: SQLException on creating Table, cause: "
								+ e.getMessage());
			}
			try {
				sql = "CREATE TABLE " + quoteTable
						+ "_signalParams (ID int NOT NULL AUTO_INCREMENT,time timestamp, instrumentToken varchar(32), high DECIMAL(26,11) ,"
						+ " low DECIMAL(26,11) ,close DECIMAL(26,11) ,pSar DECIMAL(26,11) , eP DECIMAL(26,11) ,eP_pSar DECIMAL(26,11) ,"
						+ "accFactor DECIMAL(26,11) ,eP_pSarXaccFactor DECIMAL(26,11) ,trend DECIMAL(26,11) ,upMove DECIMAL(26,11) ,"
						+ "downMove DECIMAL(26,11) ,avgUpMove DECIMAL(26,11) , avgDownMove DECIMAL(26,11) ,relativeStrength DECIMAL(26,11),"
						+ "RSI DECIMAL(26,11) ,fastEma DECIMAL(26,11) , slowEma DECIMAL(26,11) ,difference DECIMAL(26,11) ,"
						+ " strategySignal  DECIMAL(26,11) ,timestampGrp timestamp,PRIMARY KEY (ID)) ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARSET=utf8;";

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
						e.printStackTrace();
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
					prepStmt.close();
				} catch (Exception e) {
					e.printStackTrace();
				}

			} catch (SQLException e) {
				e.printStackTrace();
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

	@SuppressWarnings("deprecation")
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
					try {
						Date date = dtTmFmt.parse(dtTmFmt.format(new Date()));
						prepStmt.setTimestamp(1, new Timestamp(date.getTime()));
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
						date.setSeconds(0);
						prepStmt.setTimestamp(13, new Timestamp(date.getTime()));
						prepStmt.setString(14, "");
						prepStmt.executeUpdate();
					} catch (SQLException | ParseException e) {
						LOGGER.info(
								"StreamingQuoteStorageImpl.storeData(): ERROR: SQLException on Storing data to Table: "
										+ tick);
						LOGGER.info("StreamingQuoteStorageImpl.storeData(): [SQLException Cause]: " + e.getMessage());
					}
				}
				prepStmt.close();
			} catch (SQLException e) {
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
				stmt.close();
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
				String openSql = "SELECT InstrumentToken,ProcessSignal,Quantity,Time,id,TradePrice FROM " + quoteTable
						+ "_Signal where status ='active'";
				ResultSet openRs = stmt.executeQuery(openSql);
				List<Integer> idList = new ArrayList<Integer>();

				while (openRs.next()) {
					Order order = new Order();
					order.symbol = String.valueOf(openRs.getString(1));
					order.transactionType = String.valueOf(openRs.getString(2));
					order.quantity = String.valueOf(openRs.getString(3));
					order.price = String.valueOf(openRs.getString(6));
					idList.add(openRs.getInt(5));
					orders.add(order);
				}
				if (null != idList && idList.size() > 0) {
					stmt = conn.createStatement();
					openSql = "update " + quoteTable + "_Signal set status ='orderPlaced' where id in("
							+ commaSeperatedIDs(idList) + ")";
					stmt.executeUpdate(openSql);
				}
				stmt.close();
			} catch (SQLException e) {
				LOGGER.info(
						"StreamingQuoteStorageImpl.getOrderListToPlace(): ERROR: SQLException on fetching data from Table, cause: "
								+ e.getMessage());
			}
		} else

		{
			LOGGER.info("StreamingQuoteStorageImpl.getOrderListToPlace(): ERROR: DB conn is null !!!");
		}
		return orders;
	}

	private String commaSeperatedIDs(List<Integer> idList) {
		String ids = String.valueOf(idList.get(0));

		for (int i = 1; i < idList.size(); i++)
			ids = ids + "," + idList.get(i);
		return ids;
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
						+ "(time,instrumentToken,quantity,processSignal,status,TradePrice) " + "values(?,?,?,?,?,?)";
				PreparedStatement prepStmt = conn.prepareStatement(sql);
				if (null != instrumentList && instrumentList.size() > 0 && null != signalList
						&& signalList.size() > 0) {
					for (int index = 0; index < instrumentList.size(); index++) {
						String signalArray = signalList.get(instrumentList.get(index));
						if (null != instrumentList.get(index) && null != signalArray
								&& updateOldSignalInSignalTable(instrumentList.get(index).toString(), signalArray)) {
							String[] signalAndPrice = signalArray.split(",");
							prepStmt.setTimestamp(1, new Timestamp(new Date().getTime()));
							prepStmt.setString(2, instrumentList.get(index).toString());
							prepStmt.setString(3, "0");
							prepStmt.setString(4, signalAndPrice[0]);
							prepStmt.setString(5, "active");
							prepStmt.setDouble(6, Double.parseDouble(signalAndPrice[1]));
							prepStmt.executeUpdate();
						}
					}
				}
				prepStmt.close();
			} catch (SQLException e) {
				e.printStackTrace();
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
				String[] signalAndPrice = processSignal.split(",");
				Statement stmt = conn.createStatement();
				String openSql = "SELECT id FROM " + quoteTable
						+ "_Signal where status ='active' and InstrumentToken= '" + instrument + "' and processSignal='"
						+ signalAndPrice[0] + "'";
				ResultSet openRs = stmt.executeQuery(openSql);
				if (openRs.next())
					return false;

				openSql = "SELECT id FROM " + quoteTable + "_Signal where status ='active' and InstrumentToken= '"
						+ instrument + "'";
				openRs = stmt.executeQuery(openSql);
				ArrayList<Integer> idList = new ArrayList<Integer>();
				while (openRs.next()) {
					idList.add(openRs.getInt(1));
				}
				if (null != idList && idList.size() > 0) {
					stmt = conn.createStatement();
					openSql = "update " + quoteTable + "_Signal set status ='timeOut' where id in("
							+ commaSeperatedIDs(idList) + ")";
					stmt.executeUpdate(openSql);
				}
				stmt.close();
			} catch (SQLException e) {
				LOGGER.info(
						"StreamingQuoteStorageImpl.updateOldSignalInSignalTable(): ERROR: SQLException on fetching data from Table, cause: "
								+ e.getMessage());
			}
		} else {
			LOGGER.info("StreamingQuoteStorageImpl.updateOldSignalInSignalTable(): ERROR: DB conn is null !!!");
		}
		return true;
	}

	@SuppressWarnings("deprecation")
	@Override
	public void calculateAndStoreStrategySignalParameters(String instrumentToken, Date endingTimeToMatch) {

		if (conn != null) {
			try {
				Date endDateTime = dtTmFmt.parse(dtTmFmt.format(endingTimeToMatch));
				endDateTime.setSeconds(0);
				Timestamp endTime = new Timestamp(endDateTime.getTime());

				ArrayList<Timestamp> timeStampPeriodList = new ArrayList<Timestamp>();
				ArrayList<Integer> idsList = new ArrayList<Integer>();

				Statement timeStampRsStmt = conn.createStatement();

				String openSql = "SELECT id FROM " + quoteTable + " where InstrumentToken ='" + instrumentToken
						+ "' and usedForSignal != 'used' and timestampGrp <'" + endTime + "' ORDER BY Time ASC ";
				ResultSet timeStampRs = timeStampRsStmt.executeQuery(openSql);
				while (timeStampRs.next()) {
					idsList.add(timeStampRs.getInt(1));
				}
				if (null != idsList && idsList.size() > 0) {
					openSql = "SELECT distinct timestampGrp FROM " + quoteTable + " where id in("
							+ commaSeperatedIDs(idsList) + ") ORDER BY Time ASC ";
					timeStampRs = timeStampRsStmt.executeQuery(openSql);
					while (timeStampRs.next()) {
						timeStampPeriodList.add(timeStampRs.getTimestamp("timestampGrp"));
					}
				}
				if (null != idsList && idsList.size() > 0) {
					openSql = "update " + quoteTable + " set usedForSignal ='used' where id in("
							+ commaSeperatedIDs(idsList) + ")";
					timeStampRsStmt.executeUpdate(openSql);
				}
				timeStampRsStmt.close();

				for (int timeLoop = 0; timeLoop < timeStampPeriodList.size(); timeLoop++) {
					Double low = null;
					Double high = null;
					Double close = null;

					Statement timeLoopRsStmt = conn.createStatement();

					openSql = "SELECT * FROM " + quoteTable + " where InstrumentToken ='" + instrumentToken
							+ "' and timestampGrp ='" + timeStampPeriodList.get(timeLoop) + "' ORDER BY Time DESC ";
					ResultSet openRsHighLowClose = timeLoopRsStmt.executeQuery(openSql);
					boolean firstRecord = true;
					while (openRsHighLowClose.next()) {
						if (null == low || openRsHighLowClose.getDouble("LastTradedPrice") < low)
							low = openRsHighLowClose.getDouble("LastTradedPrice");
						if (null == high || openRsHighLowClose.getDouble("LastTradedPrice") > high)
							high = openRsHighLowClose.getDouble("LastTradedPrice");
						if (firstRecord) {
							close = openRsHighLowClose.getDouble("LastTradedPrice");
							firstRecord = false;
						}
					}

					openSql = "SELECT * FROM " + quoteTable + "_SignalParams where InstrumentToken ='" + instrumentToken
							+ "' ORDER BY Time DESC LIMIT 1 ";
					ResultSet openRsSignalParams = timeLoopRsStmt.executeQuery(openSql);

					SignalContainer signalContainer = null;

					if (openRsSignalParams.next()) {
						if (StreamingConfig.MAX_VALUE != openRsSignalParams.getDouble("strategySignal")) {
							signalContainer = this.whenPsarRsiMacdAll3sPreviousSignalsAvailable(openRsSignalParams, low,
									high, close);

						} else if (StreamingConfig.MAX_VALUE != openRsSignalParams.getDouble("RSI")) {
							signalContainer = this.whenPsarRsiPreviousSignalsAvailableButNotMacd(instrumentToken, low,
									high, close);

						} else if (StreamingConfig.MAX_VALUE != openRsSignalParams.getDouble("pSar")
								&& StreamingConfig.MAX_VALUE != openRsSignalParams.getDouble("eP")
								&& StreamingConfig.MAX_VALUE != openRsSignalParams.getDouble("eP_pSarXaccFactor")) {
							signalContainer = this.whenPsarPreviousSignalsAvailableButNotRsiAndMacd(instrumentToken,
									low, high, close);
						}

					} else {
						signalContainer = this.whenPsarRsiMacdAll3sPreviousSignalsNOTAvailable(instrumentToken, low,
								high, close);
					}
					String sql = "INSERT INTO " + quoteTable + "_signalParams "
							+ "(Time, InstrumentToken, high,low,close,pSar,eP,eP_pSar,accFactor,eP_pSarXaccFactor,trend,"
							+ "upMove,downMove,avgUpMove,avgDownMove,relativeStrength,RSI,fastEma,slowEma,difference,strategySignal,timestampGrp) "
							+ "values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)";
					PreparedStatement prepStmtInsertSignalParams = conn.prepareStatement(sql);

					PSarSignalParam p = signalContainer.p1;
					RSISignalParam r = signalContainer.r1;
					MACDSignalParam m = signalContainer.m1;

					prepStmtInsertSignalParams.setTimestamp(1,
							new Timestamp(dtTmFmt.parse(dtTmFmt.format(new Date())).getTime()));
					prepStmtInsertSignalParams.setTimestamp(22, timeStampPeriodList.get(timeLoop));
					prepStmtInsertSignalParams.setString(2, instrumentToken);
					prepStmtInsertSignalParams.setDouble(3, p.getHigh());
					prepStmtInsertSignalParams.setDouble(4, p.getLow());
					prepStmtInsertSignalParams.setDouble(5, close);
					prepStmtInsertSignalParams.setDouble(6, p.getpSar());
					prepStmtInsertSignalParams.setDouble(7, p.geteP());
					prepStmtInsertSignalParams.setDouble(8, p.geteP_pSar());
					prepStmtInsertSignalParams.setDouble(9, p.getAccFactor());
					prepStmtInsertSignalParams.setDouble(10, p.geteP_pSarXaccFactor());
					prepStmtInsertSignalParams.setInt(11, p.getTrend());
					prepStmtInsertSignalParams.setDouble(12, r.getUpMove());
					prepStmtInsertSignalParams.setDouble(13, r.getDownMove());
					prepStmtInsertSignalParams.setDouble(14, r.getAvgUpMove());
					prepStmtInsertSignalParams.setDouble(15, r.getAvgDownMove());
					prepStmtInsertSignalParams.setDouble(16, r.getRelativeStrength());
					prepStmtInsertSignalParams.setDouble(17, r.getRSI());
					prepStmtInsertSignalParams.setDouble(18, m.getFastEma());
					prepStmtInsertSignalParams.setDouble(19, m.getSlowEma());
					prepStmtInsertSignalParams.setDouble(20, m.getDifference());
					prepStmtInsertSignalParams.setDouble(21, m.getSignal());
					prepStmtInsertSignalParams.executeUpdate();

					prepStmtInsertSignalParams.close();
					timeLoopRsStmt.close();
				}
			} catch (SQLException | ParseException e) {
				LOGGER.info(
						"StreamingQuoteStorageImpl.calculateAndStoreStrategySignalParameters(): ERROR: SQLException on fetching data from Table, cause: "
								+ e.getMessage());
				e.printStackTrace();
			}
		} else {
			LOGGER.info(
					"StreamingQuoteStorageImpl.calculateAndStoreStrategySignalParameters(): ERROR: DB conn is null !!!");
		}
	}

	private SignalContainer whenPsarRsiMacdAll3sPreviousSignalsNOTAvailable(String instrumentToken, Double low,
			Double high, Double close) throws SQLException {

		String openSql = "SELECT * FROM " + quoteTable + "_SignalParams where InstrumentToken ='" + instrumentToken
				+ "' ORDER BY Time DESC LIMIT 35 ";
		SignalContainer signalContainer = new SignalContainer();
		Statement stmt = conn.createStatement();
		ResultSet openRs = stmt.executeQuery(openSql);
		List<MACDSignalParam> macdSignalParamList = new ArrayList<MACDSignalParam>();
		List<RSISignalParam> rsiSignalParamList = new ArrayList<RSISignalParam>();
		MACDSignalParam macdSignalParam;
		RSISignalParam rsiSignalParam;
		boolean firstLoop = true;
		if (firstLoop) {
			signalContainer.p1 = new PSarSignalParam(high, low);
			firstLoop = false;
		}
		while (openRs.next()) {
			macdSignalParam = new MACDSignalParam();
			rsiSignalParam = new RSISignalParam();

			macdSignalParam.setClose(openRs.getDouble("close"));
			macdSignalParam.setDifference(openRs.getDouble("difference"));
			macdSignalParam.setFastEma(openRs.getDouble("fastEma"));
			macdSignalParam.setSlowEma(openRs.getDouble("slowEma"));
			macdSignalParam.setSignal(openRs.getDouble("strategySignal"));
			macdSignalParamList.add(macdSignalParam);

			rsiSignalParam.setClose(openRs.getDouble("close"));
			rsiSignalParam.setDownMove(openRs.getDouble("downMove"));
			rsiSignalParam.setUpMove(openRs.getDouble("upMove"));
			rsiSignalParam.setAvgDownMove(openRs.getDouble("avgDownMove"));
			rsiSignalParam.setAvgDownMove(openRs.getDouble("avgDownMove"));
			rsiSignalParam.setRelativeStrength(openRs.getDouble("relativeStrength"));
			rsiSignalParam.setRSI(openRs.getDouble("rSI"));
			rsiSignalParamList.add(rsiSignalParam);
		}
		stmt.close();
		signalContainer.r1 = new RSISignalParam(rsiSignalParamList, close);
		signalContainer.m1 = new MACDSignalParam(macdSignalParamList, close);
		return signalContainer;
	}

	private SignalContainer whenPsarPreviousSignalsAvailableButNotRsiAndMacd(String instrumentToken, Double low,
			Double high, Double close) throws SQLException {

		String openSql = "SELECT * FROM " + quoteTable + "_SignalParams where InstrumentToken ='" + instrumentToken
				+ "' ORDER BY Time DESC LIMIT 35 ";
		SignalContainer signalContainer = new SignalContainer();

		Statement stmt = conn.createStatement();
		ResultSet openRs = stmt.executeQuery(openSql);
		List<MACDSignalParam> macdSignalParamList = new ArrayList<MACDSignalParam>();
		List<RSISignalParam> rsiSignalParamList = new ArrayList<RSISignalParam>();
		MACDSignalParam macdSignalParam;
		RSISignalParam rsiSignalParam;
		boolean firstLoop = true;
		while (openRs.next()) {
			if (firstLoop) {
				signalContainer.p1 = new PSarSignalParam(high, low, openRs.getDouble("pSar"), openRs.getDouble("eP"),
						openRs.getDouble("eP_pSar"), openRs.getDouble("accFactor"),
						openRs.getDouble("eP_pSarXaccFactor"), openRs.getInt("trend"));
				firstLoop = false;
			}
			macdSignalParam = new MACDSignalParam();
			rsiSignalParam = new RSISignalParam();

			macdSignalParam.setClose(openRs.getDouble("close"));
			macdSignalParam.setDifference(openRs.getDouble("difference"));
			macdSignalParam.setFastEma(openRs.getDouble("fastEma"));
			macdSignalParam.setSlowEma(openRs.getDouble("slowEma"));
			macdSignalParam.setSignal(openRs.getDouble("strategySignal"));
			macdSignalParamList.add(macdSignalParam);

			rsiSignalParam.setClose(openRs.getDouble("close"));
			rsiSignalParam.setDownMove(openRs.getDouble("downMove"));
			rsiSignalParam.setUpMove(openRs.getDouble("upMove"));
			rsiSignalParam.setAvgDownMove(openRs.getDouble("avgDownMove"));
			rsiSignalParam.setAvgDownMove(openRs.getDouble("avgDownMove"));
			rsiSignalParam.setRelativeStrength(openRs.getDouble("relativeStrength"));
			rsiSignalParam.setRSI(openRs.getDouble("rSI"));
			rsiSignalParamList.add(rsiSignalParam);
		}
		stmt.close();
		signalContainer.r1 = new RSISignalParam(rsiSignalParamList, close);
		signalContainer.m1 = new MACDSignalParam(macdSignalParamList, close);
		return signalContainer;
	}

	private SignalContainer whenPsarRsiPreviousSignalsAvailableButNotMacd(String instrumentToken, Double low,
			Double high, Double close) throws SQLException {
		String openSql = "SELECT * FROM " + quoteTable + "_SignalParams where InstrumentToken ='" + instrumentToken
				+ "' ORDER BY Time DESC LIMIT 35 ";
		SignalContainer signalContainer = new SignalContainer();
		Statement stmt = conn.createStatement();
		ResultSet openRs = stmt.executeQuery(openSql);
		List<MACDSignalParam> macdSignalParamList = new ArrayList<MACDSignalParam>();
		MACDSignalParam macdSignalParam;
		boolean firstLoop = true;

		while (openRs.next()) {
			if (firstLoop) {
				signalContainer.p1 = new PSarSignalParam(high, low, openRs.getDouble("pSar"), openRs.getDouble("eP"),
						openRs.getDouble("eP_pSar"), openRs.getDouble("accFactor"),
						openRs.getDouble("eP_pSarXaccFactor"), openRs.getInt("trend"));
				signalContainer.r1 = new RSISignalParam(openRs.getDouble("close"), close, openRs.getDouble("upMove"),
						openRs.getDouble("downMove"), openRs.getDouble("avgUpMove"), openRs.getDouble("avgDownMove"),
						openRs.getDouble("relativeStrength"), openRs.getDouble("rSI"));
				firstLoop = false;
			}
			macdSignalParam = new MACDSignalParam();
			macdSignalParam.setClose(openRs.getDouble("close"));
			macdSignalParam.setDifference(openRs.getDouble("difference"));
			macdSignalParam.setFastEma(openRs.getDouble("fastEma"));
			macdSignalParam.setSlowEma(openRs.getDouble("slowEma"));
			macdSignalParam.setSignal(openRs.getDouble("strategySignal"));
			macdSignalParamList.add(macdSignalParam);
		}
		stmt.close();
		signalContainer.m1 = new MACDSignalParam(macdSignalParamList, close);
		return signalContainer;
	}

	private SignalContainer whenPsarRsiMacdAll3sPreviousSignalsAvailable(ResultSet openRs, Double low, Double high,
			Double close) throws SQLException {
		SignalContainer signalContainer = new SignalContainer();
		signalContainer.p1 = new PSarSignalParam(high, low, openRs.getDouble("pSar"), openRs.getDouble("eP"),
				openRs.getDouble("eP_pSar"), openRs.getDouble("accFactor"), openRs.getDouble("eP_pSarXaccFactor"),
				openRs.getInt("trend"));
		signalContainer.r1 = new RSISignalParam(openRs.getDouble("close"), close, openRs.getDouble("upMove"),
				openRs.getDouble("downMove"), openRs.getDouble("avgUpMove"), openRs.getDouble("avgDownMove"),
				openRs.getDouble("relativeStrength"), openRs.getDouble("rSI"));
		signalContainer.m1 = new MACDSignalParam(close, openRs.getDouble("fastEma"), openRs.getDouble("slowEma"),
				openRs.getDouble("strategySignal"));
		return signalContainer;
	}

	@Override
	public ArrayList<String> getInstrumentTokenIdsFromSymbols(Map<String, Double> stocksSymbolArray) {

		ArrayList<String> instrumentList = new ArrayList<String>();
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
				openSql = openSql.substring(0, openSql.length() - 1) + ")";
				ResultSet openRs = stmt.executeQuery(openSql);

				while (openRs.next()) {
					instrumentList.add(openRs.getString(1));
					tradingSymbols.add(openRs.getString(2));
				}

				openSql = "INSERT INTO " + quoteTable + "_priority " + "(Time, InstrumentToken, PriorityPoint) "
						+ "values(?,?,?)";
				PreparedStatement prepStmt = conn.prepareStatement(openSql);
				for (int index = 0; index < instrumentList.size(); index++) {

					prepStmt.setTimestamp(1, new Timestamp(new Date().getTime()));
					prepStmt.setString(2, instrumentList.get(index));
					prepStmt.setDouble(3, stocksSymbolArray.get(tradingSymbols.get(index)));
					prepStmt.executeUpdate();
				}
				prepStmt.close();
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
					openSql = "SELECT trend,rSI,difference,strategySignal,close FROM " + quoteTable
							+ "_SignalParams where InstrumentToken ='" + instrumentList.get(count)
							+ "' ORDER BY Time DESC LIMIT 2 ";

					ResultSet openRs = stmt.executeQuery(openSql);
					boolean firstDataSet = true;
					Double macdSignalCurr = 0.0;
					Integer trend = 1;
					Double rsi = 0.0;
					Double price = 0.0;
					while (openRs.next()) {
						if (StreamingConfig.MAX_VALUE != openRs.getDouble("rSI")
								&& StreamingConfig.MAX_VALUE != openRs.getDouble("strategySignal")) {
							Double macdSignalPrev = openRs.getDouble("difference") - openRs.getDouble("strategySignal");
							if (firstDataSet) {
								macdSignalCurr = macdSignalPrev;
								trend = openRs.getInt("trend");
								rsi = openRs.getDouble("rSI");
								price = openRs.getDouble("close");
								firstDataSet = false;
							} else if (!firstDataSet) {
								if (trend == 2 && rsi < 70 && rsi > 30
										&& (macdSignalCurr > 0.0 && macdSignalPrev < 0.0)) {
									signalList.put(instrumentList.get(count), "BUY," + price);
								} else if (trend == 0 && rsi > 70 && rsi < 30
										&& (macdSignalCurr < 0.0 && macdSignalPrev > 0.0)) {
									signalList.put(instrumentList.get(count), "SELL," + price);
								}
							}
						}
					}
				}
				stmt.close();
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
