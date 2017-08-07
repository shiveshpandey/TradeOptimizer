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
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;
import java.util.logging.Logger;

import com.streamquote.utils.StreamingConfig;
import com.trade.optimizer.models.Instrument;
import com.trade.optimizer.models.InstrumentVolatilityScore;
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
	private Connection conn = null;

	private static String quoteTable = null;

	@Override
	public void initializeJDBCConn() {
		// LOGGER.info("Entry StreamingQuoteStorageImpl.initializeJDBCConn");
		try {
			TimeZone.setDefault(TimeZone.getTimeZone("IST"));
			Class.forName(JDBC_DRIVER);
			conn = DriverManager.getConnection(DB_URL, USER, PASS);
			Statement timeLoopRsStmt = conn.createStatement();
			timeLoopRsStmt.executeQuery("SET GLOBAL time_zone = '+5:30'");
			timeLoopRsStmt.close();
			quoteTable = StreamingConfig.getStreamingQuoteTbNameAppendFormat(
					new SimpleDateFormat("ddMMyyyy").format(Calendar.getInstance().getTime()));

		} catch (ClassNotFoundException e) {
			LOGGER.info("StreamingQuoteStorageImpl.initializeJDBCConn(): ClassNotFoundException: " + JDBC_DRIVER);
		} catch (SQLException e) {
			LOGGER.info("StreamingQuoteStorageImpl.initializeJDBCConn(): SQLException on getConnection");
		}
		// LOGGER.info("Exit StreamingQuoteStorageImpl.initializeJDBCConn");
	}

	@Override
	public void closeJDBCConn() {
		// LOGGER.info("Entry StreamingQuoteStorageImpl.closeJDBCConn");
		if (conn != null) {
			try {
				conn.close();
			} catch (SQLException e) {
				LOGGER.info("StreamingQuoteStorageImpl.closeJDBCConn(): SQLException on conn close");
			}
		} else {
			LOGGER.info("StreamingQuoteStorageImpl.closeJDBCConn(): WARNING: DB connection already null");
		}
		// LOGGER.info("Exit StreamingQuoteStorageImpl.closeJDBCConn");
	}

	@Override
	public void createDaysStreamingQuoteTable() throws SQLException {
		// LOGGER.info("Entry
		// StreamingQuoteStorageImpl.createDaysStreamingQuoteTable");
		if (conn != null) {
			Statement stmt = conn.createStatement();
			String sql;
			try {
				sql = "CREATE TABLE " + quoteTable
						+ "_ReadyFlag (backendReadyFlag int,time timestamp) ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARSET=utf8;";
				stmt.executeUpdate(sql);
			} catch (SQLException e) {
				LOGGER.info(
						"StreamingQuoteStorageImpl.createDaysStreamingQuoteTable(): ERROR: SQLException on creating Table, cause: "
								+ e.getMessage() + ">>" + e.getCause());
			}
			try {
				sql = "CREATE TABLE " + quoteTable
						+ " (ID int NOT NULL AUTO_INCREMENT,time timestamp, InstrumentToken varchar(32),"
						+ " LastTradedPrice DECIMAL(20,4) , LastTradedQty BIGINT , AvgTradedPrice DECIMAL(20,4) , Volume BIGINT,"
						+ " BuyQty BIGINT , SellQty BIGINT , OpenPrice DECIMAL(20,4) ,  HighPrice DECIMAL(20,4) , LowPrice DECIMAL(20,4),"
						+ " ClosePrice DECIMAL(20,4) ,timestampGrp timestamp,usedForSignal varchar(32),PRIMARY KEY (ID)"
						+ ") ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARSET=utf8;";
				stmt.executeUpdate(sql);
			} catch (SQLException e) {
				LOGGER.info(
						"StreamingQuoteStorageImpl.createDaysStreamingQuoteTable(): ERROR: SQLException on creating Table, cause: "
								+ e.getMessage() + ">>" + e.getCause());
			}
			try {
				sql = "CREATE TABLE " + quoteTable
						+ "_Signal (ID int NOT NULL AUTO_INCREMENT,time timestamp, InstrumentToken varchar(32) , "
						+ " Quantity varchar(32) , ProcessSignal varchar(32) , Status varchar(32) ,PRIMARY KEY (ID),"
						+ "TradePrice DECIMAL(20,4)) " + " ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARSET=utf8;";
				stmt.executeUpdate(sql);
			} catch (SQLException e) {
				LOGGER.info(
						"StreamingQuoteStorageImpl.createDaysStreamingQuoteTable(): ERROR: SQLException on creating Table, cause: "
								+ e.getMessage() + ">>" + e.getCause());
			}
			try {
				sql = "CREATE TABLE " + quoteTable + "_instrumentDetails "
						+ "(ID int NOT NULL AUTO_INCREMENT,time timestamp, instrumentToken varchar(32),dailyVolatility DECIMAL(20,4),"
						+ "annualVolatility DECIMAL(20,4),currentVolatility DECIMAL(20,4),PriorityPoint DECIMAL(20,4),tradable varchar(32),exchangeToken varchar(32),"
						+ "tradingsymbol varchar(32) ,name varchar(32) , last_price varchar(32) ,tickSize varchar(32), expiry varchar(32),"
						+ "instrumentType varchar(32), segment varchar(32) ,exchange varchar(32), strike varchar(32) ,lotSize varchar(32) ,"
						+ " PRIMARY KEY (ID)) ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARSET=utf8;";
				stmt.executeUpdate(sql);

			} catch (SQLException e) {
				LOGGER.info(
						"StreamingQuoteStorageImpl.createDaysStreamingQuoteTable(): ERROR: SQLException on creating Table, cause: "
								+ e.getMessage() + ">>" + e.getCause());
			}
			try {
				sql = "CREATE TABLE " + quoteTable
						+ "_SignalNew (ID int NOT NULL AUTO_INCREMENT,time timestamp, InstrumentToken varchar(32) , "
						+ " Quantity varchar(32) , ProcessSignal varchar(32) , Status varchar(32) ,PRIMARY KEY (ID),"
						+ "TradePrice DECIMAL(20,4),signalParamKey int) "
						+ " ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARSET=utf8;";
				stmt.executeUpdate(sql);
			} catch (SQLException e) {
				LOGGER.info(
						"StreamingQuoteStorageImpl.createDaysStreamingQuoteTable(): ERROR: SQLException on creating Table, cause: "
								+ e.getMessage() + ">>" + e.getCause());
			}
			try {
				sql = "CREATE TABLE " + quoteTable
						+ "_SignalNew1 (ID int NOT NULL AUTO_INCREMENT,time timestamp, InstrumentToken varchar(32) , "
						+ " Quantity varchar(32) , ProcessSignal varchar(32) , Status varchar(32) ,PRIMARY KEY (ID),"
						+ "TradePrice DECIMAL(20,4),signalParamKey int) "
						+ " ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARSET=utf8;";
				stmt.executeUpdate(sql);
			} catch (SQLException e) {
				LOGGER.info(
						"StreamingQuoteStorageImpl.createDaysStreamingQuoteTable(): ERROR: SQLException on creating Table, cause: "
								+ e.getMessage() + ">>" + e.getCause());
			}
			try {
				sql = "CREATE TABLE " + quoteTable
						+ "_SignalNew2 (ID int NOT NULL AUTO_INCREMENT,time timestamp, InstrumentToken varchar(32) , "
						+ " Quantity varchar(32) , ProcessSignal varchar(32) , Status varchar(32) ,PRIMARY KEY (ID),"
						+ "TradePrice DECIMAL(20,4),signalParamKey int) "
						+ " ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARSET=utf8;";
				stmt.executeUpdate(sql);
			} catch (SQLException e) {
				LOGGER.info(
						"StreamingQuoteStorageImpl.createDaysStreamingQuoteTable(): ERROR: SQLException on creating Table, cause: "
								+ e.getMessage() + ">>" + e.getCause());
			}
			try {
				sql = "CREATE TABLE " + quoteTable
						+ "_SignalNew3 (ID int NOT NULL AUTO_INCREMENT,time timestamp, InstrumentToken varchar(32) , "
						+ " Quantity varchar(32) , ProcessSignal varchar(32) , Status varchar(32) ,PRIMARY KEY (ID),"
						+ "TradePrice DECIMAL(20,4),signalParamKey int) "
						+ " ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARSET=utf8;";
				stmt.executeUpdate(sql);
			} catch (SQLException e) {
				LOGGER.info(
						"StreamingQuoteStorageImpl.createDaysStreamingQuoteTable(): ERROR: SQLException on creating Table, cause: "
								+ e.getMessage() + ">>" + e.getCause());
			}
			try {
				sql = "CREATE TABLE " + quoteTable
						+ "_SignalNew4 (ID int NOT NULL AUTO_INCREMENT,time timestamp, InstrumentToken varchar(32) , "
						+ " Quantity varchar(32) , ProcessSignal varchar(32) , Status varchar(32) ,PRIMARY KEY (ID),"
						+ "TradePrice DECIMAL(20,4),signalParamKey int) "
						+ " ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARSET=utf8;";
				stmt.executeUpdate(sql);
			} catch (SQLException e) {
				LOGGER.info(
						"StreamingQuoteStorageImpl.createDaysStreamingQuoteTable(): ERROR: SQLException on creating Table, cause: "
								+ e.getMessage() + ">>" + e.getCause());
			}
			try {
				sql = "CREATE TABLE " + quoteTable
						+ "_signalParams (ID int NOT NULL AUTO_INCREMENT,time timestamp, instrumentToken varchar(32), high DECIMAL(26,11) ,"
						+ " low DECIMAL(26,11) ,close DECIMAL(26,11) ,pSar DECIMAL(26,11) , eP DECIMAL(26,11) ,eP_pSar DECIMAL(26,11) ,"
						+ "accFactor DECIMAL(26,11) ,eP_pSarXaccFactor DECIMAL(26,11) ,trend DECIMAL(26,11) ,upMove DECIMAL(26,11) ,"
						+ "downMove DECIMAL(26,11) ,avgUpMove DECIMAL(26,11) , avgDownMove DECIMAL(26,11) ,relativeStrength DECIMAL(26,11),"
						+ "RSI DECIMAL(26,11) ,fastEma DECIMAL(26,11) , slowEma DECIMAL(26,11) ,difference DECIMAL(26,11) ,"
						+ " strategySignal  DECIMAL(26,11) ,differenceMinusSignal DECIMAL(26,11) ,macdSignal DECIMAL(26,11) ,"
						+ "timestampGrp timestamp, usedForSignal varchar(32),usedForZigZagSignal varchar(32), ContinueTrackOfPsarTrend int,"
						+ " ContinueTrackOfMacdTrend int, ContinueTrackOfRsiTrend int,usedForZigZagSignal1 varchar(32),usedForZigZagSignal2 varchar(32),usedForZigZagSignal3 varchar(32),usedForZigZagSignal4 varchar(32), PRIMARY KEY (ID)) ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARSET=utf8;";

				stmt.executeUpdate(sql);

			} catch (SQLException e) {
				LOGGER.info(
						"StreamingQuoteStorageImpl.createDaysStreamingQuoteTable(): ERROR: SQLException on creating Table, cause: "
								+ e.getMessage() + ">>" + e.getCause());
			}
		} else {
			LOGGER.info("StreamingQuoteStorageImpl.createDaysStreamingQuoteTable(): ERROR: DB conn is null !!!");
		}
		// LOGGER.info("Exit
		// StreamingQuoteStorageImpl.createDaysStreamingQuoteTable");
	}

	@SuppressWarnings("deprecation")
	@Override
	public void storeData(ArrayList<Tick> ticks) {
		// LOGGER.info("Entry StreamingQuoteStorageImpl.storeData");
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
						Date date = dtTmFmt.parse(dtTmFmt.format(Calendar.getInstance().getTime()));
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
						LOGGER.info("StreamingQuoteStorageImpl.storeData(): [SQLException Cause]: " + e.getMessage()
								+ ">>" + e.getCause());
					}
				}
				prepStmt.close();
			} catch (SQLException e) {
				LOGGER.info("StreamingQuoteStorageImpl.storeData(): ERROR: SQLException on Storing data to Table: "
						+ ticks);
				LOGGER.info("StreamingQuoteStorageImpl.storeData(): [SQLException Cause]: " + e.getMessage() + ">>"
						+ e.getCause());
			}
		} else {
			if (conn != null) {
				LOGGER.info("StreamingQuoteStorageImpl.storeData(): ERROR: DB conn is null !!!");
			} else {
				LOGGER.info(
						"StreamingQuoteStorageImpl.storeData(): ERROR: quote is not of type StreamingQuoteModeQuote !!!");
			}
		}
		// LOGGER.info("Exit StreamingQuoteStorageImpl.storeData");
	}

	@Override
	public ArrayList<Long> getTopPrioritizedTokenList(int i) {
		// LOGGER.info("Entry
		// StreamingQuoteStorageImpl.getTopPrioritizedTokenList");
		ArrayList<Long> instrumentList = new ArrayList<Long>();
		if (conn != null) {
			try {
				Statement stmt = conn.createStatement();
				String openSql = "SELECT InstrumentToken FROM " + quoteTable
						+ "_instrumentDetails where tradable='tradable' and lotsize != '0' and lotsize != 'null' and (PriorityPoint > 0.0 or dailyVolatility >= 2.0) ORDER BY PriorityPoint DESC,dailyVolatility DESC,id desc LIMIT "
						+ i + "";
				ResultSet openRs = stmt.executeQuery(openSql);
				while (openRs.next()) {
					instrumentList.add(Long.valueOf(openRs.getString(1)));
				}
				stmt.close();
			} catch (SQLException e) {
				LOGGER.info(
						"StreamingQuoteStorageImpl.getTopPrioritizedTokenList(): ERROR: SQLException on fetching data from Table, cause: "
								+ e.getMessage() + ">>" + e.getCause());
			}
		} else {
			LOGGER.info("StreamingQuoteStorageImpl.getTopPrioritizedTokenList(): ERROR: DB conn is null !!!");
		}
		// LOGGER.info("Exit
		// StreamingQuoteStorageImpl.getTopPrioritizedTokenList");
		return instrumentList;
	}

	@Override
	public List<Order> getOrderListToPlace() {
		List<Order> orders = new ArrayList<Order>();
		if (conn != null) {
			try {
				Statement stmt = conn.createStatement();
				String openSql = "SELECT InstrumentToken,ProcessSignal,Quantity,Time,id,TradePrice FROM " + quoteTable
						+ "_SignalNew where status ='active'";
				ResultSet openRs = stmt.executeQuery(openSql);
				List<Integer> idList = new ArrayList<Integer>();

				while (openRs.next()) {
					Order order = new Order();
					order.tradingSymbol = fetchTradingSymbolFromInstrumentDetails(openRs.getString(1));
					order.transactionType = String.valueOf(openRs.getString(2));
					order.quantity = String.valueOf(openRs.getString(3));
					order.price = String.valueOf(openRs.getString(6));
					order.tag = String.valueOf(openRs.getInt(5));
					idList.add(openRs.getInt(5));
					orders.add(order);
				}
				if (null != idList && idList.size() > 0) {
					stmt = conn.createStatement();
					openSql = "update " + quoteTable + "_SignalNew set status ='orderPlaced' where id in("
							+ commaSeperatedIDs(idList) + ")";
					stmt.executeUpdate(openSql);
				}
				stmt.close();
			} catch (SQLException e) {
				LOGGER.info(
						"StreamingQuoteStorageImpl.getOrderListToPlace(): ERROR: SQLException on fetching data from Table, cause: "
								+ e.getMessage() + ">>" + e.getCause());
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

	private String commaSeperatedLongIDs(List<Long> idList) {
		String ids = "'" + String.valueOf(idList.get(0));

		for (int i = 1; i < idList.size(); i++)
			ids = ids + "','" + idList.get(i);
		return ids + "'";
	}

	@Override
	public void saveInstrumentDetails(List<Instrument> instrumentList, Timestamp time) {
		// LOGGER.info("Entry
		// StreamingQuoteStorageImpl.saveInstrumentDetails()");
		if (conn != null) {

			try {
				String sql = "INSERT INTO " + quoteTable + "_instrumentDetails "
						+ "(time,instrumentToken,exchangeToken,tradingsymbol,name,last_price,"
						+ "tickSize,expiry,instrumentType,segment,exchange,strike,lotSize,PriorityPoint,currentVolatility,annualVolatility) "
						+ "values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)";
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
					prepStmt.setDouble(14, 0.0);
					prepStmt.setDouble(15, 0.0);
					prepStmt.setDouble(16, 0.0);
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
		// LOGGER.info("Exit
		// StreamingQuoteStorageImpl.saveInstrumentDetails()");
	}

	@Override
	public String getInstrumentDetailsOnTradingsymbol(String tradingsymbol) {
		// LOGGER.info("Entry
		// StreamingQuoteStorageImpl.getInstrumentDetailsOnTradingsymbol()");
		String param = new String();
		if (conn != null) {
			try {
				Statement stmt = conn.createStatement();

				String openSql = "SELECT exchange FROM " + quoteTable + "_instrumentDetails WHERE tradingsymbol='"
						+ tradingsymbol + "'";
				ResultSet openRs = stmt.executeQuery(openSql);
				while (openRs.next()) {
					param = openRs.getString(1);
				}
				stmt.close();
			} catch (SQLException e) {
				LOGGER.info(
						"StreamingQuoteStorageImpl.getInstrumentDetailsOnTradingsymbol(): ERROR: SQLException on fetching data from Table, cause: "
								+ e.getMessage() + ">>" + e.getCause());
			}
		} else {
			LOGGER.info("StreamingQuoteStorageImpl.getInstrumentDetailsOnTradingsymbol(): ERROR: DB conn is null !!!");
		}
		// LOGGER.info("Exit
		// StreamingQuoteStorageImpl.getInstrumentDetailsOnTradingsymbol()");
		return param;
	}

	@Override
	public void saveGeneratedSignals(Map<Long, String> signalList, List<Long> instrumentList) {
		// LOGGER.info("Entry
		// StreamingQuoteStorageImpl.saveGeneratedSignals()");
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
							prepStmt.setTimestamp(1, new Timestamp(Calendar.getInstance().getTime().getTime()));
							prepStmt.setString(2, instrumentList.get(index).toString());
							prepStmt.setString(3, fetchLotSizeFromInstrumentDetails(
									instrumentList.get(index).toString(), signalAndPrice[0]));
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
		// LOGGER.info("Exit StreamingQuoteStorageImpl.saveGeneratedSignals()");
	}

	private String fetchLotSizeFromInstrumentDetails(String instrumentToken, String buyOrSell) throws SQLException {
		String lotSize = "";
		Statement stmt = conn.createStatement();
		int totalQuantityProcessed = 0;
		String openSql = "SELECT lotSize FROM " + quoteTable + "_instrumentDetails where instrumentToken='"
				+ instrumentToken + "'";
		ResultSet openRs = stmt.executeQuery(openSql);

		while (openRs.next()) {
			lotSize = openRs.getString(1);
		}
		openSql = "SELECT time,quantity,processSignal FROM " + quoteTable + "_SignalNew where instrumentToken='"
				+ instrumentToken + "'and status not in('REJECTED','CANCELLED') order by time desc,id desc";
		openRs = stmt.executeQuery(openSql);

		while (openRs.next()) {
			if ("BUY".equalsIgnoreCase(openRs.getString(3)))
				totalQuantityProcessed = totalQuantityProcessed + Integer.parseInt(openRs.getString(2));
			else if ("SELL".equalsIgnoreCase(openRs.getString(3)))
				totalQuantityProcessed = totalQuantityProcessed - Integer.parseInt(openRs.getString(2));
		}

		if ("BUY".equalsIgnoreCase(buyOrSell) && totalQuantityProcessed < 0)
			lotSize = (totalQuantityProcessed + "").replaceAll("-", "");
		else if ("SELL".equalsIgnoreCase(buyOrSell) && totalQuantityProcessed > 0)
			lotSize = totalQuantityProcessed + "";

		stmt.close();
		return lotSize;
	}

	private String fetchTradingSymbolFromInstrumentDetails(String instrumentToken) throws SQLException {
		String lotSize = "";
		Statement stmt = conn.createStatement();
		String openSql = "SELECT tradingsymbol FROM " + quoteTable + "_instrumentDetails where instrumentToken='"
				+ instrumentToken + "'";
		ResultSet openRs = stmt.executeQuery(openSql);

		while (openRs.next()) {
			lotSize = openRs.getString(1);
		}
		stmt.close();
		return lotSize;
	}

	private boolean updateOldSignalInSignalTable(String instrument, String processSignal) {
		// LOGGER.info("Entry
		// StreamingQuoteStorageImpl.updateOldSignalInSignalTable()");
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
								+ e.getMessage() + ">>" + e.getCause());
			}
		} else {
			LOGGER.info("StreamingQuoteStorageImpl.updateOldSignalInSignalTable(): ERROR: DB conn is null !!!");
		}
		// LOGGER.info("Exit
		// StreamingQuoteStorageImpl.updateOldSignalInSignalTable()");
		return true;
	}

	@SuppressWarnings("deprecation")
	@Override
	public void calculateAndStoreStrategySignalParameters(String instrumentToken, Date endingTimeToMatch) {
		// LOGGER.info("Entry
		// StreamingQuoteStorageImpl.calculateAndStoreStrategySignalParameters()");
		if (conn != null) {
			try {
				Date endDateTime = dtTmFmt.parse(dtTmFmt.format(endingTimeToMatch));
				endDateTime.setSeconds(0);
				Timestamp endTime = new Timestamp(endDateTime.getTime());

				ArrayList<Timestamp> timeStampPeriodList = new ArrayList<Timestamp>();
				ArrayList<Integer> idsList = new ArrayList<Integer>();

				Statement timeStampRsStmt = conn.createStatement();

				String openSql = "SELECT id FROM " + quoteTable + " where InstrumentToken ='" + instrumentToken
						+ "' and usedForSignal != 'used' and timestampGrp <'" + endTime + "' ORDER BY Time ASC,id asc ";
				ResultSet timeStampRs = timeStampRsStmt.executeQuery(openSql);
				while (timeStampRs.next()) {
					idsList.add(timeStampRs.getInt(1));
				}
				if (null != idsList && idsList.size() > 0) {
					openSql = "SELECT distinct timestampGrp FROM " + quoteTable + " where id in("
							+ commaSeperatedIDs(idsList) + ") ORDER BY Time ASC,id asc ";
					timeStampRs = timeStampRsStmt.executeQuery(openSql);
					while (timeStampRs.next()) {
						timeStampPeriodList.add(timeStampRs.getTimestamp("timestampGrp"));
					}
				}
				timeStampRsStmt.close();

				for (int timeLoop = 0; timeLoop < timeStampPeriodList.size(); timeLoop++) {
					Double low = null;
					Double high = null;
					Double close = null;

					Statement timeLoopRsStmt = conn.createStatement();

					openSql = "SELECT * FROM " + quoteTable + " where InstrumentToken ='" + instrumentToken
							+ "' and timestampGrp ='" + timeStampPeriodList.get(timeLoop) + "' ORDER BY id DESC ";
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
					if (null == low || null == high || null == close)
						continue;

					openSql = "SELECT * FROM " + quoteTable + "_SignalParams where InstrumentToken ='" + instrumentToken
							+ "' ORDER BY id DESC LIMIT 1 ";
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
							+ "upMove,downMove,avgUpMove,avgDownMove,relativeStrength,RSI,fastEma,slowEma,difference,strategySignal,"
							+ "timestampGrp,usedForSignal,differenceMinusSignal,macdSignal,usedForZigZagSignal,ContinueTrackOfPsarTrend,"
							+ "ContinueTrackOfMacdTrend,ContinueTrackOfRsiTrend) "
							+ "values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)";
					PreparedStatement prepStmtInsertSignalParams = conn.prepareStatement(sql);

					PSarSignalParam p = new PSarSignalParam();
					RSISignalParam r = new RSISignalParam();
					MACDSignalParam m = new MACDSignalParam();

					if (null != signalContainer && null != signalContainer.p1)
						p = signalContainer.p1;
					if (null != signalContainer && null != signalContainer.p1)
						r = signalContainer.r1;
					if (null != signalContainer && null != signalContainer.p1)
						m = signalContainer.m1;

					prepStmtInsertSignalParams.setTimestamp(1,
							new Timestamp(dtTmFmt.parse(dtTmFmt.format(Calendar.getInstance().getTime())).getTime()));
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
					prepStmtInsertSignalParams.setString(23, "");
					prepStmtInsertSignalParams.setDouble(24, m.getDifferenceMinusSignal());
					prepStmtInsertSignalParams.setDouble(25, m.getMacdSignal());
					prepStmtInsertSignalParams.setString(26, "");
					prepStmtInsertSignalParams.setInt(27, p.getContinueTrackOfPsarTrend());
					prepStmtInsertSignalParams.setInt(28, m.getContinueTrackOfMacdTrend());
					prepStmtInsertSignalParams.setInt(29, r.getContinueTrackOfRsiTrend());
					prepStmtInsertSignalParams.executeUpdate();

					prepStmtInsertSignalParams.close();
					timeLoopRsStmt.close();
				}

				if (null != idsList && idsList.size() > 0) {
					lowHighCloseRsiStrategy(instrumentToken);
					lowHighCloseStrategy(instrumentToken);
					lowHighCloseTrendStrategy(instrumentToken);
					lowHighCloseChangingStrategy(instrumentToken);
					lowHighCloseChangingStrategy2(instrumentToken);
				}

				if (null != idsList && idsList.size() > 0) {
					Statement usedUpdateRsStmt = conn.createStatement();
					openSql = "update " + quoteTable + " set usedForSignal ='used' where id in("
							+ commaSeperatedIDs(idsList) + ")";
					usedUpdateRsStmt.executeUpdate(openSql);
					usedUpdateRsStmt.close();
				}
			} catch (SQLException | ParseException e) {
				LOGGER.info(
						"StreamingQuoteStorageImpl.calculateAndStoreStrategySignalParameters(): ERROR: SQLException on fetching data from Table, cause: "
								+ e.getMessage() + ">>" + e.getCause());
				e.printStackTrace();
			}
		} else {
			LOGGER.info(
					"StreamingQuoteStorageImpl.calculateAndStoreStrategySignalParameters(): ERROR: DB conn is null !!!");
		}
		// LOGGER.info("Exit
		// StreamingQuoteStorageImpl.calculateAndStoreStrategySignalParameters()");
	}

	private void lowHighCloseRsiStrategy(String instrumentToken) throws SQLException {
		// LOGGER.info("Entry
		// StreamingQuoteStorageImpl.lowHighCloseRsiStrategy()");
		int id = 0, firstRowId = 0, lastRowId = 0;
		int loopSize = 0;
		do {
			String openSql = "SELECT max(id) as maxId FROM " + quoteTable + "_SignalParams where InstrumentToken ='"
					+ instrumentToken + "' and usedForZigZagSignal='usedForZigZagSignal' ";
			Statement stmt1 = conn.createStatement();
			ResultSet rs1 = stmt1.executeQuery(openSql);
			while (rs1.next()) {
				id = rs1.getInt("maxId");
			}
			stmt1.close();
			openSql = "select * from (SELECT close,rsi,usedForZigZagSignal,id FROM " + quoteTable
					+ "_SignalParams where InstrumentToken ='" + instrumentToken + "' and rsi!="
					+ StreamingConfig.MAX_VALUE + " and id>" + id + " ORDER BY id ASC LIMIT 26)  a order by id desc";
			Statement stmt = conn.createStatement();
			ResultSet rs = stmt.executeQuery(openSql);
			double lowClose = StreamingConfig.MAX_VALUE, highClose = 0.0, firstClose = StreamingConfig.MAX_VALUE,
					lowRsi = StreamingConfig.MAX_VALUE, highRsi = 0.0, firstRsi = 0.0;
			boolean firstRecord = true;
			int signalClose = 1, signalRsi = 1;
			loopSize = 0;
			String isUnUsedRecord = "";

			while (rs.next()) {
				loopSize++;
				if (firstRecord) {
					firstRsi = rs.getDouble("rsi");
					firstClose = rs.getDouble("close");
					isUnUsedRecord = rs.getString("usedForZigZagSignal");
					firstRowId = rs.getInt("id");
					firstRecord = false;
				}
				lastRowId = rs.getInt("id");
				if (rs.getDouble("close") >= highClose) {
					highClose = rs.getDouble("close");
				}
				if (rs.getDouble("close") <= lowClose) {
					lowClose = rs.getDouble("close");
				}
				if (rs.getDouble("rsi") >= highRsi) {
					highRsi = rs.getDouble("rsi");
				}
				if (rs.getDouble("rsi") <= lowRsi) {
					lowRsi = rs.getDouble("rsi");
				}
			}
			stmt.close();
			if (firstClose == highClose) {
				signalClose = 0;
			}
			if (firstClose == lowClose) {
				signalClose = 2;
			}
			if (firstRsi == highRsi) {
				signalRsi = 0;
			}
			if (firstRsi == lowRsi) {
				signalRsi = 2;
			}
			if (loopSize >= 9 && !"usedForZigZagSignal".equalsIgnoreCase(isUnUsedRecord) && signalRsi != 1
					&& signalRsi == signalClose && !firstRecord && firstRsi != 0.0) {
				String sql = "INSERT INTO " + quoteTable + "_signalNew "
						+ "(time,instrumentToken,quantity,processSignal,status,TradePrice,signalParamKey) "
						+ "values(?,?,?,?,?,?,?)";
				PreparedStatement prepStmt = conn.prepareStatement(sql);

				prepStmt.setTimestamp(1, new Timestamp(Calendar.getInstance().getTime().getTime()));
				prepStmt.setString(2, instrumentToken);
				if (signalClose == 2) {
					prepStmt.setString(4, "BUY");
					prepStmt.setString(3, fetchLotSizeFromInstrumentDetails(instrumentToken, "BUY"));
				} else {
					prepStmt.setString(4, "SELL");
					prepStmt.setString(3, fetchLotSizeFromInstrumentDetails(instrumentToken, "SELL"));
				}
				prepStmt.setString(5, "active");
				prepStmt.setDouble(6, firstClose);
				prepStmt.setDouble(7, firstRowId);
				prepStmt.executeUpdate();
				prepStmt.close();

				Statement stmtForUpdate = conn.createStatement();
				if (firstRowId > 0) {
					sql = "update " + quoteTable
							+ "_SignalParams set usedForZigZagSignal ='usedForZigZagSignal' where id in(" + firstRowId
							+ ")";
					stmtForUpdate.executeUpdate(sql);
				}
				stmtForUpdate.close();
			} else if (loopSize == 26) {
				Statement stmtForUpdate = conn.createStatement();
				if (lastRowId > 0) {
					String sql = "update " + quoteTable
							+ "_SignalParams set usedForZigZagSignal ='usedForZigZagSignal' where id in(" + lastRowId
							+ ")";
					stmtForUpdate.executeUpdate(sql);
				}
				stmtForUpdate.close();

			}
		} while (firstRowId > id && loopSize == 26);
		// LOGGER.info("Exit
		// StreamingQuoteStorageImpl.lowHighCloseRsiStrategy()");
	}

	private void lowHighCloseStrategy(String instrumentToken) throws SQLException {
		// LOGGER.info("Entry
		// StreamingQuoteStorageImpl.lowHighCloseRsiStrategy()");
		int id = 0, firstRowId = 0, lastRowId = 0;
		int loopSize = 0;
		do {
			String openSql = "SELECT max(id) as maxId FROM " + quoteTable + "_SignalParams where InstrumentToken ='"
					+ instrumentToken + "' and usedForZigZagSignal1='usedForZigZagSignal1' ";
			Statement stmt1 = conn.createStatement();
			ResultSet rs1 = stmt1.executeQuery(openSql);
			while (rs1.next()) {
				id = rs1.getInt("maxId");
			}
			stmt1.close();
			openSql = "select * from (SELECT close,usedForZigZagSignal1,id FROM " + quoteTable
					+ "_SignalParams where InstrumentToken ='" + instrumentToken + "' and rsi!="
					+ StreamingConfig.MAX_VALUE + " and id>" + id + " ORDER BY id ASC LIMIT 26)  a order by id desc";
			Statement stmt = conn.createStatement();
			ResultSet rs = stmt.executeQuery(openSql);
			double lowClose = StreamingConfig.MAX_VALUE, highClose = 0.0, firstClose = StreamingConfig.MAX_VALUE;
			boolean firstRecord = true;
			int signalClose = 1;
			loopSize = 0;
			String isUnUsedRecord = "";

			while (rs.next()) {
				loopSize++;
				if (firstRecord) {
					firstClose = rs.getDouble("close");
					isUnUsedRecord = rs.getString("usedForZigZagSignal1");
					firstRowId = rs.getInt("id");
					firstRecord = false;
				}
				lastRowId = rs.getInt("id");
				if (rs.getDouble("close") >= highClose) {
					highClose = rs.getDouble("close");
				}
				if (rs.getDouble("close") <= lowClose) {
					lowClose = rs.getDouble("close");
				}
			}
			stmt.close();
			if (firstClose == highClose) {
				signalClose = 0;
			}
			if (firstClose == lowClose) {
				signalClose = 2;
			}

			if (loopSize >= 9 && !"usedForZigZagSignal1".equalsIgnoreCase(isUnUsedRecord) && signalClose != 1
					&& !firstRecord && firstClose != StreamingConfig.MAX_VALUE && firstClose != 0.0) {
				String sql = "INSERT INTO " + quoteTable + "_signalNew1 "
						+ "(time,instrumentToken,quantity,processSignal,status,TradePrice,signalParamKey) "
						+ "values(?,?,?,?,?,?,?)";
				PreparedStatement prepStmt = conn.prepareStatement(sql);

				prepStmt.setTimestamp(1, new Timestamp(Calendar.getInstance().getTime().getTime()));
				prepStmt.setString(2, instrumentToken);
				if (signalClose == 2) {
					prepStmt.setString(4, "BUY");
					prepStmt.setString(3, fetchLotSizeFromInstrumentDetails(instrumentToken, "BUY"));
				} else {
					prepStmt.setString(4, "SELL");
					prepStmt.setString(3, fetchLotSizeFromInstrumentDetails(instrumentToken, "SELL"));
				}
				prepStmt.setString(5, "active");
				prepStmt.setDouble(6, firstClose);
				prepStmt.setDouble(7, firstRowId);
				prepStmt.executeUpdate();
				prepStmt.close();

				Statement stmtForUpdate = conn.createStatement();
				if (firstRowId > 0) {
					sql = "update " + quoteTable
							+ "_SignalParams set usedForZigZagSignal1 ='usedForZigZagSignal1' where id in(" + firstRowId
							+ ")";
					stmtForUpdate.executeUpdate(sql);
				}
				stmtForUpdate.close();
			} else if (loopSize == 26) {
				Statement stmtForUpdate = conn.createStatement();
				if (lastRowId > 0) {
					String sql = "update " + quoteTable
							+ "_SignalParams set usedForZigZagSignal1 ='usedForZigZagSignal1' where id in(" + lastRowId
							+ ")";
					stmtForUpdate.executeUpdate(sql);
				}
				stmtForUpdate.close();

			}
		} while (firstRowId > id && loopSize == 26);
		// LOGGER.info("Exit
		// StreamingQuoteStorageImpl.lowHighCloseRsiStrategy()");
	}

	private void lowHighCloseTrendStrategy(String instrumentToken) throws SQLException {
		// LOGGER.info("Entry
		// StreamingQuoteStorageImpl.lowHighCloseRsiStrategy()");
		int id = 0, firstRowId = 0;
		int loopSize = 0;
		do {
			String openSql = "SELECT max(id) as maxId FROM " + quoteTable + "_SignalParams where InstrumentToken ='"
					+ instrumentToken + "' and usedForZigZagSignal2='usedForZigZagSignal2' ";
			Statement stmt1 = conn.createStatement();
			ResultSet rs1 = stmt1.executeQuery(openSql);
			while (rs1.next()) {
				id = rs1.getInt("maxId");
			}
			stmt1.close();
			openSql = "select * from (SELECT close,rsi,trend,macdSignal,ContinueTrackOfPsarTrend,ContinueTrackOfMacdTrend,ContinueTrackOfRsiTrend,usedForZigZagSignal2,id FROM "
					+ quoteTable + "_SignalParams where InstrumentToken ='" + instrumentToken + "' and rsi!="
					+ StreamingConfig.MAX_VALUE + " and id>" + id + " ORDER BY id ASC LIMIT 1)  a order by id desc";
			Statement stmt = conn.createStatement();
			ResultSet rs = stmt.executeQuery(openSql);
			double rsi = StreamingConfig.MAX_VALUE, firstClose = StreamingConfig.MAX_VALUE;
			double trend = StreamingConfig.MAX_VALUE, macdSignal = StreamingConfig.MAX_VALUE;
			loopSize = 0;
			String isUnUsedRecord = "";

			while (rs.next()) {
				loopSize++;
				firstClose = rs.getDouble("close");
				rsi = rs.getDouble("rsi");
				isUnUsedRecord = rs.getString("usedForZigZagSignal2");
				firstRowId = rs.getInt("id");
				trend = rs.getDouble("trend");
				macdSignal = rs.getDouble("macdSignal");
			}
			stmt.close();

			if (!"usedForZigZagSignal2".equalsIgnoreCase(isUnUsedRecord)
					&& ((trend == macdSignal && trend == 0.0 && rsi <= 40.0) || (trend == macdSignal && trend == 2.0
							&& rsi >= 60.0 && rsi != StreamingConfig.MAX_VALUE))) {
				String sql = "INSERT INTO " + quoteTable + "_signalNew2 "
						+ "(time,instrumentToken,quantity,processSignal,status,TradePrice,signalParamKey) "
						+ "values(?,?,?,?,?,?,?)";
				PreparedStatement prepStmt = conn.prepareStatement(sql);

				prepStmt.setTimestamp(1, new Timestamp(Calendar.getInstance().getTime().getTime()));
				prepStmt.setString(2, instrumentToken);
				if (rsi <= 40.0) {
					prepStmt.setString(4, "BUY");
					prepStmt.setString(3, fetchLotSizeFromInstrumentDetails(instrumentToken, "BUY"));
				} else {
					prepStmt.setString(4, "SELL");
					prepStmt.setString(3, fetchLotSizeFromInstrumentDetails(instrumentToken, "SELL"));
				}
				prepStmt.setString(5, "active");
				prepStmt.setDouble(6, firstClose);
				prepStmt.setDouble(7, firstRowId);
				prepStmt.executeUpdate();
				prepStmt.close();
			}
			Statement stmtForUpdate = conn.createStatement();
			if (firstRowId > 0) {
				String sql = "update " + quoteTable
						+ "_SignalParams set usedForZigZagSignal2 ='usedForZigZagSignal2' where id in(" + firstRowId
						+ ")";
				stmtForUpdate.executeUpdate(sql);
			}
			stmtForUpdate.close();
		} while (firstRowId > id && loopSize > 0);
		// LOGGER.info("Exit
		// StreamingQuoteStorageImpl.lowHighCloseRsiStrategy()");
	}

	private void lowHighCloseChangingStrategy(String instrumentToken) throws SQLException {
		// LOGGER.info("Entry
		// StreamingQuoteStorageImpl.lowHighCloseRsiStrategy()");
		int id = 0, firstRowId = 0;
		int loopSize = 0;
		do {
			String openSql = "SELECT max(id) as maxId FROM " + quoteTable + "_SignalParams where InstrumentToken ='"
					+ instrumentToken + "' and usedForZigZagSignal3='usedForZigZagSignal3' ";
			Statement stmt1 = conn.createStatement();
			ResultSet rs1 = stmt1.executeQuery(openSql);
			while (rs1.next()) {
				id = rs1.getInt("maxId");
			}
			stmt1.close();
			openSql = "select * from (SELECT close,rsi,trend,macdSignal,ContinueTrackOfPsarTrend,ContinueTrackOfMacdTrend,ContinueTrackOfRsiTrend,usedForZigZagSignal3,id FROM "
					+ quoteTable + "_SignalParams where InstrumentToken ='" + instrumentToken + "' and rsi!="
					+ StreamingConfig.MAX_VALUE + " and id>" + id + " ORDER BY id ASC LIMIT 1)  a order by id desc";
			Statement stmt = conn.createStatement();
			ResultSet rs = stmt.executeQuery(openSql);
			double rsi = StreamingConfig.MAX_VALUE, firstClose = StreamingConfig.MAX_VALUE;
			boolean firstRecord = true;
			double trend = StreamingConfig.MAX_VALUE, macdSignal = StreamingConfig.MAX_VALUE;
			int psarConTrend = 0, macdConTrend = 0, rsiConTrend = 0;
			loopSize = 0;
			String isUnUsedRecord = "";

			while (rs.next()) {
				loopSize++;
				if (firstRecord) {
					firstClose = rs.getDouble("close");
					rsi = rs.getDouble("rsi");
					isUnUsedRecord = rs.getString("usedForZigZagSignal3");
					firstRowId = rs.getInt("id");
					trend = rs.getDouble("trend");
					macdSignal = rs.getDouble("macdSignal");
					psarConTrend = rs.getInt("ContinueTrackOfPsarTrend");
					macdConTrend = rs.getInt("ContinueTrackOfMacdTrend");
					rsiConTrend = rs.getInt("ContinueTrackOfRsiTrend");
					firstRecord = false;
				}
			}
			stmt.close();

			if (loopSize == 1 && !"usedForZigZagSignal3".equalsIgnoreCase(isUnUsedRecord)
					&& firstClose != StreamingConfig.MAX_VALUE && firstClose != 0.0 && rsi != StreamingConfig.MAX_VALUE
					&& trend != StreamingConfig.MAX_VALUE && (macdSignal == 2.0 || macdSignal == 0.0)
					&& (psarConTrend >= -2 && psarConTrend <= 2) && (macdConTrend >= -2 && macdConTrend <= 2)
					&& (rsiConTrend >= -2 && rsiConTrend <= 2)) {
				String sql = "INSERT INTO " + quoteTable + "_signalNew3 "
						+ "(time,instrumentToken,quantity,processSignal,status,TradePrice,signalParamKey) "
						+ "values(?,?,?,?,?,?,?)";
				PreparedStatement prepStmt = conn.prepareStatement(sql);

				prepStmt.setTimestamp(1, new Timestamp(Calendar.getInstance().getTime().getTime()));
				prepStmt.setString(2, instrumentToken);
				if (macdSignal == 0.0) {
					prepStmt.setString(4, "BUY");
					prepStmt.setString(3, fetchLotSizeFromInstrumentDetails(instrumentToken, "BUY"));
				} else {
					prepStmt.setString(4, "SELL");
					prepStmt.setString(3, fetchLotSizeFromInstrumentDetails(instrumentToken, "SELL"));
				}
				prepStmt.setString(5, "active");
				prepStmt.setDouble(6, firstClose);
				prepStmt.setDouble(7, firstRowId);
				prepStmt.executeUpdate();
				prepStmt.close();

			}
			Statement stmtForUpdate = conn.createStatement();
			if (firstRowId > 0 && loopSize == 1) {
				String sql = "update " + quoteTable
						+ "_SignalParams set usedForZigZagSignal3 ='usedForZigZagSignal3' where id in(" + firstRowId
						+ ")";
				stmtForUpdate.executeUpdate(sql);
			}
			stmtForUpdate.close();
		} while (loopSize == 1);
		// LOGGER.info("Exit
		// StreamingQuoteStorageImpl.lowHighCloseRsiStrategy()");
	}

	private void lowHighCloseChangingStrategy2(String instrumentToken) throws SQLException {
		// LOGGER.info("Entry
		// StreamingQuoteStorageImpl.lowHighCloseRsiStrategy()");
		int id = 0, firstRowId = 0, lastRowId = 0;
		int loopSize = 0;
		do {
			String openSql = "SELECT max(id) as maxId FROM " + quoteTable + "_SignalParams where InstrumentToken ='"
					+ instrumentToken + "' and usedForZigZagSignal4='usedForZigZagSignal4' ";
			Statement stmt1 = conn.createStatement();
			ResultSet rs1 = stmt1.executeQuery(openSql);
			while (rs1.next()) {
				id = rs1.getInt("maxId");
			}
			stmt1.close();
			openSql = "select * from (SELECT close,rsi,trend,macdSignal,ContinueTrackOfPsarTrend,ContinueTrackOfMacdTrend,ContinueTrackOfRsiTrend,usedForZigZagSignal4,id FROM "
					+ quoteTable + "_SignalParams where InstrumentToken ='" + instrumentToken + "' and rsi!="
					+ StreamingConfig.MAX_VALUE + " and id>" + id + " ORDER BY id ASC LIMIT 2)  a order by id desc";
			Statement stmt = conn.createStatement();
			ResultSet rs = stmt.executeQuery(openSql);
			double rsi = StreamingConfig.MAX_VALUE, firstClose = StreamingConfig.MAX_VALUE;
			boolean firstRecord = true;
			double trend = StreamingConfig.MAX_VALUE, macdSignal = StreamingConfig.MAX_VALUE;
			int psarConTrend = 0, macdConTrend = 0;
			int psarConTrendPre = 0, macdConTrendPre = 0;
			loopSize = 0;
			String isUnUsedRecord = "";

			while (rs.next()) {
				loopSize++;
				if (firstRecord) {
					firstClose = rs.getDouble("close");
					rsi = rs.getDouble("rsi");
					isUnUsedRecord = rs.getString("usedForZigZagSignal4");
					firstRowId = rs.getInt("id");
					trend = rs.getDouble("trend");
					macdSignal = rs.getDouble("macdSignal");
					psarConTrend = rs.getInt("ContinueTrackOfPsarTrend");
					macdConTrend = rs.getInt("ContinueTrackOfMacdTrend");
					firstRecord = false;
				}
				lastRowId = rs.getInt("id");
				psarConTrendPre = rs.getInt("ContinueTrackOfPsarTrend");
				macdConTrendPre = rs.getInt("ContinueTrackOfMacdTrend");
			}
			stmt.close();

			if (loopSize == 2 && !"usedForZigZagSignal4".equalsIgnoreCase(isUnUsedRecord)
					&& firstClose != StreamingConfig.MAX_VALUE && firstClose != 0.0 && rsi != StreamingConfig.MAX_VALUE
					&& (macdSignal == 2.0 || macdSignal == 0.0)
					&& ((psarConTrend >= -1 && psarConTrend <= 1 && (psarConTrendPre >= 10 || psarConTrendPre <= -10)) 
					|| (macdConTrend >= -1 && macdConTrend <= 1 && (macdConTrendPre >= 10 || macdConTrendPre <= -10)))) 
			{
				String sql = "INSERT INTO " + quoteTable + "_signalNew4 "
						+ "(time,instrumentToken,quantity,processSignal,status,TradePrice,signalParamKey) "
						+ "values(?,?,?,?,?,?,?)";
				PreparedStatement prepStmt = conn.prepareStatement(sql);

				prepStmt.setTimestamp(1, new Timestamp(Calendar.getInstance().getTime().getTime()));
				prepStmt.setString(2, instrumentToken);
				if (macdSignal == 0.0) {
					prepStmt.setString(4, "BUY");
					prepStmt.setString(3, fetchLotSizeFromInstrumentDetails(instrumentToken, "BUY"));
				} else {
					prepStmt.setString(4, "SELL");
					prepStmt.setString(3, fetchLotSizeFromInstrumentDetails(instrumentToken, "SELL"));
				}
				prepStmt.setString(5, "active");
				prepStmt.setDouble(6, firstClose);
				prepStmt.setDouble(7, firstRowId);
				prepStmt.executeUpdate();
				prepStmt.close();

			}
			Statement stmtForUpdate = conn.createStatement();
			if (lastRowId > 0 && loopSize == 2) {
				String sql = "update " + quoteTable
						+ "_SignalParams set usedForZigZagSignal4 ='usedForZigZagSignal4' where id in(" + lastRowId
						+ ")";
				stmtForUpdate.executeUpdate(sql);
			}
			stmtForUpdate.close();
		} while (loopSize == 2);
		// LOGGER.info("Exit
		// StreamingQuoteStorageImpl.lowHighCloseRsiStrategy()");
	}
	private void lowHighCloseChangingStrategy3(String instrumentToken) throws SQLException {
        // LOGGER.info("Entry
        // StreamingQuoteStorageImpl.lowHighCloseRsiStrategy()");
        int id = 0, firstRowId = 0, lastRowId = 0;
        int loopSize = 0;
        do {
            String openSql = "SELECT max(id) as maxId FROM " + quoteTable + "_SignalParams where InstrumentToken ='"
                    + instrumentToken + "' and usedForZigZagSignal4='usedForZigZagSignal4' ";
            Statement stmt1 = conn.createStatement();
            ResultSet rs1 = stmt1.executeQuery(openSql);
            while (rs1.next()) {
                id = rs1.getInt("maxId");
            }
            stmt1.close();
            openSql = "select * from (SELECT close,rsi,trend,macdSignal,ContinueTrackOfPsarTrend,ContinueTrackOfMacdTrend,ContinueTrackOfRsiTrend,usedForZigZagSignal4,id FROM "
                    + quoteTable + "_SignalParams where InstrumentToken ='" + instrumentToken + "' and rsi!="
                    + StreamingConfig.MAX_VALUE + " and id>" + id + " ORDER BY id ASC LIMIT 2)  a order by id desc";
            Statement stmt = conn.createStatement();
            ResultSet rs = stmt.executeQuery(openSql);
            double rsi = StreamingConfig.MAX_VALUE, firstClose = StreamingConfig.MAX_VALUE;
            boolean firstRecord = true;
            double trend = StreamingConfig.MAX_VALUE, macdSignal = StreamingConfig.MAX_VALUE;
            int psarConTrend = 0, macdConTrend = 0;
            int psarConTrendPre = 0, macdConTrendPre = 0;
            loopSize = 0;
            String isUnUsedRecord = "";

            while (rs.next()) {
                loopSize++;
                if (firstRecord) {
                    firstClose = rs.getDouble("close");
                    rsi = rs.getDouble("rsi");
                    isUnUsedRecord = rs.getString("usedForZigZagSignal4");
                    firstRowId = rs.getInt("id");
                    trend = rs.getDouble("trend");
                    macdSignal = rs.getDouble("macdSignal");
                    psarConTrend = rs.getInt("ContinueTrackOfPsarTrend");
                    macdConTrend = rs.getInt("ContinueTrackOfMacdTrend");
                    firstRecord = false;
                }
                lastRowId = rs.getInt("id");
                psarConTrendPre = rs.getInt("ContinueTrackOfPsarTrend");
                macdConTrendPre = rs.getInt("ContinueTrackOfMacdTrend");
            }
            stmt.close();

            if (loopSize == 2 && !"usedForZigZagSignal4".equalsIgnoreCase(isUnUsedRecord)
                    && firstClose != StreamingConfig.MAX_VALUE && firstClose != 0.0 && rsi != StreamingConfig.MAX_VALUE
                    && (macdSignal == 2.0 || macdSignal == 0.0)
                    && ((psarConTrend >= -1 && psarConTrend <= 1 && (psarConTrendPre >= 5 || psarConTrendPre <= -5)) 
                    || (macdConTrend >= -1 && macdConTrend <= 1 && (macdConTrendPre >= 5 || macdConTrendPre <= -5)))) 
            {
                String sql = "INSERT INTO " + quoteTable + "_signalNew4 "
                        + "(time,instrumentToken,quantity,processSignal,status,TradePrice,signalParamKey) "
                        + "values(?,?,?,?,?,?,?)";
                PreparedStatement prepStmt = conn.prepareStatement(sql);

                prepStmt.setTimestamp(1, new Timestamp(Calendar.getInstance().getTime().getTime()));
                prepStmt.setString(2, instrumentToken);
                if (macdSignal == 0.0) {
                    prepStmt.setString(4, "BUY");
                    prepStmt.setString(3, fetchLotSizeFromInstrumentDetails(instrumentToken, "BUY"));
                } else {
                    prepStmt.setString(4, "SELL");
                    prepStmt.setString(3, fetchLotSizeFromInstrumentDetails(instrumentToken, "SELL"));
                }
                prepStmt.setString(5, "active");
                prepStmt.setDouble(6, firstClose);
                prepStmt.setDouble(7, firstRowId);
                prepStmt.executeUpdate();
                prepStmt.close();

            }
            Statement stmtForUpdate = conn.createStatement();
            if (lastRowId > 0 && loopSize == 2) {
                String sql = "update " + quoteTable
                        + "_SignalParams set usedForZigZagSignal4 ='usedForZigZagSignal4' where id in(" + lastRowId
                        + ")";
                stmtForUpdate.executeUpdate(sql);
            }
            stmtForUpdate.close();
        } while (loopSize == 2);
        // LOGGER.info("Exit
        // StreamingQuoteStorageImpl.lowHighCloseRsiStrategy()");
    }
	private void lowHighCloseChangingStrategy4(String instrumentToken) throws SQLException {
        // LOGGER.info("Entry
        // StreamingQuoteStorageImpl.lowHighCloseRsiStrategy()");
        int id = 0, firstRowId = 0, lastRowId = 0;
        int loopSize = 0;
        do {
            String openSql = "SELECT max(id) as maxId FROM " + quoteTable + "_SignalParams where InstrumentToken ='"
                    + instrumentToken + "' and usedForZigZagSignal4='usedForZigZagSignal4' ";
            Statement stmt1 = conn.createStatement();
            ResultSet rs1 = stmt1.executeQuery(openSql);
            while (rs1.next()) {
                id = rs1.getInt("maxId");
            }
            stmt1.close();
            openSql = "select * from (SELECT close,rsi,trend,macdSignal,ContinueTrackOfPsarTrend,ContinueTrackOfMacdTrend,ContinueTrackOfRsiTrend,usedForZigZagSignal4,id FROM "
                    + quoteTable + "_SignalParams where InstrumentToken ='" + instrumentToken + "' and rsi!="
                    + StreamingConfig.MAX_VALUE + " and id>" + id + " ORDER BY id ASC LIMIT 2)  a order by id desc";
            Statement stmt = conn.createStatement();
            ResultSet rs = stmt.executeQuery(openSql);
            double rsi = StreamingConfig.MAX_VALUE, firstClose = StreamingConfig.MAX_VALUE;
            boolean firstRecord = true;
            double trend = StreamingConfig.MAX_VALUE, macdSignal = StreamingConfig.MAX_VALUE;
            int psarConTrend = 0, macdConTrend = 0;
            int psarConTrendPre = 0, macdConTrendPre = 0;
            loopSize = 0;
            String isUnUsedRecord = "";

            while (rs.next()) {
                loopSize++;
                if (firstRecord) {
                    firstClose = rs.getDouble("close");
                    rsi = rs.getDouble("rsi");
                    isUnUsedRecord = rs.getString("usedForZigZagSignal4");
                    firstRowId = rs.getInt("id");
                    trend = rs.getDouble("trend");
                    macdSignal = rs.getDouble("macdSignal");
                    psarConTrend = rs.getInt("ContinueTrackOfPsarTrend");
                    macdConTrend = rs.getInt("ContinueTrackOfMacdTrend");
                    firstRecord = false;
                }
                lastRowId = rs.getInt("id");
                psarConTrendPre = rs.getInt("ContinueTrackOfPsarTrend");
                macdConTrendPre = rs.getInt("ContinueTrackOfMacdTrend");
            }
            stmt.close();

            if (loopSize == 2 && !"usedForZigZagSignal4".equalsIgnoreCase(isUnUsedRecord)
                    && firstClose != StreamingConfig.MAX_VALUE && firstClose != 0.0 && rsi != StreamingConfig.MAX_VALUE
                    && ((macdSignal == 2.0 && psarConTrend >= -1 && psarConTrend <= 1 && psarConTrendPre >= 5) 
                    || (macdSignal == 2.0 && macdConTrend >= -1 && macdConTrend <= 1 && macdConTrendPre >= 5)
                    || (macdSignal == 0.0 && psarConTrend >= -1 && psarConTrend <= 1 && psarConTrendPre <= -5) 
                    || (macdSignal == 0.0 && macdConTrend >= -1 && macdConTrend <= 1 && macdConTrendPre <= -5))) 
                {
                String sql = "INSERT INTO " + quoteTable + "_signalNew4 "
                        + "(time,instrumentToken,quantity,processSignal,status,TradePrice,signalParamKey) "
                        + "values(?,?,?,?,?,?,?)";
                PreparedStatement prepStmt = conn.prepareStatement(sql);

                prepStmt.setTimestamp(1, new Timestamp(Calendar.getInstance().getTime().getTime()));
                prepStmt.setString(2, instrumentToken);
                if (macdSignal == 0.0) {
                    prepStmt.setString(4, "BUY");
                    prepStmt.setString(3, fetchLotSizeFromInstrumentDetails(instrumentToken, "BUY"));
                } else {
                    prepStmt.setString(4, "SELL");
                    prepStmt.setString(3, fetchLotSizeFromInstrumentDetails(instrumentToken, "SELL"));
                }
                prepStmt.setString(5, "active");
                prepStmt.setDouble(6, firstClose);
                prepStmt.setDouble(7, firstRowId);
                prepStmt.executeUpdate();
                prepStmt.close();

            }
            Statement stmtForUpdate = conn.createStatement();
            if (lastRowId > 0 && loopSize == 2) {
                String sql = "update " + quoteTable
                        + "_SignalParams set usedForZigZagSignal4 ='usedForZigZagSignal4' where id in(" + lastRowId
                        + ")";
                stmtForUpdate.executeUpdate(sql);
            }
            stmtForUpdate.close();
        } while (loopSize == 2);
        // LOGGER.info("Exit
        // StreamingQuoteStorageImpl.lowHighCloseRsiStrategy()");
    }
	private SignalContainer whenPsarRsiMacdAll3sPreviousSignalsNOTAvailable(String instrumentToken, Double low,
			Double high, Double close) throws SQLException {
		// LOGGER.info("Entry
		// StreamingQuoteStorageImpl.whenPsarRsiMacdAll3sPreviousSignalsNOTAvailable()");

		String openSql = "SELECT * FROM " + quoteTable + "_SignalParams where InstrumentToken ='" + instrumentToken
				+ "' ORDER BY Time DESC,id desc LIMIT 35 ";
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
			macdSignalParam.setDifferenceMinusSignal(openRs.getDouble("differenceMinusSignal"));
			macdSignalParam.setMacdSignal(openRs.getDouble("macdSignal"));
			macdSignalParam.setContinueTrackOfMacdTrend(openRs.getInt("continueTrackOfMacdTrend"));
			macdSignalParamList.add(macdSignalParam);

			rsiSignalParam.setClose(openRs.getDouble("close"));
			rsiSignalParam.setDownMove(openRs.getDouble("downMove"));
			rsiSignalParam.setUpMove(openRs.getDouble("upMove"));
			rsiSignalParam.setAvgDownMove(openRs.getDouble("avgDownMove"));
			rsiSignalParam.setAvgDownMove(openRs.getDouble("avgDownMove"));
			rsiSignalParam.setRelativeStrength(openRs.getDouble("relativeStrength"));
			rsiSignalParam.setContinueTrackOfRsiTrend(openRs.getInt("ContinueTrackOfRsiTrend"));
			rsiSignalParam.setRSI(openRs.getDouble("rSI"));
			rsiSignalParamList.add(rsiSignalParam);
		}
		stmt.close();
		signalContainer.r1 = new RSISignalParam(rsiSignalParamList, close);
		signalContainer.m1 = new MACDSignalParam(macdSignalParamList, close);

		// LOGGER.info("Exit
		// StreamingQuoteStorageImpl.whenPsarRsiMacdAll3sPreviousSignalsNOTAvailable()");

		return signalContainer;
	}

	private SignalContainer whenPsarPreviousSignalsAvailableButNotRsiAndMacd(String instrumentToken, Double low,
			Double high, Double close) throws SQLException {
		// LOGGER.info("Entry
		// StreamingQuoteStorageImpl.whenPsarPreviousSignalsAvailableButNotRsiAndMacd()");

		String openSql = "SELECT * FROM " + quoteTable + "_SignalParams where InstrumentToken ='" + instrumentToken
				+ "' ORDER BY Time DESC,id desc LIMIT 35 ";
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
						openRs.getDouble("eP_pSarXaccFactor"), openRs.getInt("trend"),
						openRs.getInt("continueTrackOfPsarTrend"));
				firstLoop = false;
			}
			macdSignalParam = new MACDSignalParam();
			rsiSignalParam = new RSISignalParam();

			macdSignalParam.setClose(openRs.getDouble("close"));
			macdSignalParam.setDifference(openRs.getDouble("difference"));
			macdSignalParam.setFastEma(openRs.getDouble("fastEma"));
			macdSignalParam.setSlowEma(openRs.getDouble("slowEma"));
			macdSignalParam.setSignal(openRs.getDouble("strategySignal"));
			macdSignalParam.setDifferenceMinusSignal(openRs.getDouble("differenceMinusSignal"));
			macdSignalParam.setMacdSignal(openRs.getDouble("macdSignal"));
			macdSignalParam.setContinueTrackOfMacdTrend(openRs.getInt("continueTrackOfMacdTrend"));
			macdSignalParamList.add(macdSignalParam);

			rsiSignalParam.setClose(openRs.getDouble("close"));
			rsiSignalParam.setDownMove(openRs.getDouble("downMove"));
			rsiSignalParam.setUpMove(openRs.getDouble("upMove"));
			rsiSignalParam.setAvgDownMove(openRs.getDouble("avgDownMove"));
			rsiSignalParam.setAvgDownMove(openRs.getDouble("avgDownMove"));
			rsiSignalParam.setRelativeStrength(openRs.getDouble("relativeStrength"));
			rsiSignalParam.setRSI(openRs.getDouble("rSI"));
			rsiSignalParam.setContinueTrackOfRsiTrend(openRs.getInt("ContinueTrackOfRsiTrend"));
			rsiSignalParamList.add(rsiSignalParam);
		}
		stmt.close();
		signalContainer.r1 = new RSISignalParam(rsiSignalParamList, close);
		signalContainer.m1 = new MACDSignalParam(macdSignalParamList, close);

		// LOGGER.info("Exit
		// StreamingQuoteStorageImpl.whenPsarPreviousSignalsAvailableButNotRsiAndMacd()");

		return signalContainer;
	}

	private SignalContainer whenPsarRsiPreviousSignalsAvailableButNotMacd(String instrumentToken, Double low,
			Double high, Double close) throws SQLException {
		// LOGGER.info("Entry
		// StreamingQuoteStorageImpl.whenPsarRsiPreviousSignalsAvailableButNotMacd()");

		String openSql = "SELECT * FROM " + quoteTable + "_SignalParams where InstrumentToken ='" + instrumentToken
				+ "' ORDER BY Time DESC,id desc LIMIT 35 ";
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
						openRs.getDouble("eP_pSarXaccFactor"), openRs.getInt("trend"),
						openRs.getInt("continueTrackOfPsarTrend"));
				signalContainer.r1 = new RSISignalParam(openRs.getDouble("close"), close, openRs.getDouble("upMove"),
						openRs.getDouble("downMove"), openRs.getDouble("avgUpMove"), openRs.getDouble("avgDownMove"),
						openRs.getDouble("relativeStrength"), openRs.getDouble("rSI"),
						openRs.getInt("ContinueTrackOfRsiTrend"));
				firstLoop = false;
			}
			macdSignalParam = new MACDSignalParam();
			macdSignalParam.setClose(openRs.getDouble("close"));
			macdSignalParam.setDifference(openRs.getDouble("difference"));
			macdSignalParam.setFastEma(openRs.getDouble("fastEma"));
			macdSignalParam.setSlowEma(openRs.getDouble("slowEma"));
			macdSignalParam.setSignal(openRs.getDouble("strategySignal"));
			macdSignalParam.setDifferenceMinusSignal(openRs.getDouble("differenceMinusSignal"));
			macdSignalParam.setMacdSignal(openRs.getDouble("macdSignal"));
			macdSignalParam.setContinueTrackOfMacdTrend(openRs.getInt("continueTrackOfMacdTrend"));
			macdSignalParamList.add(macdSignalParam);
		}
		stmt.close();
		signalContainer.m1 = new MACDSignalParam(macdSignalParamList, close);

		// LOGGER.info("Exit
		// StreamingQuoteStorageImpl.whenPsarRsiPreviousSignalsAvailableButNotMacd()");

		return signalContainer;
	}

	private SignalContainer whenPsarRsiMacdAll3sPreviousSignalsAvailable(ResultSet openRs, Double low, Double high,
			Double close) throws SQLException {
		// LOGGER.info("Entry
		// StreamingQuoteStorageImpl.whenPsarRsiMacdAll3sPreviousSignalsAvailable()");

		SignalContainer signalContainer = new SignalContainer();
		signalContainer.p1 = new PSarSignalParam(high, low, openRs.getDouble("pSar"), openRs.getDouble("eP"),
				openRs.getDouble("eP_pSar"), openRs.getDouble("accFactor"), openRs.getDouble("eP_pSarXaccFactor"),
				openRs.getInt("trend"), openRs.getInt("continueTrackOfPsarTrend"));
		signalContainer.r1 = new RSISignalParam(openRs.getDouble("close"), close, openRs.getDouble("upMove"),
				openRs.getDouble("downMove"), openRs.getDouble("avgUpMove"), openRs.getDouble("avgDownMove"),
				openRs.getDouble("relativeStrength"), openRs.getDouble("rSI"),
				openRs.getInt("ContinueTrackOfRsiTrend"));
		signalContainer.m1 = new MACDSignalParam(close, openRs.getDouble("fastEma"), openRs.getDouble("slowEma"),
				openRs.getDouble("strategySignal"), openRs.getDouble("differenceMinusSignal"),
				openRs.getDouble("macdSignal"), openRs.getInt("continueTrackOfMacdTrend"));

		// LOGGER.info("Exit
		// StreamingQuoteStorageImpl.whenPsarRsiMacdAll3sPreviousSignalsAvailable()");

		return signalContainer;
	}

	@Override
	public void saveInstrumentTokenPriority(Map<String, InstrumentVolatilityScore> stocksSymbolArray) {
		// LOGGER.info("Entry
		// StreamingQuoteStorageImpl.saveInstrumentTokenPriority()");

		if (conn != null && stocksSymbolArray != null && stocksSymbolArray.size() > 0) {
			try {
				Statement stmt = conn.createStatement();
				String openSql = "UPDATE " + quoteTable
						+ "_instrumentDetails SET PriorityPoint = ?, currentVolatility = ?, last_price = ?, lotSize = ? where tradingSymbol = ? ";
				PreparedStatement prepStmt = conn.prepareStatement(openSql);
				Object[] symbolKeys = stocksSymbolArray.keySet().toArray();
				for (int i = 0; i < symbolKeys.length; i++) {

					prepStmt.setDouble(1, stocksSymbolArray.get(symbolKeys[i]).getDailyVolatility());
					prepStmt.setDouble(2, stocksSymbolArray.get(symbolKeys[i]).getCurrentVolatility());
					prepStmt.setString(3, String.valueOf(stocksSymbolArray.get(symbolKeys[i]).getPrice()));
					prepStmt.setString(4, String.valueOf(stocksSymbolArray.get(symbolKeys[i]).getLotSize()));
					prepStmt.setString(5, (String) symbolKeys[i]);

					prepStmt.executeUpdate();
				}
				prepStmt.close();
				stmt.close();

			} catch (SQLException e) {
				LOGGER.info(
						"StreamingQuoteStorageImpl.saveInstrumentTokenPriority(): ERROR: SQLException on fetching data from Table, cause: "
								+ e.getMessage() + ">>" + e.getCause());
			}
		} else {
			LOGGER.info("StreamingQuoteStorageImpl.saveInstrumentTokenPriority(): ERROR: DB conn is null !!!");
		}

		// LOGGER.info("Exit
		// StreamingQuoteStorageImpl.saveInstrumentTokenPriority()");

	}

	@Override
	public Map<Long, String> calculateSignalsFromStrategyParams(ArrayList<Long> instrumentList) {
		// LOGGER.info("Entry
		// StreamingQuoteStorageImpl.calculateSignalsFromStrategyParams()");

		Map<Long, String> signalList = new HashMap<Long, String>();

		if (conn != null && instrumentList != null && instrumentList.size() > 0) {
			Statement stmt;
			try {
				stmt = conn.createStatement();

				String openSql;
				for (int count = 0; count < instrumentList.size(); count++) {
					openSql = "SELECT trend,rSI,difference,strategySignal,close,id,usedForSignal FROM " + quoteTable
							+ "_SignalParams where InstrumentToken ='" + instrumentList.get(count)
							+ "' ORDER BY Time DESC,id desc LIMIT 4 ";

					ResultSet openRs = stmt.executeQuery(openSql);

					boolean firstDataSet = true;
					boolean secondDataSet = false;
					boolean thirdDataSet = false;

					double macdSignalTempLvlVal = StreamingConfig.MAX_VALUE,
							macdSignalCurrLvl = StreamingConfig.MAX_VALUE,
							macdSignalPrevLvl1 = StreamingConfig.MAX_VALUE,
							macdSignalPrevLvl2 = StreamingConfig.MAX_VALUE,
							macdSignalPrevLvl3 = StreamingConfig.MAX_VALUE;

					int trend = 1, firstRowId = 0;
					double rsiCurr = 0.0, rsiPrev = 0.0;
					double price = 0.0;
					String isUnUsedRecord = "";
					while (openRs.next()) {
						if (StreamingConfig.MAX_VALUE != openRs.getDouble("rSI")
								&& StreamingConfig.MAX_VALUE != openRs.getDouble("strategySignal")) {
							macdSignalTempLvlVal = openRs.getDouble("difference") - openRs.getDouble("strategySignal");
							if (firstDataSet) {
								macdSignalCurrLvl = macdSignalTempLvlVal;
								trend = openRs.getInt("trend");
								rsiCurr = openRs.getDouble("rSI");
								price = openRs.getDouble("close");
								firstRowId = openRs.getInt("id");
								isUnUsedRecord = openRs.getString("usedForSignal");
								firstDataSet = false;
								secondDataSet = true;
							} else if (secondDataSet) {
								macdSignalPrevLvl1 = macdSignalTempLvlVal;
								rsiPrev = openRs.getDouble("rSI");
								secondDataSet = false;
								thirdDataSet = true;
							} else if (thirdDataSet) {
								macdSignalPrevLvl2 = macdSignalTempLvlVal;
								thirdDataSet = false;
							}

							if ((!firstDataSet && !secondDataSet && !thirdDataSet
									&& !"used".equalsIgnoreCase(isUnUsedRecord)
									&& StreamingConfig.MAX_VALUE != macdSignalCurrLvl
									&& StreamingConfig.MAX_VALUE != macdSignalPrevLvl1
									&& StreamingConfig.MAX_VALUE != macdSignalPrevLvl2
									&& StreamingConfig.MAX_VALUE != macdSignalTempLvlVal)
									|| (macdSignalCurrLvl > 0.0 && macdSignalPrevLvl1 < 0.0
											&& StreamingConfig.MAX_VALUE != macdSignalCurrLvl)
									|| (macdSignalCurrLvl < 0.0 && macdSignalPrevLvl1 > 0.0
											&& StreamingConfig.MAX_VALUE != macdSignalPrevLvl1)) {
								if (!firstDataSet && !secondDataSet && !thirdDataSet)
									macdSignalPrevLvl3 = macdSignalTempLvlVal;

								if (trend == 2 && ((rsiPrev != 0.0 && rsiPrev < 40.0 && rsiCurr >= 40.0)
										|| (macdSignalCurrLvl >= 0.0 && macdSignalPrevLvl1 <= 0.0
												&& macdSignalPrevLvl2 <= 0.0 && macdSignalPrevLvl3 <= 0.0
												&& macdSignalCurrLvl > macdSignalPrevLvl1
												&& macdSignalPrevLvl1 > macdSignalPrevLvl2
												&& macdSignalPrevLvl2 > macdSignalPrevLvl3))) {
									signalList.put(instrumentList.get(count), "BUY," + price);
									Statement stmtForUpdate = conn.createStatement();
									if (firstRowId > 0) {
										openSql = "update " + quoteTable
												+ "_SignalParams set usedForSignal ='used' where id in(" + firstRowId
												+ ")";
										stmtForUpdate.executeUpdate(openSql);
									}
									stmtForUpdate.close();

								} else if (trend == 0 && ((rsiCurr >= 60.0 && rsiPrev > 60.0)
										|| (StreamingConfig.MAX_VALUE != macdSignalCurrLvl
												&& StreamingConfig.MAX_VALUE != macdSignalPrevLvl1
												&& StreamingConfig.MAX_VALUE != macdSignalPrevLvl2
												&& StreamingConfig.MAX_VALUE != macdSignalPrevLvl3
												&& macdSignalCurrLvl <= 0.0 && macdSignalPrevLvl1 >= 0.0
												&& macdSignalPrevLvl2 >= 0.0 && macdSignalPrevLvl3 >= 0.0
												&& macdSignalCurrLvl < macdSignalPrevLvl1
												&& macdSignalPrevLvl1 < macdSignalPrevLvl2
												&& macdSignalPrevLvl2 < macdSignalPrevLvl3))) {
									signalList.put(instrumentList.get(count), "SELL," + price);

									Statement stmtForUpdate = conn.createStatement();
									if (firstRowId > 0) {
										openSql = "update " + quoteTable
												+ "_SignalParams set usedForSignal ='used' where id in(" + firstRowId
												+ ")";
										stmtForUpdate.executeUpdate(openSql);
									}
									stmtForUpdate.close();
								}
							}
						}
					}
				}
				stmt.close();
			} catch (SQLException e) {
				LOGGER.info(
						"StreamingQuoteStorageImpl.calculateSignalsFromStrategyParams(): ERROR: SQLException on fetching data from Table, cause: "
								+ e.getMessage() + ">>" + e.getCause());
			}
		} else {
			LOGGER.info("StreamingQuoteStorageImpl.calculateSignalsFromStrategyParams(): ERROR: DB conn is null !!!");
		}

		// LOGGER.info("Exit
		// StreamingQuoteStorageImpl.calculateSignalsFromStrategyParams()");

		return signalList;
	}

	@Override
	public void saveInstrumentVolatilityDetails(List<InstrumentVolatilityScore> instrumentVolatilityScoreList) {

		// LOGGER.info("Entry
		// StreamingQuoteStorageImpl.getInstrumentTokenIdsFromSymbols()");

		if (conn != null && instrumentVolatilityScoreList != null && instrumentVolatilityScoreList.size() > 0) {
			try {
				Statement stmt = conn.createStatement();
				String openSql = "UPDATE " + quoteTable
						+ "_instrumentDetails SET dailyVolatility = ?, annualVolatility = ?, last_price = ?, lotSize = ? where tradingSymbol = ? ";
				PreparedStatement prepStmt = conn.prepareStatement(openSql);
				for (int index = 0; index < instrumentVolatilityScoreList.size(); index++) {

					prepStmt.setDouble(1, instrumentVolatilityScoreList.get(index).getDailyVolatility());
					prepStmt.setDouble(2, instrumentVolatilityScoreList.get(index).getAnnualVolatility());
					prepStmt.setString(3, String.valueOf(instrumentVolatilityScoreList.get(index).getPrice()));
					prepStmt.setString(4, String.valueOf(instrumentVolatilityScoreList.get(index).getLotSize()));
					prepStmt.setString(5, instrumentVolatilityScoreList.get(index).getInstrumentName());

					prepStmt.executeUpdate();
				}
				prepStmt.close();
				stmt.close();
			} catch (SQLException e) {
				LOGGER.info(
						"StreamingQuoteStorageImpl.getInstrumentTokenIdsFromSymbols(): ERROR: SQLException on fetching data from Table, cause: "
								+ e.getMessage() + ">>" + e.getCause());
			}
		} else {
			LOGGER.info("StreamingQuoteStorageImpl.getInstrumentTokenIdsFromSymbols(): ERROR: DB conn is null !!!");
		}

		// LOGGER.info("Exit
		// StreamingQuoteStorageImpl.getInstrumentTokenIdsFromSymbols()");
	}

	@Override
	public void markTradableInstruments(List<InstrumentVolatilityScore> instrumentVolatilityScoreList) {

		// LOGGER.info("Entry
		// StreamingQuoteStorageImpl.markTradableInstruments()");

		if (conn != null && instrumentVolatilityScoreList != null && instrumentVolatilityScoreList.size() > 0) {
			try {
				Statement stmt = conn.createStatement();
				String openSql = "UPDATE " + quoteTable
						+ "_instrumentDetails SET tradable = ? where tradingSymbol = ? ";
				PreparedStatement prepStmt = conn.prepareStatement(openSql);
				for (int index = 0; index < instrumentVolatilityScoreList.size(); index++) {

					prepStmt.setString(1, instrumentVolatilityScoreList.get(index).getTradable());
					prepStmt.setString(2, instrumentVolatilityScoreList.get(index).getInstrumentName());

					prepStmt.executeUpdate();
				}
				prepStmt.close();
				stmt.close();
			} catch (SQLException e) {
				LOGGER.info(
						"StreamingQuoteStorageImpl.markTradableInstruments(): ERROR: SQLException on fetching data from Table, cause: "
								+ e.getMessage() + ">>" + e.getCause());
			}
		} else {
			LOGGER.info("StreamingQuoteStorageImpl.markTradableInstruments(): ERROR: DB conn is null !!!");
		}

		// LOGGER.info("Exit
		// StreamingQuoteStorageImpl.markTradableInstruments()");

	}

	@Override
	public void saveBackendReadyFlag(boolean backendReadyForProcessing) {

		// LOGGER.info("Entry
		// StreamingQuoteStorageImpl.saveBackendReadyFlag()");
		if (conn != null) {
			try {
				String sql = "INSERT INTO " + quoteTable + "_ReadyFlag " + "(time,backendReadyFlag) " + "values(?,?)";
				PreparedStatement prepStmt = conn.prepareStatement(sql);
				prepStmt.setTimestamp(1, new Timestamp(Calendar.getInstance().getTime().getTime()));
				if (backendReadyForProcessing)
					prepStmt.setInt(2, 1);
				else
					prepStmt.setInt(2, 0);
				prepStmt.executeUpdate();
				prepStmt.close();
			} catch (SQLException e) {
				e.printStackTrace();
			}
		} else {
			if (conn != null) {
				LOGGER.info("StreamingQuoteStorageImpl.saveBackendReadyFlag(): ERROR: DB conn is null !!!");
			} else {
				LOGGER.info(
						"StreamingQuoteStorageImpl.saveBackendReadyFlag(): ERROR: quote is not of type StreamingQuoteModeQuote !!!");
			}
		}
		// LOGGER.info("Exit StreamingQuoteStorageImpl.saveGeneratedSignals()");
	}

	@Override
	public boolean getBackendReadyFlag() {

		// LOGGER.info("Entry
		// StreamingQuoteStorageImpl.getBackendReadyFlag");
		boolean backendReady = false;
		if (conn != null) {
			try {
				Statement stmt = conn.createStatement();
				String openSql = "SELECT backendReadyFlag FROM " + quoteTable
						+ "_ReadyFlag  ORDER BY time DESC LIMIT 1";

				ResultSet openRs = stmt.executeQuery(openSql);
				while (openRs.next()) {
					if (openRs.getInt(1) == 1)
						backendReady = true;
				}
				stmt.close();
			} catch (SQLException e) {
				LOGGER.info(
						"StreamingQuoteStorageImpl.getBackendReadyFlag(): ERROR: SQLException on fetching data from Table, cause: "
								+ e.getMessage() + ">>" + e.getCause());
			}
		} else {
			LOGGER.info("StreamingQuoteStorageImpl.getBackendReadyFlag(): ERROR: DB conn is null !!!");
		}
		// LOGGER.info("Exit
		// StreamingQuoteStorageImpl.getBackendReadyFlag");
		return backendReady;
	}

	@Override
	public ArrayList<String> tradingSymbolListOnInstrumentTokenId(ArrayList<Long> quoteStreamingInstrumentsArr) {

		// LOGGER.info("Entry
		// StreamingQuoteStorageImpl.tradingSymbolListOnInstrumentTokenId()");
		ArrayList<String> symbolList = new ArrayList<String>();
		if (conn != null) {
			try {
				if (quoteStreamingInstrumentsArr.size() > 0) {
					Statement stmt = conn.createStatement();

					String openSql = "SELECT tradingsymbol FROM " + quoteTable
							+ "_instrumentDetails WHERE instrumentToken in ("
							+ commaSeperatedLongIDs(quoteStreamingInstrumentsArr)
							+ ") order by instrumentToken desc,id desc";
					ResultSet openRs = stmt.executeQuery(openSql);
					while (openRs.next()) {
						symbolList.add(openRs.getString(1));
					}
					stmt.close();
				}
			} catch (SQLException e) {
				LOGGER.info(
						"StreamingQuoteStorageImpl.tradingSymbolListOnInstrumentTokenId(): ERROR: SQLException on fetching data from Table, cause: "
								+ e.getMessage() + ">>" + e.getCause());
			}
		} else {
			LOGGER.info("StreamingQuoteStorageImpl.tradingSymbolListOnInstrumentTokenId(): ERROR: DB conn is null !!!");
		}
		// LOGGER.info("Exit
		// StreamingQuoteStorageImpl.tradingSymbolListOnInstrumentTokenId()");
		return symbolList;

	}

	@Override
	public void orderStatusSyncBetweenLocalAndMarket(String tradingSymbol, String transactionType, String quantity,
			String status, String tagId) {
		// LOGGER.info("Entry
		// StreamingQuoteStorageImpl.orderStatusSyncBetweenLocalAndMarket()");

		if (conn != null) {
			try {
				if (null != tagId && !"".equalsIgnoreCase(tagId) && !"dayOff".equalsIgnoreCase(tagId)) {

					Statement stmt = conn.createStatement();
					String openSql = "SELECT id FROM " + quoteTable + "_SignalNew where id =" + tagId + " and status!='"
							+ status + "' order by time desc,id desc";
					ResultSet openRs = stmt.executeQuery(openSql);
					int updateRequired = 0;
					while (openRs.next()) {
						updateRequired = openRs.getInt(1);
					}

					if (updateRequired != 0) {
						openSql = "UPDATE " + quoteTable + "_SignalNew SET status = ? where id = ? ";
						PreparedStatement prepStmt = conn.prepareStatement(openSql);
						prepStmt.setString(1, status);
						prepStmt.setInt(2, Integer.parseInt(tagId));
						prepStmt.executeUpdate();
						prepStmt.close();
					}
					stmt.close();
				}
			} catch (SQLException e) {
				LOGGER.info(
						"StreamingQuoteStorageImpl.orderStatusSyncBetweenLocalAndMarket(): ERROR: SQLException on fetching data from Table, cause: "
								+ e.getMessage() + ">>" + e.getCause());
			}
		} else {
			LOGGER.info("StreamingQuoteStorageImpl.orderStatusSyncBetweenLocalAndMarket(): ERROR: DB conn is null !!!");
		}
		// LOGGER.info("Exit
		// StreamingQuoteStorageImpl.orderStatusSyncBetweenLocalAndMarket()");
	}

	@Override
	public void fetchAllOrdersForDayOffActivity(ArrayList<Long> quoteStreamingInstrumentsArr) {

		if (conn != null) {
			try {
				Statement stmt = conn.createStatement();
				for (int index = 0; index < quoteStreamingInstrumentsArr.size(); index++) {
					int totalQ = 0;
					double tradeprice = 0.0;
					String openSql = "SELECT sum(Quantity) FROM " + quoteTable + "_SignalNew where InstrumentToken ='"
							+ quoteStreamingInstrumentsArr.get(index)
							+ "' and ProcessSignal='BUY' group by ProcessSignal";
					ResultSet openRs1 = stmt.executeQuery(openSql);

					while (openRs1.next()) {
						totalQ = openRs1.getInt(1);
					}
					openSql = "SELECT sum(Quantity) FROM " + quoteTable + "_SignalNew where InstrumentToken ='"
							+ quoteStreamingInstrumentsArr.get(index)
							+ "' and ProcessSignal='SELL' group by ProcessSignal";

					ResultSet openRs2 = stmt.executeQuery(openSql);
					while (openRs2.next()) {
						totalQ = totalQ - openRs2.getInt(1);
					}
					openSql = "SELECT close FROM " + quoteTable + "_Signalparams where InstrumentToken ='"
							+ quoteStreamingInstrumentsArr.get(index) + "' order by id desc limit 1";

					ResultSet openRs3 = stmt.executeQuery(openSql);
					while (openRs3.next()) {
						tradeprice = openRs3.getDouble(1);
					}
					if (totalQ > 0) {
						String sql = "INSERT INTO " + quoteTable + "_signalNew "
								+ "(time,instrumentToken,quantity,processSignal,status,TradePrice,signalParamKey) "
								+ "values(?,?,?,?,?,?,?)";
						PreparedStatement prepStmt = conn.prepareStatement(sql);

						prepStmt.setTimestamp(1, new Timestamp(Calendar.getInstance().getTime().getTime()));
						prepStmt.setString(2, quoteStreamingInstrumentsArr.get(index).toString());
						prepStmt.setString(4, "SELL");
						prepStmt.setString(3, totalQ + "");
						prepStmt.setString(5, "dayOff");
						prepStmt.setDouble(6, tradeprice);
						prepStmt.setDouble(7, 0.0);
						prepStmt.executeUpdate();
						prepStmt.close();

					} else if (totalQ < 0) {
						String sql = "INSERT INTO " + quoteTable + "_signalNew "
								+ "(time,instrumentToken,quantity,processSignal,status,TradePrice,signalParamKey) "
								+ "values(?,?,?,?,?,?,?)";
						PreparedStatement prepStmt = conn.prepareStatement(sql);

						prepStmt.setTimestamp(1, new Timestamp(Calendar.getInstance().getTime().getTime()));
						prepStmt.setString(2, quoteStreamingInstrumentsArr.get(index).toString());
						prepStmt.setString(4, "BUY");
						prepStmt.setString(3, (0 - totalQ) + "");
						prepStmt.setString(5, "dayOff");
						prepStmt.setDouble(6, tradeprice);
						prepStmt.setDouble(7, 0.0);
						prepStmt.executeUpdate();
						prepStmt.close();
					}

					totalQ = 0;
					tradeprice = 0.0;
					openSql = "SELECT sum(Quantity) FROM " + quoteTable + "_SignalNew1 where InstrumentToken ='"
							+ quoteStreamingInstrumentsArr.get(index)
							+ "' and ProcessSignal='BUY' group by ProcessSignal";
					openRs1 = stmt.executeQuery(openSql);

					while (openRs1.next()) {
						totalQ = openRs1.getInt(1);
					}
					openSql = "SELECT sum(Quantity) FROM " + quoteTable + "_SignalNew1 where InstrumentToken ='"
							+ quoteStreamingInstrumentsArr.get(index)
							+ "' and ProcessSignal='SELL' group by ProcessSignal";

					openRs2 = stmt.executeQuery(openSql);
					while (openRs2.next()) {
						totalQ = totalQ - openRs2.getInt(1);
					}
					openSql = "SELECT close FROM " + quoteTable + "_Signalparams where InstrumentToken ='"
							+ quoteStreamingInstrumentsArr.get(index) + "' order by id desc limit 1";

					openRs3 = stmt.executeQuery(openSql);
					while (openRs3.next()) {
						tradeprice = openRs3.getDouble(1);
					}
					if (totalQ > 0) {
						String sql = "INSERT INTO " + quoteTable + "_signalNew1 "
								+ "(time,instrumentToken,quantity,processSignal,status,TradePrice,signalParamKey) "
								+ "values(?,?,?,?,?,?,?)";
						PreparedStatement prepStmt = conn.prepareStatement(sql);

						prepStmt.setTimestamp(1, new Timestamp(Calendar.getInstance().getTime().getTime()));
						prepStmt.setString(2, quoteStreamingInstrumentsArr.get(index).toString());
						prepStmt.setString(4, "SELL");
						prepStmt.setString(3, totalQ + "");
						prepStmt.setString(5, "dayOff");
						prepStmt.setDouble(6, tradeprice);
						prepStmt.setDouble(7, 0.0);
						prepStmt.executeUpdate();
						prepStmt.close();

					} else if (totalQ < 0) {
						String sql = "INSERT INTO " + quoteTable + "_signalNew1 "
								+ "(time,instrumentToken,quantity,processSignal,status,TradePrice,signalParamKey) "
								+ "values(?,?,?,?,?,?,?)";
						PreparedStatement prepStmt = conn.prepareStatement(sql);

						prepStmt.setTimestamp(1, new Timestamp(Calendar.getInstance().getTime().getTime()));
						prepStmt.setString(2, quoteStreamingInstrumentsArr.get(index).toString());
						prepStmt.setString(4, "BUY");
						prepStmt.setString(3, (0 - totalQ) + "");
						prepStmt.setString(5, "dayOff");
						prepStmt.setDouble(6, tradeprice);
						prepStmt.setDouble(7, 0.0);
						prepStmt.executeUpdate();
						prepStmt.close();
					}

					totalQ = 0;
					tradeprice = 0.0;
					openSql = "SELECT sum(Quantity) FROM " + quoteTable + "_SignalNew2 where InstrumentToken ='"
							+ quoteStreamingInstrumentsArr.get(index)
							+ "' and ProcessSignal='BUY' group by ProcessSignal";
					openRs1 = stmt.executeQuery(openSql);

					while (openRs1.next()) {
						totalQ = openRs1.getInt(1);
					}
					openSql = "SELECT sum(Quantity) FROM " + quoteTable + "_SignalNew2 where InstrumentToken ='"
							+ quoteStreamingInstrumentsArr.get(index)
							+ "' and ProcessSignal='SELL' group by ProcessSignal";

					openRs2 = stmt.executeQuery(openSql);
					while (openRs2.next()) {
						totalQ = totalQ - openRs2.getInt(1);
					}
					openSql = "SELECT close FROM " + quoteTable + "_Signalparams where InstrumentToken ='"
							+ quoteStreamingInstrumentsArr.get(index) + "' order by id desc limit 1";

					openRs3 = stmt.executeQuery(openSql);
					while (openRs3.next()) {
						tradeprice = openRs3.getDouble(1);
					}
					if (totalQ > 0) {
						String sql = "INSERT INTO " + quoteTable + "_signalNew2 "
								+ "(time,instrumentToken,quantity,processSignal,status,TradePrice,signalParamKey) "
								+ "values(?,?,?,?,?,?,?)";
						PreparedStatement prepStmt = conn.prepareStatement(sql);

						prepStmt.setTimestamp(1, new Timestamp(Calendar.getInstance().getTime().getTime()));
						prepStmt.setString(2, quoteStreamingInstrumentsArr.get(index).toString());
						prepStmt.setString(4, "SELL");
						prepStmt.setString(3, totalQ + "");
						prepStmt.setString(5, "dayOff");
						prepStmt.setDouble(6, tradeprice);
						prepStmt.setDouble(7, 0.0);
						prepStmt.executeUpdate();
						prepStmt.close();

					} else if (totalQ < 0) {
						String sql = "INSERT INTO " + quoteTable + "_signalNew2 "
								+ "(time,instrumentToken,quantity,processSignal,status,TradePrice,signalParamKey) "
								+ "values(?,?,?,?,?,?,?)";
						PreparedStatement prepStmt = conn.prepareStatement(sql);

						prepStmt.setTimestamp(1, new Timestamp(Calendar.getInstance().getTime().getTime()));
						prepStmt.setString(2, quoteStreamingInstrumentsArr.get(index).toString());
						prepStmt.setString(4, "BUY");
						prepStmt.setString(3, (0 - totalQ) + "");
						prepStmt.setString(5, "dayOff");
						prepStmt.setDouble(6, tradeprice);
						prepStmt.setDouble(7, 0.0);
						prepStmt.executeUpdate();
						prepStmt.close();
					}

					totalQ = 0;
					tradeprice = 0.0;
					openSql = "SELECT sum(Quantity) FROM " + quoteTable + "_SignalNew3 where InstrumentToken ='"
							+ quoteStreamingInstrumentsArr.get(index)
							+ "' and ProcessSignal='BUY' group by ProcessSignal";
					openRs1 = stmt.executeQuery(openSql);

					while (openRs1.next()) {
						totalQ = openRs1.getInt(1);
					}
					openSql = "SELECT sum(Quantity) FROM " + quoteTable + "_SignalNew3 where InstrumentToken ='"
							+ quoteStreamingInstrumentsArr.get(index)
							+ "' and ProcessSignal='SELL' group by ProcessSignal";

					openRs2 = stmt.executeQuery(openSql);
					while (openRs2.next()) {
						totalQ = totalQ - openRs2.getInt(1);
					}
					openSql = "SELECT close FROM " + quoteTable + "_Signalparams where InstrumentToken ='"
							+ quoteStreamingInstrumentsArr.get(index) + "' order by id desc limit 1";

					openRs3 = stmt.executeQuery(openSql);
					while (openRs3.next()) {
						tradeprice = openRs3.getDouble(1);
					}
					if (totalQ > 0) {
						String sql = "INSERT INTO " + quoteTable + "_signalNew3 "
								+ "(time,instrumentToken,quantity,processSignal,status,TradePrice,signalParamKey) "
								+ "values(?,?,?,?,?,?,?)";
						PreparedStatement prepStmt = conn.prepareStatement(sql);

						prepStmt.setTimestamp(1, new Timestamp(Calendar.getInstance().getTime().getTime()));
						prepStmt.setString(2, quoteStreamingInstrumentsArr.get(index).toString());
						prepStmt.setString(4, "SELL");
						prepStmt.setString(3, totalQ + "");
						prepStmt.setString(5, "dayOff");
						prepStmt.setDouble(6, tradeprice);
						prepStmt.setDouble(7, 0.0);
						prepStmt.executeUpdate();
						prepStmt.close();

					} else if (totalQ < 0) {
						String sql = "INSERT INTO " + quoteTable + "_signalNew3 "
								+ "(time,instrumentToken,quantity,processSignal,status,TradePrice,signalParamKey) "
								+ "values(?,?,?,?,?,?,?)";
						PreparedStatement prepStmt = conn.prepareStatement(sql);

						prepStmt.setTimestamp(1, new Timestamp(Calendar.getInstance().getTime().getTime()));
						prepStmt.setString(2, quoteStreamingInstrumentsArr.get(index).toString());
						prepStmt.setString(4, "BUY");
						prepStmt.setString(3, (0 - totalQ) + "");
						prepStmt.setString(5, "dayOff");
						prepStmt.setDouble(6, tradeprice);
						prepStmt.setDouble(7, 0.0);
						prepStmt.executeUpdate();
						prepStmt.close();
					}

					totalQ = 0;
					tradeprice = 0.0;
					openSql = "SELECT sum(Quantity) FROM " + quoteTable + "_SignalNew4 where InstrumentToken ='"
							+ quoteStreamingInstrumentsArr.get(index)
							+ "' and ProcessSignal='BUY' group by ProcessSignal";
					openRs1 = stmt.executeQuery(openSql);

					while (openRs1.next()) {
						totalQ = openRs1.getInt(1);
					}
					openSql = "SELECT sum(Quantity) FROM " + quoteTable + "_SignalNew4 where InstrumentToken ='"
							+ quoteStreamingInstrumentsArr.get(index)
							+ "' and ProcessSignal='SELL' group by ProcessSignal";

					openRs2 = stmt.executeQuery(openSql);
					while (openRs2.next()) {
						totalQ = totalQ - openRs2.getInt(1);
					}
					openSql = "SELECT close FROM " + quoteTable + "_Signalparams where InstrumentToken ='"
							+ quoteStreamingInstrumentsArr.get(index) + "' order by id desc limit 1";

					openRs3 = stmt.executeQuery(openSql);
					while (openRs3.next()) {
						tradeprice = openRs3.getDouble(1);
					}
					if (totalQ > 0) {
						String sql = "INSERT INTO " + quoteTable + "_signalNew4 "
								+ "(time,instrumentToken,quantity,processSignal,status,TradePrice,signalParamKey) "
								+ "values(?,?,?,?,?,?,?)";
						PreparedStatement prepStmt = conn.prepareStatement(sql);

						prepStmt.setTimestamp(1, new Timestamp(Calendar.getInstance().getTime().getTime()));
						prepStmt.setString(2, quoteStreamingInstrumentsArr.get(index).toString());
						prepStmt.setString(4, "SELL");
						prepStmt.setString(3, totalQ + "");
						prepStmt.setString(5, "dayOff");
						prepStmt.setDouble(6, tradeprice);
						prepStmt.setDouble(7, 0.0);
						prepStmt.executeUpdate();
						prepStmt.close();

					} else if (totalQ < 0) {
						String sql = "INSERT INTO " + quoteTable + "_signalNew4 "
								+ "(time,instrumentToken,quantity,processSignal,status,TradePrice,signalParamKey) "
								+ "values(?,?,?,?,?,?,?)";
						PreparedStatement prepStmt = conn.prepareStatement(sql);

						prepStmt.setTimestamp(1, new Timestamp(Calendar.getInstance().getTime().getTime()));
						prepStmt.setString(2, quoteStreamingInstrumentsArr.get(index).toString());
						prepStmt.setString(4, "BUY");
						prepStmt.setString(3, (0 - totalQ) + "");
						prepStmt.setString(5, "dayOff");
						prepStmt.setDouble(6, tradeprice);
						prepStmt.setDouble(7, 0.0);
						prepStmt.executeUpdate();
						prepStmt.close();
					}
				}
				stmt.close();
			} catch (SQLException e) {
				LOGGER.info(
						"StreamingQuoteStorageImpl.getOrderListToPlace(): ERROR: SQLException on fetching data from Table, cause: "
								+ e.getMessage() + ">>" + e.getCause());
			}
		} else {
			LOGGER.info("StreamingQuoteStorageImpl.getOrderListToPlace(): ERROR: DB conn is null !!!");
		}
	}
}
