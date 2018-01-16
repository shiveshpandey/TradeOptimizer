package com.gold.buzzer.dao;

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

import com.gold.buzzer.models.GoldBuzz;
import com.gold.buzzer.models.GoldBuzzSignal;
import com.gold.buzzer.models.Instrument;
import com.gold.buzzer.models.InstrumentOHLCData;
import com.gold.buzzer.models.InstrumentVolatilityScore;
import com.gold.buzzer.models.Order;
import com.gold.buzzer.models.Tick;
import com.gold.buzzer.utils.StreamingConfig;

public class StreamingQuoteStorageImpl implements StreamingQuoteStorage {
	private final static Logger LOGGER = Logger.getLogger(StreamingQuoteStorageImpl.class.getName());

	private static final String JDBC_DRIVER = "com.mysql.jdbc.Driver";
	private static final String DB_URL = StreamingConfig.QUOTE_STREAMING_DB_URL;

	private static final String USER = StreamingConfig.QUOTE_STREAMING_DB_USER;
	private static final String PASS = StreamingConfig.QUOTE_STREAMING_DB_PWD;
	private DateFormat dtTmFmt = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
	private Connection conn = null;

	private static String quoteTable = null;
	private static HashMap<String, GoldBuzz> goldBuzzList = new HashMap<String, GoldBuzz>();

	@Override
	public void initializeJDBCConn() {
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
	public void createDaysStreamingQuoteTable() throws SQLException {
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
				sql = "CREATE TABLE " + quoteTable + "_instrumentDetails "
						+ "(ID int NOT NULL AUTO_INCREMENT,time timestamp, instrumentToken varchar(32),dailyVolatility DECIMAL(20,4),"
						+ "annualVolatility DECIMAL(20,4),currentVolatility DECIMAL(20,4),PriorityPoint DECIMAL(20,4),tradable varchar(32),exchangeToken varchar(32),"
						+ "tradingsymbol varchar(32) ,name varchar(32) , last_price varchar(32) ,tickSize varchar(32), expiry varchar(32),"
						+ "instrumentType varchar(32), segment varchar(32) ,exchange varchar(32), strike varchar(32) ,lotSize varchar(32) ,"
						+ "daily2AvgVolatility DECIMAL(20,4), daily3AvgVolatility DECIMAL(20,4),daily5AvgVolatility DECIMAL(20,4), daily10AvgVolatility DECIMAL(20,4),"
						+ "dailyWtdVolatility DECIMAL(20,4),lastTradedQt DECIMAL(20,4),lastDeliveryQt DECIMAL(20,4),deliveryToTradeRatio DECIMAL(20,4),"
						+ "last2AvgTradedQt DECIMAL(20,4), last2AvgDeliveryQt DECIMAL(20,4), deliveryToTrade2AvgRatio DECIMAL(20,4),last3AvgTradedQt DECIMAL(20,4),"
						+ "last3AvgDeliveryQt DECIMAL(20,4), deliveryToTrade3AvgRatio DECIMAL(20,4),last5AvgTradedQt DECIMAL(20,4), last5AvgDeliveryQt DECIMAL(20,4),"
						+ "deliveryToTrade5AvgRatio DECIMAL(20,4),last10AvgTradedQt DECIMAL(20,4),last10AvgDeliveryQt DECIMAL(20,4),deliveryToTrade10AvgRatio DECIMAL(20,4),"
						+ "lastWtdTradedQt DECIMAL(20,4),lastWtdDeliveryQt DECIMAL(20,4), deliveryToTradeWtdRatio DECIMAL(20,4),lastclose DECIMAL(20,4),lasthigh DECIMAL(20,4),"
						+ "lastlow DECIMAL(20,4),lastopen DECIMAL(20,4),last2Avgclose DECIMAL(20,4),last2Avghigh DECIMAL(20,4),last2Avglow DECIMAL(20,4),last2Avgopen DECIMAL(20,4),"
						+ "last3Avgclose DECIMAL(20,4), last3Avghigh DECIMAL(20,4), last3Avglow DECIMAL(20,4), last3Avgopen DECIMAL(20,4),last5Avgclose DECIMAL(20,4),"
						+ "last5Avghigh DECIMAL(20,4), last5Avglow DECIMAL(20,4), last5Avgopen DECIMAL(20,4),last10Avgclose DECIMAL(20,4), last10Avghigh DECIMAL(20,4),"
						+ "last10Avglow DECIMAL(20,4), last10Avgopen DECIMAL(20,4),lastwtdAvgclose DECIMAL(20,4), lastwtdAvghigh DECIMAL(20,4), lastwtdAvglow DECIMAL(20,4),"
						+ "lastwtdAvgopen DECIMAL(20,4),weightHighMinusLow DECIMAL(20,4),HighMinusLow DECIMAL(20,4),cama_pp  DECIMAL(20,4),cama_h1 DECIMAL(20,4),"
						+ "cama_h2 DECIMAL(20,4),cama_h3 DECIMAL(20,4),cama_h4 DECIMAL(20,4),cama_l1 DECIMAL(20,4),cama_l2 DECIMAL(20,4),cama_l3 DECIMAL(20,4),"
						+ "cama_l4 DECIMAL(20,4), PRIMARY KEY (ID)) ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARSET=utf8;";
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
						+ "TradePrice DECIMAL(20,4),signalParamKey int,SignalLevel varchar(32)) "
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
	}

	@SuppressWarnings("deprecation")
	@Override
	public void storeTickData(ArrayList<Tick> ticks) {
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
								"StreamingQuoteStorageImpl.storeTickData(): ERROR: SQLException on Storing data to Table: "
										+ tick);
						LOGGER.info("StreamingQuoteStorageImpl.storeTickData(): [SQLException Cause]: " + e.getMessage()
								+ ">>" + e.getCause());
					}
				}
				prepStmt.close();
			} catch (SQLException e) {
				LOGGER.info("StreamingQuoteStorageImpl.storeTickData(): ERROR: SQLException on Storing data to Table: "
						+ ticks);
				LOGGER.info("StreamingQuoteStorageImpl.storeTickData(): [SQLException Cause]: " + e.getMessage() + ">>"
						+ e.getCause());
			}
		} else {
			if (conn != null) {
				LOGGER.info("StreamingQuoteStorageImpl.storeTickData(): ERROR: DB conn is null !!!");
			} else {
				LOGGER.info(
						"StreamingQuoteStorageImpl.storeTickData(): ERROR: quote is not of type StreamingQuoteModeQuote !!!");
			}
		}
	}

	@Override
	public ArrayList<Long> getTopPrioritizedTokenList(int i) {

		ArrayList<Long> instrumentList = new ArrayList<Long>();
		if (conn != null) {
			try {
				Statement stmt = conn.createStatement();
				String openSql = "SELECT InstrumentToken FROM " + quoteTable
						+ "_instrumentDetails where tradable='tradable' and lotsize != '0' and lotsize != 'null' and ((PriorityPoint > 0.0 and dailyWtdVolatility >= 1.91)"
						+ " or dailyWtdVolatility >= 2.09) and lastclose > 50.0 and lastclose < 2000.0 ORDER BY dailyWtdVolatility DESC,PriorityPoint DESC,id desc LIMIT "
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

		calculateAndCacheGoldBuzzLevelData(instrumentList);

		return instrumentList;
	}

	private void calculateAndCacheGoldBuzzLevelData(ArrayList<Long> instrumentList) {

		if (conn != null) {
			try {
				Statement stmt = conn.createStatement();
				String openSql = "SELECT instrumentToken,lastwtdAvgclose, weightHighMinusLow, cama_pp FROM "
						+ quoteTable + "_instrumentDetails where instrumentToken in ("
						+ commaSeperatedLongIDs(instrumentList) + ")";
				ResultSet openRs = stmt.executeQuery(openSql);

				while (openRs.next()) {
					GoldBuzz cama = new GoldBuzz();

					cama.setCamaPP(openRs.getDouble("cama_pp"));
					cama.setCamaH1(openRs.getDouble("cama_pp") + (0.11 * openRs.getDouble("weightHighMinusLow")));
					cama.setCamaH2(openRs.getDouble("cama_pp") + (0.2 * openRs.getDouble("weightHighMinusLow")));
					cama.setCamaH3(openRs.getDouble("cama_pp") + (0.29 * openRs.getDouble("weightHighMinusLow")));
					cama.setCamaH4(openRs.getDouble("cama_pp") + (0.38 * openRs.getDouble("weightHighMinusLow")));
					cama.setCamaH5(openRs.getDouble("cama_pp") + (0.55 * openRs.getDouble("weightHighMinusLow")));
					cama.setCamaL1(openRs.getDouble("cama_pp") - (0.11 * openRs.getDouble("weightHighMinusLow")));
					cama.setCamaL2(openRs.getDouble("cama_pp") - (0.2 * openRs.getDouble("weightHighMinusLow")));
					cama.setCamaL3(openRs.getDouble("cama_pp") - (0.29 * openRs.getDouble("weightHighMinusLow")));
					cama.setCamaL4(openRs.getDouble("cama_pp") - (0.38 * openRs.getDouble("weightHighMinusLow")));
					cama.setCamaL5(openRs.getDouble("cama_pp") - (0.55 * openRs.getDouble("weightHighMinusLow")));

					goldBuzzList.put(openRs.getString("instrumentToken"), cama);
				}
				stmt.close();
			} catch (SQLException e) {
				LOGGER.info(
						"StreamingQuoteStorageImpl.calculateAndCacheGoldBuzzLevelData(): ERROR: SQLException on fetching data from Table, cause: "
								+ e.getMessage() + ">>" + e.getCause());
			}
		} else {
			LOGGER.info("StreamingQuoteStorageImpl.calculateAndCacheGoldBuzzLevelData(): ERROR: DB conn is null !!!");
		}
	}

	@Override
	public List<Order> fetchOrderListToPlace() {
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
					order.tradingSymbol = tradingSymbolOnInstrumentToken(openRs.getString(1));
					order.transactionType = String.valueOf(openRs.getString(2));
					order.quantity = String.valueOf(openRs.getString(3));
					order.price = String.valueOf(openRs.getString(6));
					order.tag = String.valueOf(openRs.getInt(5));
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
						"StreamingQuoteStorageImpl.fetchOrderListToPlace(): ERROR: SQLException on fetching data from Table, cause: "
								+ e.getMessage() + ">>" + e.getCause());
			}
		} else

		{
			LOGGER.info("StreamingQuoteStorageImpl.fetchOrderListToPlace(): ERROR: DB conn is null !!!");
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
	}

	private String lotSizeOnInstrumentToken(String instrumentToken, String buyOrSell) throws SQLException {
		String lotSize = "";
		Statement stmt = conn.createStatement();
		int totalQuantityProcessed = 0, unitQuantity = 0;
		String openSql = "SELECT lotSize FROM " + quoteTable + "_instrumentDetails where instrumentToken='"
				+ instrumentToken + "'";
		ResultSet openRs = stmt.executeQuery(openSql);

		while (openRs.next()) {
			lotSize = openRs.getString(1);
			unitQuantity = Integer.parseInt(lotSize);
		}
		totalQuantityProcessed = fetchOrderedHistQuantity(instrumentToken);

		if ("SELL".equalsIgnoreCase(buyOrSell) && totalQuantityProcessed < 0) {
			lotSize = (totalQuantityProcessed + "").replaceAll("-", "");
			if (unitQuantity * 3 < totalQuantityProcessed * (-1))
				lotSize = "";
		} else if ("BUY".equalsIgnoreCase(buyOrSell) && totalQuantityProcessed > 0) {
			lotSize = totalQuantityProcessed + "";
			if (unitQuantity * 3 < totalQuantityProcessed)
				lotSize = "";
		} else if ("SQUAREOFF".equalsIgnoreCase(buyOrSell))
			lotSize = totalQuantityProcessed + "";
		else if ("SELL".equalsIgnoreCase(buyOrSell) && totalQuantityProcessed > 0) {
			lotSize = totalQuantityProcessed + "";
			if (unitQuantity * 3 < totalQuantityProcessed)
				lotSize = "";
		} else if ("BUY".equalsIgnoreCase(buyOrSell) && totalQuantityProcessed < 0) {
			lotSize = (totalQuantityProcessed + "").replaceAll("-", "");
			if (unitQuantity * 3 < totalQuantityProcessed * (-1))
				lotSize = "";
		}

		stmt.close();
		return lotSize;
	}

	private String tradingSymbolOnInstrumentToken(String instrumentToken) throws SQLException {
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

	@SuppressWarnings("deprecation")
	@Override
	public void calculateAndSaveStrategy(String instrumentToken, Date endingTimeToMatch) {
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

					String sql = "INSERT INTO " + quoteTable + "_signalParams "
							+ "(Time,InstrumentToken,high,low,close,timestampGrp) values(?,?,?,?,?,?)";
					PreparedStatement prepStmtInsertSignalParams = conn.prepareStatement(sql);

					prepStmtInsertSignalParams.setTimestamp(1,
							new Timestamp(dtTmFmt.parse(dtTmFmt.format(Calendar.getInstance().getTime())).getTime()));
					prepStmtInsertSignalParams.setTimestamp(6, timeStampPeriodList.get(timeLoop));
					prepStmtInsertSignalParams.setString(2, instrumentToken);
					prepStmtInsertSignalParams.setDouble(3, high);
					prepStmtInsertSignalParams.setDouble(4, low);
					prepStmtInsertSignalParams.setDouble(5, close);

					prepStmtInsertSignalParams.executeUpdate();

					prepStmtInsertSignalParams.close();
					timeLoopRsStmt.close();
				}

				if (null != idsList && idsList.size() > 0) {
					executeGoldBuzzStrategy(instrumentToken);

					// put it on day end
					executeGoldBuzzStrategyCloseRoundOff(instrumentToken);
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
						"StreamingQuoteStorageImpl.calculateAndSaveStrategy(): ERROR: SQLException on fetching data from Table, cause: "
								+ e.getMessage() + ">>" + e.getCause());
				e.printStackTrace();
			}
		} else {
			LOGGER.info("StreamingQuoteStorageImpl.calculateAndSaveStrategy(): ERROR: DB conn is null !!!");
		}
	}

	@Override
	public void executeGoldBuzzStrategyCloseRoundOff(String instrumentToken) throws SQLException {

		String openSql = "SELECT close FROM " + quoteTable + "_SignalParams where InstrumentToken ='" + instrumentToken
				+ "' order by id desc LIMIT 1 ";
		Statement stmt1 = conn.createStatement();
		ResultSet rs1 = stmt1.executeQuery(openSql);
		double firstClose = 0.0;
		while (rs1.next()) {
			firstClose = rs1.getDouble("close");
		}

		stmt1.close();

		String sql = "INSERT INTO " + quoteTable + "_Signal "
				+ "(time,instrumentToken,quantity,processSignal,status,TradePrice,signalParamKey,SignalLevel) "
				+ "values(?,?,?,?,?,?,?,?)";
		PreparedStatement prepStmt = conn.prepareStatement(sql);

		prepStmt.setTimestamp(1, new Timestamp(Calendar.getInstance().getTime().getTime()));
		prepStmt.setString(2, instrumentToken);
		String temp1 = lotSizeOnInstrumentToken(instrumentToken, "SQUAREOFF");
		if (!"".equalsIgnoreCase(temp1)) {
			int q = Integer.parseInt(temp1);
			if (q > 0) {
				prepStmt.setString(4, "SELLOFFROUND");
				prepStmt.setString(3, q + "");
			} else if (q == 0) {
				temp1 = "";
			} else {
				prepStmt.setString(4, "BUYOFFROUND");
				prepStmt.setString(3, (q + "").replaceAll("-", ""));
			}
		}

		prepStmt.setString(5, "active");
		prepStmt.setDouble(6, firstClose);
		prepStmt.setDouble(7, 0.0);
		prepStmt.setString(8, "ROUND");
		if (!"".equalsIgnoreCase(temp1)) {
			prepStmt.executeUpdate();
		}
		prepStmt.close();
	}

	private void executeGoldBuzzStrategy(String instrumentToken) throws SQLException {

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
					+ "_SignalParams where InstrumentToken ='" + instrumentToken + "' and id>" + id
					+ " ORDER BY id ASC LIMIT 2)  a order by id desc";
			Statement stmt = conn.createStatement();
			ResultSet rs = stmt.executeQuery(openSql);
			double firstClose = StreamingConfig.MAX_VALUE, secondClose = StreamingConfig.MAX_VALUE;
			boolean firstRecord = true;

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
				secondClose = rs.getDouble("close");
			}
			stmt.close();

			GoldBuzz goldBuzz = goldBuzzList.get(instrumentToken);

			GoldBuzzSignal signalClose = calculateGoldBuzzSignal(goldBuzz, firstClose, secondClose, instrumentToken);

			if (loopSize == 2 && !"usedForZigZagSignal1".equalsIgnoreCase(isUnUsedRecord) && null != signalClose
					&& signalClose.getSignal() != 1 && !firstRecord && firstClose != StreamingConfig.MAX_VALUE
					&& firstClose != 0.0) {

				saveGoldBuzzSignal(instrumentToken, signalClose, firstClose, firstRowId);

				Statement stmtForUpdate = conn.createStatement();
				if (lastRowId > 0) {
					String sql = "update " + quoteTable
							+ "_SignalParams set usedForZigZagSignal1 ='usedForZigZagSignal1' where id in(" + lastRowId
							+ ")";
					stmtForUpdate.executeUpdate(sql);
				}
				stmtForUpdate.close();
			} else if (loopSize == 2) {
				Statement stmtForUpdate = conn.createStatement();
				if (lastRowId > 0) {
					String sql = "update " + quoteTable
							+ "_SignalParams set usedForZigZagSignal1 ='usedForZigZagSignal1' where id in(" + lastRowId
							+ ")";
					stmtForUpdate.executeUpdate(sql);
				}
				stmtForUpdate.close();
			}

		} while (loopSize == 2);
		LOGGER.info("Exit		 StreamingQuoteStorageImpl.goldBuzzStrategy()");
	}

	private void saveGoldBuzzSignal(String instrumentToken, GoldBuzzSignal signalClose, double firstClose,
			double firstRowId) throws SQLException {

		String sql = "INSERT INTO " + quoteTable + "_Signal "
				+ "(time,instrumentToken,quantity,processSignal,status,TradePrice,signalParamKey,SignalLevel) "
				+ "values(?,?,?,?,?,?,?,?)";
		PreparedStatement prepStmt = conn.prepareStatement(sql);

		prepStmt.setTimestamp(1, new Timestamp(Calendar.getInstance().getTime().getTime()));
		prepStmt.setString(2, instrumentToken);
		String temp1 = "";
		if (signalClose.getSignal() == 2) {
			prepStmt.setString(4, "BUY");
			temp1 = lotSizeOnInstrumentToken(instrumentToken, "BUY");
			prepStmt.setString(3, temp1);
		} else if (signalClose.getSignal() == 0) {
			prepStmt.setString(4, "SELL");
			temp1 = lotSizeOnInstrumentToken(instrumentToken, "SELL");
			prepStmt.setString(3, temp1);
		} else {
			temp1 = lotSizeOnInstrumentToken(instrumentToken, "SQUAREOFF");
			if (!"".equalsIgnoreCase(temp1)) {
				int q = Integer.parseInt(temp1);
				if (q > 0) {
					prepStmt.setString(4, "SELLOFF");
					prepStmt.setString(3, q + "");
				} else if (q == 0) {
					temp1 = "";
				} else {
					prepStmt.setString(4, "BUYOFF");
					prepStmt.setString(3, (q + "").replaceAll("-", ""));
				}
			}
		}
		prepStmt.setString(5, "active");
		prepStmt.setDouble(6, firstClose);
		prepStmt.setDouble(7, firstRowId);
		prepStmt.setString(8, signalClose.getSignalLevel());
		if (!"".equalsIgnoreCase(temp1)) {
			prepStmt.executeUpdate();
		}
		prepStmt.close();

	}

	private GoldBuzzSignal calculateGoldBuzzSignal(GoldBuzz goldBuzz, double firstClose, double secondClose,
			String instrumentToken) throws SQLException {
		GoldBuzzSignal signalClose;

		if (firstClose > secondClose) {
			if (firstClose > goldBuzz.getCamaPP() && firstClose <= goldBuzz.getCamaH1()) {
				signalClose = new GoldBuzzSignal(0, "H0");
			} else if (firstClose > goldBuzz.getCamaH1() && firstClose <= goldBuzz.getCamaH2()) {
				signalClose = new GoldBuzzSignal(0, "H1");
			} else if (firstClose > goldBuzz.getCamaH2() && firstClose <= goldBuzz.getCamaH3()) {
				signalClose = new GoldBuzzSignal(0, "H2");
			} else if (firstClose > goldBuzz.getCamaH3() && firstClose <= goldBuzz.getCamaH4()) {
				signalClose = new GoldBuzzSignal(0, "H3");
			} else if (firstClose > goldBuzz.getCamaH4() && firstClose <= goldBuzz.getCamaH5()) {
				signalClose = new GoldBuzzSignal(0, "H4");
			} else if (firstClose > goldBuzz.getCamaH5()) {
				signalClose = new GoldBuzzSignal(-1, "H5");

			} else if (firstClose < goldBuzz.getCamaPP() && firstClose >= goldBuzz.getCamaL1()) {
				signalClose = new GoldBuzzSignal(1, "L0");
			} else if (firstClose < goldBuzz.getCamaL1() && firstClose >= goldBuzz.getCamaL2()) {
				signalClose = new GoldBuzzSignal(1, "L1");
			} else if (firstClose < goldBuzz.getCamaL2() && firstClose >= goldBuzz.getCamaL3()) {
				signalClose = new GoldBuzzSignal(1, "L2");
			} else if (firstClose < goldBuzz.getCamaL3() && firstClose >= goldBuzz.getCamaL4()) {
				signalClose = new GoldBuzzSignal(1, "L3");
			} else if (firstClose < goldBuzz.getCamaL4() && firstClose >= goldBuzz.getCamaL5()) {
				signalClose = new GoldBuzzSignal(1, "L4");
			} else if (firstClose < goldBuzz.getCamaL5()) {
				signalClose = new GoldBuzzSignal(-1, "L5");

			} else
				signalClose = new GoldBuzzSignal(1, "");
		} else if (secondClose > firstClose) {

			if (firstClose > goldBuzz.getCamaPP() && firstClose <= goldBuzz.getCamaH1()) {
				signalClose = new GoldBuzzSignal(1, "H0");
			} else if (firstClose > goldBuzz.getCamaH1() && firstClose <= goldBuzz.getCamaH2()) {
				signalClose = new GoldBuzzSignal(1, "H1");
			} else if (firstClose > goldBuzz.getCamaH2() && firstClose <= goldBuzz.getCamaH3()) {
				signalClose = new GoldBuzzSignal(1, "H2");
			} else if (firstClose > goldBuzz.getCamaH3() && firstClose <= goldBuzz.getCamaH4()) {
				signalClose = new GoldBuzzSignal(1, "H3");
			} else if (firstClose > goldBuzz.getCamaH4() && firstClose <= goldBuzz.getCamaH5()) {
				signalClose = new GoldBuzzSignal(1, "H4");
			} else if (firstClose > goldBuzz.getCamaH5()) {
				signalClose = new GoldBuzzSignal(-1, "H5");

			} else if (firstClose < goldBuzz.getCamaPP() && firstClose >= goldBuzz.getCamaL1()) {
				signalClose = new GoldBuzzSignal(2, "L0");
			} else if (firstClose < goldBuzz.getCamaL1() && firstClose >= goldBuzz.getCamaL2()) {
				signalClose = new GoldBuzzSignal(2, "L1");
			} else if (firstClose < goldBuzz.getCamaL2() && firstClose >= goldBuzz.getCamaL3()) {
				signalClose = new GoldBuzzSignal(2, "L2");
			} else if (firstClose < goldBuzz.getCamaL3() && firstClose >= goldBuzz.getCamaL4()) {
				signalClose = new GoldBuzzSignal(2, "L3");
			} else if (firstClose < goldBuzz.getCamaL4() && firstClose >= goldBuzz.getCamaL5()) {
				signalClose = new GoldBuzzSignal(2, "L4");
			} else if (firstClose < goldBuzz.getCamaL5()) {
				signalClose = new GoldBuzzSignal(-1, "L5");

			} else
				signalClose = new GoldBuzzSignal(1, "");
		} else
			signalClose = new GoldBuzzSignal(1, "");

		Statement stmt = conn.createStatement();

		String openSql = "SELECT processSignal,SignalLevel FROM " + quoteTable + "_Signal  where instrumentToken='"
				+ instrumentToken + "' order by id desc LIMIT 1";
		ResultSet openRs = stmt.executeQuery(openSql);
		String preSignal = "";
		String preSignalLevel = "";

		while (openRs.next()) {
			preSignal = openRs.getString(1);
			preSignalLevel = openRs.getString(2);
		}
		stmt.close();

		if ((("".equalsIgnoreCase(preSignalLevel) && "".equalsIgnoreCase(preSignal))) || signalClose.getSignal() == 1) {
			return signalClose;
		} else {
			int tempsignal1 = Integer.parseInt(preSignalLevel.substring(1));
			int tempsignal2 = Integer.parseInt(signalClose.getSignalLevel().substring(1));
			String tempsignalString1 = preSignalLevel.substring(0, 1);
			String tempsignalString2 = signalClose.getSignalLevel().substring(0, 1);
			int diff = Math.abs(tempsignal1 - tempsignal2);

			if ((tempsignal2 == 3 || tempsignal2 == 4) && !preSignalLevel.equalsIgnoreCase(signalClose.getSignalLevel())
					&& (diff == 1 || (diff == 0 && !tempsignalString1.equalsIgnoreCase(tempsignalString2)
							&& (tempsignal1 == 0 || tempsignal2 == 0)))) {

				if (preSignal.equalsIgnoreCase("BUY")// ||preSignal.equalsIgnoreCase("SELLOFF")
				)
					signalClose.setSignal(2);
				else if (preSignal.equalsIgnoreCase("SELL")// ||preSignal.equalsIgnoreCase("BUYOFF")
				)
					signalClose.setSignal(0);

				return signalClose;
			} else if (!preSignalLevel.equalsIgnoreCase(signalClose.getSignalLevel())
					&& (diff > 1 || (diff == 1 && !tempsignalString1.equalsIgnoreCase(tempsignalString2)
							&& (tempsignal1 == 0 || tempsignal2 == 0)))) {
				stmt = conn.createStatement();
				openSql = "SELECT processSignal,SignalLevel FROM " + quoteTable + "_Signal  where instrumentToken='"
						+ instrumentToken + "' order by id desc LIMIT 2";
				openRs = stmt.executeQuery(openSql);
				String[] preSignalArray = { "", "" };
				String[] preSignalLevelArray = { "", "" };
				int i = 0;
				tempsignal1 = 0;
				tempsignal2 = 0;
				while (openRs.next()) {
					preSignalArray[i] = openRs.getString(1);
					preSignalLevelArray[i] = openRs.getString(2);
					i++;
				}
				stmt.close();

				if (!"".equalsIgnoreCase(preSignalLevelArray[0]))
					tempsignal1 = Integer.parseInt(preSignalLevelArray[0].substring(1));
				if (!"".equalsIgnoreCase(preSignalLevelArray[1]))
					tempsignal2 = Integer.parseInt(preSignalLevelArray[1].substring(1));

				if (preSignalArray[0].equalsIgnoreCase(preSignalArray[1]) && (tempsignal1 == 3 || tempsignal1 == 4))
					signalClose.setSignal(-1);

				return signalClose;
			} else
				return null;
		}
	}

	@Override
	public void saveInstrumentTokenPriorityData(Map<String, InstrumentVolatilityScore> stocksSymbolArray) {

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
						"StreamingQuoteStorageImpl.saveInstrumentTokenPriorityData(): ERROR: SQLException on fetching data from Table, cause: "
								+ e.getMessage() + ">>" + e.getCause());
			}
		} else {
			LOGGER.info("StreamingQuoteStorageImpl.saveInstrumentTokenPriorityData(): ERROR: DB conn is null !!!");
		}
	}

	@Override
	public void saveInstrumentVolumeData(HashMap<String, ArrayList<InstrumentOHLCData>> stocksSymbolArray) {

		if (conn != null && stocksSymbolArray != null && stocksSymbolArray.size() > 0) {
			try {
				String sql = "UPDATE " + quoteTable
						+ "_instrumentDetails SET lastTradedQt = ?, lastDeliveryQt = ?, deliveryToTradeRatio = ?,"
						+ " last2AvgTradedQt = ?, last2AvgDeliveryQt = ?, deliveryToTrade2AvgRatio = ?,"
						+ " last3AvgTradedQt = ?, last3AvgDeliveryQt = ?, deliveryToTrade3AvgRatio = ?,"
						+ "last5AvgTradedQt = ?, last5AvgDeliveryQt = ?, deliveryToTrade5AvgRatio = ?,"
						+ "last10AvgTradedQt = ?, last10AvgDeliveryQt = ?, deliveryToTrade10AvgRatio = ?,"
						+ "lastWtdTradedQt = ?, lastWtdDeliveryQt = ?, deliveryToTradeWtdRatio = ? "
						+ " where tradingSymbol = ? ";

				PreparedStatement prepStmt = conn.prepareStatement(sql);
				Object[] keyList = stocksSymbolArray.keySet().toArray();
				for (int index = 0; index < stocksSymbolArray.size(); index++) {
					try {
						InstrumentOHLCData instrument = new InstrumentOHLCData("",
								stocksSymbolArray.get(keyList[index].toString()));

						prepStmt.setDouble(1, instrument.getClose());
						prepStmt.setDouble(2, instrument.getHigh());
						prepStmt.setDouble(3, instrument.getLow());
						prepStmt.setDouble(4, instrument.getAvg2Dayclose());
						prepStmt.setDouble(5, instrument.getAvg2Dayhigh());
						prepStmt.setDouble(6, instrument.getAvg2Daylow());
						prepStmt.setDouble(7, instrument.getAvg3Dayclose());
						prepStmt.setDouble(8, instrument.getAvg3Dayhigh());
						prepStmt.setDouble(9, instrument.getAvg3Daylow());
						prepStmt.setDouble(10, instrument.getAvg5Dayclose());
						prepStmt.setDouble(11, instrument.getAvg5Dayhigh());
						prepStmt.setDouble(12, instrument.getAvg5Daylow());
						prepStmt.setDouble(13, instrument.getAvg10Dayclose());
						prepStmt.setDouble(14, instrument.getAvg10Dayhigh());
						prepStmt.setDouble(15, instrument.getAvg10Daylow());
						prepStmt.setDouble(16, instrument.getAvgWaitedclose());
						prepStmt.setDouble(17, instrument.getAvgWaitedhigh());
						prepStmt.setDouble(18, instrument.getAvgWaitedlow());
						prepStmt.setString(19, instrument.getInstrumentName());

						prepStmt.executeUpdate();
					} catch (SQLException e) {
						LOGGER.info(
								"StreamingQuoteStorageImpl.saveInstrumentVolumeData(): ERROR: SQLException on fetching data from Table, cause: "
										+ e.getMessage() + ">>" + e.getCause());
					}
				}
				prepStmt.close();

			} catch (SQLException e) {
				LOGGER.info(
						"StreamingQuoteStorageImpl.saveInstrumentVolumeData(): ERROR: SQLException on fetching data from Table, cause: "
								+ e.getMessage() + ">>" + e.getCause());
			}
		} else {
			LOGGER.info("StreamingQuoteStorageImpl.saveInstrumentVolumeData(): ERROR: DB conn is null !!!");
		}
	}

	@Override
	public void saveInstrumentVolatilityData(
			HashMap<String, ArrayList<InstrumentOHLCData>> instrumentVolatilityScoreList) {

		if (conn != null && instrumentVolatilityScoreList != null && instrumentVolatilityScoreList.size() > 0) {
			try {
				Statement stmt = conn.createStatement();
				String openSql = "UPDATE " + quoteTable
						+ "_instrumentDetails SET dailyVolatility = ?, annualVolatility = ?, last_price = ?, lotSize = ?,"
						+ "daily2AvgVolatility = ?, daily3AvgVolatility = ?,daily5AvgVolatility = ?, daily10AvgVolatility = ?,"
						+ "dailyWtdVolatility = ? where tradingSymbol = ? ";
				PreparedStatement prepStmt = conn.prepareStatement(openSql);
				Object[] keyList = instrumentVolatilityScoreList.keySet().toArray();
				InstrumentVolatilityScore instrumentVolatilityScore;

				for (int index = 0; index < instrumentVolatilityScoreList.size(); index++) {
					try {
						InstrumentOHLCData instrument = new InstrumentOHLCData(1,
								instrumentVolatilityScoreList.get(keyList[index].toString()));

						instrumentVolatilityScore = new InstrumentVolatilityScore();
						instrumentVolatilityScore.setPrice(instrument.getClose());

						prepStmt.setDouble(1, instrument.getHigh());
						prepStmt.setDouble(2, instrument.getLow());
						prepStmt.setString(3, String.valueOf(instrument.getClose()));
						prepStmt.setString(4, String.valueOf(instrumentVolatilityScore.getLotSize()));
						prepStmt.setDouble(5, instrument.getAvg2Dayhigh());
						prepStmt.setDouble(6, instrument.getAvg3Dayhigh());
						prepStmt.setDouble(7, instrument.getAvg5Dayhigh());
						prepStmt.setDouble(8, instrument.getAvg10Dayhigh());
						prepStmt.setDouble(9, instrument.getAvgWaitedhigh());
						prepStmt.setString(10, instrument.getInstrumentName());

						prepStmt.executeUpdate();
					} catch (SQLException e) {
						LOGGER.info(
								"StreamingQuoteStorageImpl.saveInstrumentVolatilityData(): ERROR: SQLException on fetching data from Table, cause: "
										+ e.getMessage() + ">>" + e.getCause());
					}
				}
				prepStmt.close();
				stmt.close();
			} catch (SQLException e) {
				LOGGER.info(
						"StreamingQuoteStorageImpl.saveInstrumentVolatilityData(): ERROR: SQLException on fetching data from Table, cause: "
								+ e.getMessage() + ">>" + e.getCause());
			}
		} else {
			LOGGER.info("StreamingQuoteStorageImpl.saveInstrumentVolatilityData(): ERROR: DB conn is null !!!");
		}
	}

	@Override
	public void markInstrumentsTradable(List<InstrumentVolatilityScore> instrumentVolatilityScoreList) {

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
						"StreamingQuoteStorageImpl.markInstrumentsTradable(): ERROR: SQLException on fetching data from Table, cause: "
								+ e.getMessage() + ">>" + e.getCause());
			}
		} else {
			LOGGER.info("StreamingQuoteStorageImpl.markInstrumentsTradable(): ERROR: DB conn is null !!!");
		}
	}

	@Override
	public void saveBackendReadyFlag(boolean backendReadyForProcessing) {

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
	}

	@Override
	public boolean getBackendReadyFlag() {

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
		return backendReady;
	}

	@Override
	public ArrayList<String> tradingSymbolListOnInstrumentTokenId(ArrayList<Long> quoteStreamingInstrumentsArr) {

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
		return symbolList;
	}

	@Override
	public void orderStatusSyncBetweenLocalAndMarket(String tradingSymbol, String transactionType, String quantity,
			String status, String tagId) {

		if (conn != null) {
			try {
				if (null != tagId && !"".equalsIgnoreCase(tagId) && !"dayOff".equalsIgnoreCase(tagId)) {

					Statement stmt = conn.createStatement();
					String openSql = "SELECT id FROM " + quoteTable + "_Signal where id =" + tagId + " and status!='"
							+ status + "' order by time desc,id desc";
					ResultSet openRs = stmt.executeQuery(openSql);
					int updateRequired = 0;
					while (openRs.next()) {
						updateRequired = openRs.getInt(1);
					}

					if (updateRequired != 0) {
						openSql = "UPDATE " + quoteTable + "_Signal SET status = ? where id = ? ";
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
	}

	private int fetchOrderedHistQuantity(String quoteStreamingInstrumentsArr) throws SQLException {
		int totalQ = 0;
		if (conn != null) {
			Statement stmt = conn.createStatement();

			String openSql = "SELECT sum(Quantity) FROM " + quoteTable + "_Signal where InstrumentToken ='"
					+ quoteStreamingInstrumentsArr + "' and ProcessSignal like '%BUY%' group by ProcessSignal";
			ResultSet openRs = stmt.executeQuery(openSql);

			while (openRs.next()) {
				totalQ = totalQ + openRs.getInt(1);
			}
			openSql = "SELECT sum(Quantity) FROM " + quoteTable + "_Signal where InstrumentToken ='"
					+ quoteStreamingInstrumentsArr + "' and ProcessSignal like '%SELL%' group by ProcessSignal";

			openRs = stmt.executeQuery(openSql);
			while (openRs.next()) {
				totalQ = totalQ - openRs.getInt(1);
			}
		}
		return totalQ;
	}

	@Override
	public void saveLast10DaysOHLCData(HashMap<String, ArrayList<InstrumentOHLCData>> stocksSymbolArray) {
		if (conn != null && stocksSymbolArray != null && stocksSymbolArray.size() > 0) {
			try {
				String sql = "UPDATE " + quoteTable
						+ "_instrumentDetails SET lastclose = ?, lasthigh = ?, lastlow = ?, lastopen = ?,"
						+ "last2Avgclose = ?, last2Avghigh = ?, last2Avglow = ?, last2Avgopen = ?,"
						+ "last3Avgclose = ?, last3Avghigh = ?, last3Avglow = ?, last3Avgopen = ?,"
						+ "last5Avgclose = ?, last5Avghigh = ?, last5Avglow = ?, last5Avgopen = ?,"
						+ "last10Avgclose = ?, last10Avghigh = ?, last10Avglow = ?, last10Avgopen = ?,"
						+ "lastwtdAvgclose = ?, lastwtdAvghigh = ?, lastwtdAvglow = ?, lastwtdAvgopen = ?,"
						+ "weightHighMinusLow = ?, HighMinusLow = ?, cama_pp = ?, cama_h1 = ?, cama_h2 = ?,"
						+ "cama_h3 = ?, cama_h4 = ?, cama_l1 = ?, cama_l2 = ?, cama_l3 = ?, cama_l4 = ?"
						+ " where tradingSymbol = ? ";

				PreparedStatement prepStmt = conn.prepareStatement(sql);
				Object[] keyList = stocksSymbolArray.keySet().toArray();
				for (int index = 0; index < stocksSymbolArray.size(); index++) {
					try {
						InstrumentOHLCData instrument = new InstrumentOHLCData(
								stocksSymbolArray.get(keyList[index].toString()));

						prepStmt.setDouble(1, instrument.getClose());
						prepStmt.setDouble(2, instrument.getHigh());
						prepStmt.setDouble(3, instrument.getLow());
						prepStmt.setDouble(4, instrument.getOpen());
						prepStmt.setDouble(5, instrument.getAvg2Dayclose());
						prepStmt.setDouble(6, instrument.getAvg2Dayhigh());
						prepStmt.setDouble(7, instrument.getAvg2Daylow());
						prepStmt.setDouble(8, instrument.getAvg2DayOpen());
						prepStmt.setDouble(9, instrument.getAvg3Dayclose());
						prepStmt.setDouble(10, instrument.getAvg3Dayhigh());
						prepStmt.setDouble(11, instrument.getAvg3Daylow());
						prepStmt.setDouble(12, instrument.getAvg3Dayopen());
						prepStmt.setDouble(13, instrument.getAvg5Dayclose());
						prepStmt.setDouble(14, instrument.getAvg5Dayhigh());
						prepStmt.setDouble(15, instrument.getAvg5Daylow());
						prepStmt.setDouble(16, instrument.getAvg5Dayopen());
						prepStmt.setDouble(17, instrument.getAvg10Dayclose());
						prepStmt.setDouble(18, instrument.getAvg10Dayhigh());
						prepStmt.setDouble(19, instrument.getAvg10Daylow());
						prepStmt.setDouble(20, instrument.getAvg10Dayopen());
						prepStmt.setDouble(21, instrument.getAvgWaitedclose());
						prepStmt.setDouble(22, instrument.getAvgWaitedhigh());
						prepStmt.setDouble(23, instrument.getAvgWaitedlow());
						prepStmt.setDouble(24, instrument.getAvgWaitedOpen());
						prepStmt.setDouble(25, instrument.getWaitedHighMinusLow());
						prepStmt.setDouble(26, instrument.getHighMinusLow());
						prepStmt.setDouble(27, (instrument.getAvgWaitedclose() + instrument.getAvgWaitedhigh()
								+ instrument.getAvgWaitedlow()) / 3.0);
						prepStmt.setDouble(28, instrument.getAvgWaitedclose()
								+ instrument.getWaitedHighMinusLow() * StreamingConfig.CAMA_H1);
						prepStmt.setDouble(29, instrument.getAvgWaitedclose()
								+ instrument.getWaitedHighMinusLow() * StreamingConfig.CAMA_H2);
						prepStmt.setDouble(30, instrument.getAvgWaitedclose()
								+ instrument.getWaitedHighMinusLow() * StreamingConfig.CAMA_H3);
						prepStmt.setDouble(31, instrument.getAvgWaitedclose()
								+ instrument.getWaitedHighMinusLow() * StreamingConfig.CAMA_H4);
						prepStmt.setDouble(32, instrument.getAvgWaitedclose()
								- instrument.getWaitedHighMinusLow() * StreamingConfig.CAMA_L1);
						prepStmt.setDouble(33, instrument.getAvgWaitedclose()
								- instrument.getWaitedHighMinusLow() * StreamingConfig.CAMA_L2);
						prepStmt.setDouble(34, instrument.getAvgWaitedclose()
								- instrument.getWaitedHighMinusLow() * StreamingConfig.CAMA_L3);
						prepStmt.setDouble(35, instrument.getAvgWaitedclose()
								- instrument.getWaitedHighMinusLow() * StreamingConfig.CAMA_L4);

						prepStmt.setString(36, instrument.getInstrumentName());

						prepStmt.executeUpdate();
					} catch (SQLException e) {
						LOGGER.info(
								"StreamingQuoteStorageImpl.last10DaysOHLCData(): ERROR: SQLException on fetching data from Table, cause: "
										+ e.getMessage() + ">>" + e.getCause());
					}
				}
				prepStmt.close();

			} catch (SQLException e) {
				LOGGER.info(
						"StreamingQuoteStorageImpl.last10DaysOHLCData(): ERROR: SQLException on fetching data from Table, cause: "
								+ e.getMessage() + ">>" + e.getCause());
			}
		} else {
			LOGGER.info("StreamingQuoteStorageImpl.last10DaysOHLCData(): ERROR: DB conn is null !!!");
		}
	}
}