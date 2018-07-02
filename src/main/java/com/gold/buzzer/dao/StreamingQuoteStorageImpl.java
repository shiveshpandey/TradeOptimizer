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
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TimeZone;
import java.util.logging.Logger;

import com.gold.buzzer.models.GoldBuzz;
import com.gold.buzzer.models.GoldBuzzSignal;
import com.gold.buzzer.models.InstrumentOHLCData;
import com.gold.buzzer.models.InstrumentVolatilityScore;
import com.gold.buzzer.utils.StreamingConfig;
import com.zerodhatech.models.HistoricalData;
import com.zerodhatech.models.Instrument;
import com.zerodhatech.models.Order;
import com.zerodhatech.models.Tick;

public class StreamingQuoteStorageImpl implements StreamingQuoteStorage {
	private final static Logger LOGGER = Logger.getLogger(StreamingQuoteStorageImpl.class.getName());

	private static final String JDBC_DRIVER = "com.mysql.jdbc.Driver";
	private static final String DB_URL = StreamingConfig.QUOTE_STREAMING_DB_URL;

	private static final String USER = StreamingConfig.QUOTE_STREAMING_DB_USER;
	private static final String PASS = StreamingConfig.QUOTE_STREAMING_DB_PWD;
	private DateFormat dtTmFmt = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
	SimpleDateFormat histDataFmt = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss");

	private Connection conn = null;

	private static String quoteTable = null;
	private static String preQuoteTable = null;
	private static HashMap<String, GoldBuzz> goldBuzzList = new HashMap<String, GoldBuzz>();
	private static HashMap<String, Integer> trendPositiveList = new HashMap<String, Integer>();
	private static Set<String> forbiddenGoldBuzzList = new HashSet<String>();

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
			preQuoteTable = StreamingConfig.getPreStreamingQuoteTbNameAppendFormat(
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
						if (null != tick.getTickTimestamp())
							date = tick.getTickTimestamp();
						prepStmt.setTimestamp(1, new Timestamp(date.getTime()));
						prepStmt.setString(2, String.valueOf(tick.getInstrumentToken()));
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
				String openSql = "SELECT distinct InstrumentToken FROM " + quoteTable
						+ "_instrumentDetails ORDER BY dailyWtdVolatility DESC,PriorityPoint DESC,id desc LIMIT " + i
						+ "";
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

		// calculateAndCacheGoldBuzzLevelData(instrumentList);

		return instrumentList;
	}

	private void calculateAndCacheGoldBuzzLevelData(ArrayList<Long> instrumentList) {

		if (conn != null) {
			try {
				Statement stmt = conn.createStatement();
				String openSql = "SELECT instrumentToken,lastwtdAvgclose,GREATEST(last10avghigh-last10avglow,last2avghigh-last2avglow,"
						+ "last3avghigh-last3avglow,last5avghigh-last5avglow,HighMinusLow,weightHighMinusLow) as highMinusLow, (lastclose+lasthigh+lastlow)/3.0 as cama_pp FROM "
						+ quoteTable + "_instrumentDetails where instrumentToken in ("
						+ commaSeperatedLongIDs(instrumentList) + ")";
				ResultSet openRs = stmt.executeQuery(openSql);

				while (openRs.next()) {
					GoldBuzz cama = new GoldBuzz();

					cama.setCamaPP(openRs.getDouble("cama_pp"));
					cama.setCamaH1(
							openRs.getDouble("cama_pp") + (StreamingConfig.CAMA_H1 * openRs.getDouble("highMinusLow")));
					cama.setCamaH2(
							openRs.getDouble("cama_pp") + (StreamingConfig.CAMA_H2 * openRs.getDouble("highMinusLow")));
					cama.setCamaH3(
							openRs.getDouble("cama_pp") + (StreamingConfig.CAMA_H3 * openRs.getDouble("highMinusLow")));
					cama.setCamaH4(
							openRs.getDouble("cama_pp") + (StreamingConfig.CAMA_H4 * openRs.getDouble("highMinusLow")));
					cama.setCamaH5(
							openRs.getDouble("cama_pp") + (StreamingConfig.CAMA_H5 * openRs.getDouble("highMinusLow")));
					cama.setCamaL1(
							openRs.getDouble("cama_pp") - (StreamingConfig.CAMA_L1 * openRs.getDouble("highMinusLow")));
					cama.setCamaL2(
							openRs.getDouble("cama_pp") - (StreamingConfig.CAMA_L2 * openRs.getDouble("highMinusLow")));
					cama.setCamaL3(
							openRs.getDouble("cama_pp") - (StreamingConfig.CAMA_L3 * openRs.getDouble("highMinusLow")));
					cama.setCamaL4(
							openRs.getDouble("cama_pp") - (StreamingConfig.CAMA_L4 * openRs.getDouble("highMinusLow")));
					cama.setCamaL5(
							openRs.getDouble("cama_pp") - (StreamingConfig.CAMA_L5 * openRs.getDouble("highMinusLow")));

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
					prepStmt.setString(8, String.valueOf(instrument.getExpiry()));
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

	private String lotSizeOnInstrumentToken(String instrumentToken, String buyOrSell, boolean directionalMove)
			throws SQLException {
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

		if (directionalMove) {

		} else if ("SELL".equalsIgnoreCase(buyOrSell) && totalQuantityProcessed < 0) {
			lotSize = (unitQuantity + "").replaceAll("-", "");
			if (unitQuantity * 2 <= totalQuantityProcessed * (-1))
				lotSize = "";
		} else if ("BUY".equalsIgnoreCase(buyOrSell) && totalQuantityProcessed > 0) {
			lotSize = unitQuantity + "";
			if (unitQuantity * 2 <= totalQuantityProcessed)
				lotSize = "";
		} else if ("SQUAREOFF".equalsIgnoreCase(buyOrSell))
			lotSize = totalQuantityProcessed + "";
		else if ("SELL".equalsIgnoreCase(buyOrSell) && totalQuantityProcessed > 0) {
			lotSize = totalQuantityProcessed + "";
		} else if ("BUY".equalsIgnoreCase(buyOrSell) && totalQuantityProcessed < 0) {
			lotSize = (totalQuantityProcessed + "").replaceAll("-", "");
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

				/*
				 * Date endDateTime = dtTmFmt.parse(dtTmFmt.format(endingTimeToMatch));
				 * endDateTime.setSeconds(0); Timestamp endTime = new
				 * Timestamp(endDateTime.getTime());
				 * 
				 * ArrayList<Timestamp> timeStampPeriodList = new ArrayList<Timestamp>();
				 * ArrayList<Integer> idsList = new ArrayList<Integer>();
				 * 
				 * Statement timeStampRsStmt = conn.createStatement();
				 * 
				 * String openSql = "SELECT id FROM " + quoteTable + " where InstrumentToken ='"
				 * + instrumentToken + "' and usedForSignal != 'used' and timestampGrp <'" +
				 * endTime + "' ORDER BY Time ASC,id asc "; ResultSet timeStampRs =
				 * timeStampRsStmt.executeQuery(openSql); while (timeStampRs.next()) {
				 * idsList.add(timeStampRs.getInt(1)); } if (null != idsList && idsList.size() >
				 * 0) { openSql = "SELECT distinct timestampGrp FROM " + quoteTable +
				 * " where id in(" + commaSeperatedIDs(idsList) + ") ORDER BY Time ASC,id asc ";
				 * timeStampRs = timeStampRsStmt.executeQuery(openSql); while
				 * (timeStampRs.next()) {
				 * timeStampPeriodList.add(timeStampRs.getTimestamp("timestampGrp")); } }
				 * timeStampRsStmt.close();
				 * 
				 * for (int timeLoop = 0; timeLoop < timeStampPeriodList.size(); timeLoop++) {
				 * Double low = null; Double high = null; Double close = null;
				 * 
				 * Statement timeLoopRsStmt = conn.createStatement();
				 * 
				 * openSql = "SELECT LastTradedPrice FROM " + quoteTable +
				 * " where InstrumentToken ='" + instrumentToken + "' and timestampGrp ='" +
				 * timeStampPeriodList.get(timeLoop) + "' ORDER BY id DESC "; ResultSet
				 * openRsHighLowClose = timeLoopRsStmt.executeQuery(openSql); boolean
				 * firstRecord = true; Double lastTradedPrice = null; while
				 * (openRsHighLowClose.next()) { lastTradedPrice =
				 * openRsHighLowClose.getDouble("LastTradedPrice"); if (null == low ||
				 * lastTradedPrice < low) low = lastTradedPrice; if (null == high ||
				 * lastTradedPrice > high) high = lastTradedPrice; if (firstRecord) { close =
				 * lastTradedPrice; firstRecord = false; } } if (null == low || null == high ||
				 * null == close) continue;
				 * 
				 * String sql = "INSERT INTO " + quoteTable + "_signalParams " +
				 * "(Time,InstrumentToken,high,low,close,timestampGrp) values(?,?,?,?,?,?)";
				 * PreparedStatement prepStmtInsertSignalParams = conn.prepareStatement(sql);
				 * 
				 * prepStmtInsertSignalParams.setTimestamp(1, new
				 * Timestamp(dtTmFmt.parse(dtTmFmt.format(Calendar.getInstance().getTime())).
				 * getTime())); prepStmtInsertSignalParams.setTimestamp(6,
				 * timeStampPeriodList.get(timeLoop)); prepStmtInsertSignalParams.setString(2,
				 * instrumentToken); prepStmtInsertSignalParams.setDouble(3, high);
				 * prepStmtInsertSignalParams.setDouble(4, low);
				 * prepStmtInsertSignalParams.setDouble(5, close);
				 * 
				 * prepStmtInsertSignalParams.executeUpdate();
				 * 
				 * prepStmtInsertSignalParams.close(); timeLoopRsStmt.close(); }
				 */

				// if (null != idsList && idsList.size() > 0) {
				// executeBatchGoldBuzzStrategy(instrumentToken);
				executeBatchAOStrategy(instrumentToken);
				// executeGoldBuzzStrategy(instrumentToken);

				// put it on day end
				executeGoldBuzzStrategyCloseRoundOff(instrumentToken);
				// forbiddenGoldBuzzList.clear();
				// }

				/*
				 * if (null != idsList && idsList.size() > 0) { Statement usedUpdateRsStmt =
				 * conn.createStatement(); openSql = "update " + quoteTable +
				 * " set usedForSignal ='used' where id in(" + commaSeperatedIDs(idsList) + ")";
				 * usedUpdateRsStmt.executeUpdate(openSql); usedUpdateRsStmt.close(); }
				 */
			} catch (SQLException e) {
				LOGGER.info(
						"StreamingQuoteStorageImpl.calculateAndSaveStrategy(): ERROR: SQLException on fetching data from Table, cause: "
								+ e.getMessage() + ">>" + e.getCause());
				e.printStackTrace();
			}
		} else {
			LOGGER.info("StreamingQuoteStorageImpl.calculateAndSaveStrategy(): ERROR: DB conn is null !!!");
		}
	}

	private void executeBatchAOStrategy(String instrumentToken) throws SQLException {
		class PriceArray {
			Integer id;
			double price;
			double high;
			double low;
			double sma29;
			double sma11;
			GoldBuzzSignal goldBuzzSignal;
			double aoVal;
			double k11;
			double k20;
			double k29;

			void calculateSma11(List<PriceArray> priceArrayList) {
				for (int i = 0; i < priceArrayList.size(); i++) {
					this.sma11 += ((priceArrayList.get(i).low + priceArrayList.get(i).high) / 2);
				}
			}

			void calculateSma29(List<PriceArray> priceArrayList) {
				for (int i = 0; i < priceArrayList.size(); i++) {
					this.sma11 += ((priceArrayList.get(i).low + priceArrayList.get(i).high) / 2);
				}
			}

			void finalizeSma11() {
				this.sma11 = this.sma11 / 11;
			}

			void finalizeSma29() {
				this.sma29 = this.sma29 / 29;
			}

			void calculateK11(List<PriceArray> priceArrayList) {
				double high = 0.0;
				double low = 999999999.0;
				for (int j = 0; j < priceArrayList.size(); j++) {
					if (low > priceArrayList.get(j).low) {
						low = priceArrayList.get(j).low;
					}
					if (high < priceArrayList.get(j).high) {
						high = priceArrayList.get(j).high;
					}
				}
				this.k11 = ((this.price - low) * 100) / (high - low);
			}

			void calculateK20(List<PriceArray> priceArrayList) {
				double high = 0.0;
				double low = 999999999.0;
				for (int j = 0; j < priceArrayList.size(); j++) {
					if (low > priceArrayList.get(j).low) {
						low = priceArrayList.get(j).low;
					}
					if (high < priceArrayList.get(j).high) {
						high = priceArrayList.get(j).high;
					}
				}
				this.k20 = ((this.price - low) * 100) / (high - low);
			}

			void calculateK29(List<PriceArray> priceArrayList) {
				double high = 0.0;
				double low = 999999999.0;
				for (int j = 0; j < priceArrayList.size(); j++) {
					if (low > priceArrayList.get(j).low) {
						low = priceArrayList.get(j).low;
					}
					if (high < priceArrayList.get(j).high) {
						high = priceArrayList.get(j).high;
					}
				}
				this.k29 = ((this.price - low) * 100) / (high - low);
			}

			public void finalizeAO() {
				this.aoVal = this.sma11 - this.sma29;
			}
		}
		ArrayList<PriceArray> priceList = new ArrayList<PriceArray>();
		String openSql = "select * from (select -(100000-x.id) as id,x.close,x.high,x.low FROM " + preQuoteTable
				+ "_signalparams x where x.instrumenttoken='" + instrumentToken
				+ "' order by x.timestampgrp desc limit 28) y order by y.id asc";
		Statement stmt = conn.createStatement();
		ResultSet rs = stmt.executeQuery(openSql);
		PriceArray priceArray;
		while (rs.next()) {
			priceArray = new PriceArray();
			priceArray.price = rs.getDouble("close");
			priceArray.high = rs.getDouble("high");
			priceArray.low = rs.getDouble("low");
			priceArray.id = rs.getInt("id");
			priceArray.goldBuzzSignal = new GoldBuzzSignal(1, "");
			priceList.add(priceArray);
		}
		stmt.close();

		openSql = "select x.id,x.close,x.sma29,y.sma11,((x.close-zz.min11)*100)/(zz.max11-zz.min11) as k11,"
				+ "((x.close-yy.min20)*100)/(yy.max20-yy.min20) as k20,((x.close-xx.min29)*100)/(xx.max29-xx.min29) as k29 "
				+ "from (SELECT a.id,a.close,a.instrumenttoken,a.timestampGrp,sum((b.high+b.low)/2) as sma29"
				+ "  FROM " + quoteTable + "_signalparams a," + quoteTable + "_signalparams b where a.instrumenttoken='"
				+ instrumentToken
				+ "' and b.instrumenttoken=a.instrumenttoken and TIMEDIFF(a.timestampGrp,b.timestampGrp) < '00:29:00' and "
				+ "TIMEDIFF(a.timestampGrp,b.timestampGrp) >= '00:00:00' group by a.timestampGrp order by a.timestampgrp,b.timestampgrp asc) x,"
				+ "(SELECT a.id,a.instrumenttoken,a.timestampGrp,sum((b.high+b.low)/2) as sma11 FROM " + quoteTable
				+ "_signalparams a," + quoteTable + "_signalparams b where a.instrumenttoken='" + instrumentToken
				+ "' and b.instrumenttoken=a.instrumenttoken and TIMEDIFF(a.timestampGrp,b.timestampGrp) < '00:11:00' and "
				+ "TIMEDIFF(a.timestampGrp,b.timestampGrp) >= '00:00:00' group by a.timestampGrp order by a.timestampgrp,b.timestampgrp asc) y"
				+ ",(SELECT a.id,a.close,a.instrumenttoken,a.timestampGrp,max(b.high) as max29,min(b.low) as min29 FROM "
				+ quoteTable + "_signalparams a," + quoteTable + "_signalparams b where a.instrumenttoken='"
				+ instrumentToken
				+ "' and b.instrumenttoken=a.instrumenttoken and TIMEDIFF(a.timestampGrp,b.timestampGrp) < '00:29:00' and "
				+ "TIMEDIFF(a.timestampGrp,b.timestampGrp) >= '00:00:00' group by a.timestampGrp order by a.timestampgrp,b.timestampgrp asc) "
				+ "xx,(SELECT a.id,a.instrumenttoken,a.timestampGrp,max(b.high) as max20,min(b.low) as min20 FROM "
				+ quoteTable + "_signalparams a," + quoteTable + "_signalparams b where a.instrumenttoken='"
				+ instrumentToken
				+ "' and b.instrumenttoken=a.instrumenttoken and TIMEDIFF(a.timestampGrp,b.timestampGrp) < '00:20:00' and "
				+ "TIMEDIFF(a.timestampGrp,b.timestampGrp) >= '00:00:00' group by a.timestampGrp order by a.timestampgrp,b.timestampgrp asc) "
				+ "yy,(SELECT a.id,a.instrumenttoken,a.timestampGrp,max(b.high) as max11,min(b.low) as min11 FROM "
				+ quoteTable + "_signalparams a," + quoteTable + "_signalparams b where a.instrumenttoken='"
				+ instrumentToken
				+ "' and b.instrumenttoken=a.instrumenttoken and TIMEDIFF(a.timestampGrp,b.timestampGrp) < '00:11:00' and TIMEDIFF"
				+ "(a.timestampGrp,b.timestampGrp) >= '00:00:00' group by a.timestampGrp order by a.timestampgrp,b.timestampgrp asc) "
				+ "zz  where x.id=y.id and x.id=xx.id and xx.id=yy.id and xx.id=zz.id and x.instrumenttoken=y.instrumenttoken and "
				+ "x.instrumenttoken=xx.instrumenttoken and xx.instrumenttoken=yy.instrumenttoken and xx.instrumenttoken="
				+ "zz.instrumenttoken and xx.timestampgrp=yy.timestampgrp and xx.timestampgrp=zz.timestampgrp and x.timestampgrp=y.timestampgrp "
				+ "and x.timestampgrp=xx.timestampgrp order by x.id asc ";
		stmt = conn.createStatement();
		rs = stmt.executeQuery(openSql);
		while (rs.next()) {
			priceArray = new PriceArray();
			priceArray.price = rs.getDouble("close");
			priceArray.sma11 = rs.getDouble("sma11");
			priceArray.sma29 = rs.getDouble("sma29");
			priceArray.k11 = rs.getDouble("k11");
			priceArray.k20 = rs.getDouble("k20");
			priceArray.k29 = rs.getDouble("k29");
			priceArray.id = rs.getInt("id");
			priceArray.goldBuzzSignal = new GoldBuzzSignal(1, "");
			priceList.add(priceArray);
		}
		stmt.close();
		PriceArray price0, price1, price2, price3, price4;

		for (int i = 28; i < 56; i++) {
			try {
				if (i < 38) {
					priceList.get(i).calculateSma11(priceList.subList(i - 10, 28));
					priceList.get(i).calculateK11(priceList.subList(i - 10, 28));
				}
				if (i < 47)
					priceList.get(i).calculateK20(priceList.subList(i - 19, 28));

				priceList.get(i).calculateSma29(priceList.subList(i - 28, 28));
				priceList.get(i).calculateK29(priceList.subList(i - 28, 28));
			} catch (Exception ex) {
				ex.printStackTrace();
			}
		}
		for (int i = 0; i < priceList.size(); i++) {
			priceList.get(i).finalizeSma11();
			priceList.get(i).finalizeSma29();
			priceList.get(i).finalizeAO();
		}
		for (int i = 28; i < priceList.size(); i++) {
			price0 = priceList.get(i);
			price1 = priceList.get(i - 1);
			price2 = priceList.get(i - 2);
			price3 = priceList.get(i - 3);
			price4 = priceList.get(i - 4);

			/*
			 * if (price0.k11 > 90.0 && price0.k20 > 90.0 && price0.k29 > 90.0 &&
			 * price0.aoVal > price1.aoVal && price1.aoVal > price2.aoVal && price2.aoVal >
			 * price3.aoVal && price3.aoVal > price4.aoVal) {
			 * 
			 * priceList.get(i).goldBuzzSignal.setSignal(2);
			 * priceList.get(i).goldBuzzSignal.setSignalLevel("directional");
			 * 
			 * } else if (price0.k11 < 10.0 && price0.k20 < 10.0 && price0.k29 < 10.0 &&
			 * price0.aoVal < price1.aoVal && price1.aoVal < price2.aoVal && price2.aoVal <
			 * price3.aoVal && price3.aoVal < price4.aoVal) {
			 * 
			 * priceList.get(i).goldBuzzSignal.setSignal(0);
			 * priceList.get(i).goldBuzzSignal.setSignalLevel("directional");
			 * 
			 * } else
			 */ if (price0.k11 >= 80.0 && price0.k20 >= 80.0 && price0.k29 >= 80.0
					//&& ((price0.k11 < 90.0 && price0.k20 < 90.0) || (price0.k29 < 90.0 && price0.k20 < 90.0)
					//		|| (price0.k11 < 90.0 && price0.k29 < 90.0))
					&& ((price1.k11 >= price0.k11 && price2.k11 <= price1.k11)
							|| (price1.k20 >= price0.k20 && price2.k20 <= price1.k20)
							|| (price1.k29 >= price0.k29 && price2.k29 <= price1.k29))
					&& price0.aoVal > price1.aoVal && price1.aoVal > price2.aoVal && price2.aoVal > price3.aoVal
					&& price3.aoVal > price4.aoVal) {

				priceList.get(i).goldBuzzSignal.setSignal(0);
				priceList.get(i).goldBuzzSignal.setSignalLevel("above");

			} else if (price0.k11 <= 20.0 && price0.k20 <= 20.0 && price0.k29 <= 20.0
					//&& ((price0.k29 > 10.0 && price0.k20 > 10.0) || (price0.k11 > 10.0 && price0.k20 > 10.0)
					//		|| (price0.k11 > 10.0 && price0.k29 > 10.0))
					&& ((price1.k11 <= price0.k11 && price2.k11 >= price1.k11)
							|| (price1.k20 <= price0.k20 && price2.k20 >= price1.k20)
							|| (price1.k29 <= price0.k29 && price2.k29 >= price1.k29))
					&& price0.aoVal < price1.aoVal && price1.aoVal < price2.aoVal && price2.aoVal < price3.aoVal
					&& price3.aoVal < price4.aoVal) {

				priceList.get(i).goldBuzzSignal.setSignal(2);
				priceList.get(i).goldBuzzSignal.setSignalLevel("below");
			}

			boolean breakerLoop = true;
			PriceArray priceT;
			for (int j = i; j > 0 && breakerLoop; j--) {
				priceT = priceList.get(j - 1);
				if ((priceT.goldBuzzSignal.getSignal() == 2
						&& (price0.price / priceT.price <= 0.99 || price0.price / priceT.price >= 1.01))
						|| (priceT.goldBuzzSignal.getSignal() == 0
								&& (price0.price / priceT.price >= 1.01 || price0.price / priceT.price <= 0.99))) {

					priceList.get(i).goldBuzzSignal.setSignal(-1);
					priceList.get(i).goldBuzzSignal.setSignalLevel("off");
				}

				if (priceT.goldBuzzSignal.getSignal() == 2 || priceT.goldBuzzSignal.getSignal() == 0
						|| priceT.goldBuzzSignal.getSignal() == -1)
					breakerLoop = false;
			}

			/*
			 * int breakerLoopk = 0; int trendPositiveSqrOff = 0; PriceArray pricepre =
			 * null, pricepre1 = null, pricepre2 = null, pricepre3 = null; for (int k = i; k
			 * > 0 && breakerLoopk < 3; k--) { pricepre = priceList.get(k - 1); if
			 * (pricepre.goldBuzzSignal.getSignal() == 2 ||
			 * pricepre.goldBuzzSignal.getSignal() == 0 ||
			 * pricepre.goldBuzzSignal.getSignal() == -1) { if (breakerLoopk == 0) pricepre1
			 * = pricepre; else if (breakerLoopk == 1) pricepre2 = pricepre; else if
			 * (breakerLoopk == 2) pricepre3 = pricepre; breakerLoopk++; } }
			 * 
			 * if (priceList.get(i).goldBuzzSignal.getSignal() == 2 && null != pricepre1 &&
			 * null != pricepre2 && null != pricepre3 &&
			 * !trendPositiveList.containsKey(instrumentToken) &&
			 * !forbiddenGoldBuzzList.contains(instrumentToken) && null !=
			 * pricepre1.goldBuzzSignal && null != pricepre2.goldBuzzSignal && null !=
			 * pricepre3.goldBuzzSignal && priceList.get(i).goldBuzzSignal.getSignal() ==
			 * pricepre1.goldBuzzSignal.getSignal() && pricepre1.goldBuzzSignal.getSignal()
			 * == pricepre2.goldBuzzSignal.getSignal() &&
			 * pricepre3.goldBuzzSignal.getSignal() == pricepre2.goldBuzzSignal.getSignal()
			 * && priceList.get(i).price < pricepre1.price && pricepre1.price <
			 * pricepre2.price && pricepre2.price < pricepre3.price && priceList.get(i).k11
			 * < 10.0 && priceList.get(i).k20 < 10.0 && priceList.get(i).k29 < 10.0) {
			 * trendPositiveList.put(instrumentToken, 0); trendPositiveSqrOff = 1;
			 * priceList.get(i).goldBuzzSignal.setSignal(0); } else if
			 * (priceList.get(i).goldBuzzSignal.getSignal() == 0 && priceList.get(i).k11 >
			 * 90.0 && priceList.get(i).k20 > 90.0 && priceList.get(i).k29 > 90.0 && null !=
			 * pricepre1 && null != pricepre2 && null != pricepre3 &&
			 * !trendPositiveList.containsKey(instrumentToken) &&
			 * !forbiddenGoldBuzzList.contains(instrumentToken) && null !=
			 * pricepre1.goldBuzzSignal && null != pricepre2.goldBuzzSignal && null !=
			 * pricepre3.goldBuzzSignal && priceList.get(i).goldBuzzSignal.getSignal() ==
			 * pricepre1.goldBuzzSignal.getSignal() && pricepre1.goldBuzzSignal.getSignal()
			 * == pricepre2.goldBuzzSignal.getSignal() &&
			 * pricepre1.goldBuzzSignal.getSignal() == pricepre3.goldBuzzSignal.getSignal()
			 * && priceList.get(i).price > pricepre1.price && pricepre1.price >
			 * pricepre2.price && pricepre2.price > pricepre3.price) {
			 * trendPositiveList.put(instrumentToken, 2); trendPositiveSqrOff = 1;
			 * priceList.get(i).goldBuzzSignal.setSignal(2); } if (trendPositiveSqrOff == 1)
			 * { String sql = "INSERT INTO " + quoteTable + "_Signal " +
			 * "(time,instrumentToken,quantity,processSignal,status,TradePrice,signalParamKey,SignalLevel) "
			 * + "values(?,?,?,?,?,?,?,?)"; PreparedStatement prepStmt =
			 * conn.prepareStatement(sql);
			 * 
			 * prepStmt.setTimestamp(1, new
			 * Timestamp(Calendar.getInstance().getTime().getTime())); prepStmt.setString(2,
			 * instrumentToken); String temp1 = lotSizeOnInstrumentToken(instrumentToken,
			 * "SQUAREOFF", false); if (!"".equalsIgnoreCase(temp1)) { int q =
			 * Integer.parseInt(temp1); if (q > 0) { prepStmt.setString(4, "SELLOFF");
			 * prepStmt.setString(3, q + ""); } else if (q == 0) { temp1 = ""; } else {
			 * prepStmt.setString(4, "BUYOFF"); prepStmt.setString(3, (q +
			 * "").replaceAll("-", "")); } } prepStmt.setString(5, "active");
			 * prepStmt.setDouble(6, priceList.get(i).price); prepStmt.setDouble(7,
			 * priceList.get(i).id); prepStmt.setString(8, "directional"); if
			 * (!"".equalsIgnoreCase(temp1)) { prepStmt.executeUpdate(); } prepStmt.close();
			 * }
			 * 
			 * else if (trendPositiveList.containsKey(instrumentToken) &&
			 * !forbiddenGoldBuzzList.contains(instrumentToken)) { if
			 * (priceList.get(i).goldBuzzSignal.getSignal() == 2 &&
			 * trendPositiveList.get(instrumentToken).intValue() == 0) {
			 * forbiddenGoldBuzzList.add(instrumentToken);
			 * trendPositiveList.remove(instrumentToken);
			 * priceList.get(i).goldBuzzSignal.setSignal(-1); } else if
			 * (priceList.get(i).goldBuzzSignal.getSignal() == 0 &&
			 * trendPositiveList.get(instrumentToken).intValue() == 2) {
			 * forbiddenGoldBuzzList.add(instrumentToken);
			 * trendPositiveList.remove(instrumentToken);
			 * priceList.get(i).goldBuzzSignal.setSignal(-1); } else if
			 * (priceList.get(i).goldBuzzSignal.getSignal() == 2)
			 * priceList.get(i).goldBuzzSignal.setSignal(0); else if
			 * (priceList.get(i).goldBuzzSignal.getSignal() == 0)
			 * priceList.get(i).goldBuzzSignal.setSignal(2); }
			 */
			price0 = priceList.get(i);
			if (null != price0.goldBuzzSignal && (price0.goldBuzzSignal.getSignal() == 2
					|| price0.goldBuzzSignal.getSignal() == 0 || price0.goldBuzzSignal.getSignal() == -1)) {
				String sql = "INSERT INTO " + quoteTable + "_Signal "
						+ "(time,instrumentToken,quantity,processSignal,status,TradePrice,signalParamKey,SignalLevel) "
						+ "values(?,?,?,?,?,?,?,?)";
				PreparedStatement prepStmt = conn.prepareStatement(sql);

				prepStmt.setTimestamp(1, new Timestamp(Calendar.getInstance().getTime().getTime()));
				prepStmt.setString(2, instrumentToken);
				String temp1 = "";
				if (price0.goldBuzzSignal.getSignal() == 2) {
					prepStmt.setString(4, "BUY");
					temp1 = lotSizeOnInstrumentToken(instrumentToken, "BUY", false);
					prepStmt.setString(3, temp1);
				} else if (price0.goldBuzzSignal.getSignal() == 0) {
					prepStmt.setString(4, "SELL");
					temp1 = lotSizeOnInstrumentToken(instrumentToken, "SELL", false);
					prepStmt.setString(3, temp1);
				} else if (price0.goldBuzzSignal.getSignal() == -1) {
					temp1 = lotSizeOnInstrumentToken(instrumentToken, "SQUAREOFF", false);
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
				prepStmt.setDouble(6, price0.price);
				prepStmt.setDouble(7, price0.id);
				prepStmt.setString(8, price0.goldBuzzSignal.getSignalLevel());
				if (forbiddenGoldBuzzList.contains(instrumentToken) && price0.goldBuzzSignal.getSignal() != -1)
					temp1 = "";
				if (!"".equalsIgnoreCase(temp1)) {
					prepStmt.executeUpdate();
				} else {
					priceList.get(i).goldBuzzSignal.setSignal(1);
				}
				prepStmt.close();
			}
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
		String temp1 = lotSizeOnInstrumentToken(instrumentToken, "SQUAREOFF", false);
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

	private void executeBatchGoldBuzzStrategy(String instrumentToken) throws SQLException {
		class PriceArray {
			Integer id;
			Double price;
			GoldBuzzSignal goldBuzzSignal;
		}
		ArrayList<PriceArray> priceList = new ArrayList<PriceArray>();
		String openSql = "SELECT close,id FROM " + quoteTable + "_SignalParams where InstrumentToken ='"
				+ instrumentToken + "' ORDER BY id ASC ";
		Statement stmt = conn.createStatement();
		ResultSet rs = stmt.executeQuery(openSql);
		PriceArray priceArray;
		while (rs.next()) {
			priceArray = new PriceArray();
			priceArray.price = rs.getDouble("close");
			priceArray.id = rs.getInt("id");
			priceList.add(priceArray);
		}
		stmt.close();
		GoldBuzz goldBuzz = goldBuzzList.get(instrumentToken);
		int lotSize = 0;
		if (priceList.size() > 0) {

			if (priceList.get(0).price > goldBuzz.getCamaPP() && priceList.get(0).price <= goldBuzz.getCamaH1()) {
				priceList.get(0).goldBuzzSignal = new GoldBuzzSignal(0, "106");
			} else if (priceList.get(0).price > goldBuzz.getCamaH1()
					&& priceList.get(0).price <= goldBuzz.getCamaH2()) {
				priceList.get(0).goldBuzzSignal = new GoldBuzzSignal(0, "107");
			} else if (priceList.get(0).price > goldBuzz.getCamaH2()
					&& priceList.get(0).price <= goldBuzz.getCamaH3()) {
				priceList.get(0).goldBuzzSignal = new GoldBuzzSignal(0, "108");
			} else if (priceList.get(0).price > goldBuzz.getCamaH3()
					&& priceList.get(0).price <= goldBuzz.getCamaH4()) {
				priceList.get(0).goldBuzzSignal = new GoldBuzzSignal(0, "109");
			} else if (priceList.get(0).price > goldBuzz.getCamaH4()
					&& priceList.get(0).price <= goldBuzz.getCamaH5()) {
				priceList.get(0).goldBuzzSignal = new GoldBuzzSignal(0, "110");
			} else if (priceList.get(0).price > goldBuzz.getCamaH5()) {
				priceList.get(0).goldBuzzSignal = new GoldBuzzSignal(10, "111");

			} else if (priceList.get(0).price < goldBuzz.getCamaPP()
					&& priceList.get(0).price >= goldBuzz.getCamaL1()) {
				priceList.get(0).goldBuzzSignal = new GoldBuzzSignal(1, "105");
			} else if (priceList.get(0).price < goldBuzz.getCamaL1()
					&& priceList.get(0).price >= goldBuzz.getCamaL2()) {
				priceList.get(0).goldBuzzSignal = new GoldBuzzSignal(1, "104");
			} else if (priceList.get(0).price < goldBuzz.getCamaL2()
					&& priceList.get(0).price >= goldBuzz.getCamaL3()) {
				priceList.get(0).goldBuzzSignal = new GoldBuzzSignal(1, "103");
			} else if (priceList.get(0).price < goldBuzz.getCamaL3()
					&& priceList.get(0).price >= goldBuzz.getCamaL4()) {
				priceList.get(0).goldBuzzSignal = new GoldBuzzSignal(1, "102");
			} else if (priceList.get(0).price < goldBuzz.getCamaL4()
					&& priceList.get(0).price >= goldBuzz.getCamaL5()) {
				priceList.get(0).goldBuzzSignal = new GoldBuzzSignal(1, "101");
			} else if (priceList.get(0).price < goldBuzz.getCamaL5()) {
				priceList.get(0).goldBuzzSignal = new GoldBuzzSignal(-10, "100");
			}

			if (null != priceList.get(0).goldBuzzSignal) {
				if (priceList.get(0).goldBuzzSignal.getSignal() == 2)
					lotSize = 1;
				else if (priceList.get(0).goldBuzzSignal.getSignal() == 0)
					lotSize = -1;
				else if (priceList.get(0).goldBuzzSignal.getSignal() == 10
						&& !forbiddenGoldBuzzList.contains(instrumentToken))
					lotSize = 1;
				else if (priceList.get(0).goldBuzzSignal.getSignal() == -10
						&& !forbiddenGoldBuzzList.contains(instrumentToken))
					lotSize = -1;
			}

			if (null != priceList.get(0).goldBuzzSignal && priceList.get(0).goldBuzzSignal.getSignal() != 1
					&& priceList.get(0).price != StreamingConfig.MAX_VALUE && priceList.get(0).price != 0.0) {

				if (priceList.get(0).goldBuzzSignal.getSignal() != -10
						&& priceList.get(0).goldBuzzSignal.getSignal() != 10)
					saveGoldBuzzSignal(instrumentToken, priceList.get(0).goldBuzzSignal, priceList.get(0).price,
							priceList.get(0).id);
				else
					saveGoldBuzzSignalOnDirectinalMove(instrumentToken, priceList.get(0).goldBuzzSignal,
							priceList.get(0).price, priceList.get(0).id);
			}
		}

		for (int i = 1; i < priceList.size(); i++) {

			PriceArray firstClose = priceList.get(i);
			PriceArray secondClose = priceList.get(i - 1);

			if (firstClose.price > secondClose.price) {
				if (firstClose.price > goldBuzz.getCamaPP() && firstClose.price <= goldBuzz.getCamaH1()) {
					priceList.get(i).goldBuzzSignal = new GoldBuzzSignal(0, "106");
				} else if (firstClose.price > goldBuzz.getCamaH1() && firstClose.price <= goldBuzz.getCamaH2()) {
					priceList.get(i).goldBuzzSignal = new GoldBuzzSignal(0, "107");
				} else if (firstClose.price > goldBuzz.getCamaH2() && firstClose.price <= goldBuzz.getCamaH3()) {
					priceList.get(i).goldBuzzSignal = new GoldBuzzSignal(0, "108");
				} else if (firstClose.price > goldBuzz.getCamaH3() && firstClose.price <= goldBuzz.getCamaH4()) {
					priceList.get(i).goldBuzzSignal = new GoldBuzzSignal(0, "109");
				} else if (firstClose.price > goldBuzz.getCamaH4() && firstClose.price <= goldBuzz.getCamaH5()) {
					priceList.get(i).goldBuzzSignal = new GoldBuzzSignal(0, "110");
				} else if (firstClose.price > goldBuzz.getCamaH5()) {
					priceList.get(i).goldBuzzSignal = new GoldBuzzSignal(10, "111");

				} else if (firstClose.price < goldBuzz.getCamaPP() && firstClose.price >= goldBuzz.getCamaL1()) {
					priceList.get(i).goldBuzzSignal = new GoldBuzzSignal(1, "105");
				} else if (firstClose.price < goldBuzz.getCamaL1() && firstClose.price >= goldBuzz.getCamaL2()) {
					priceList.get(i).goldBuzzSignal = new GoldBuzzSignal(1, "104");
				} else if (firstClose.price < goldBuzz.getCamaL2() && firstClose.price >= goldBuzz.getCamaL3()) {
					priceList.get(i).goldBuzzSignal = new GoldBuzzSignal(1, "103");
				} else if (firstClose.price < goldBuzz.getCamaL3() && firstClose.price >= goldBuzz.getCamaL4()) {
					priceList.get(i).goldBuzzSignal = new GoldBuzzSignal(1, "102");
				} else if (firstClose.price < goldBuzz.getCamaL4() && firstClose.price >= goldBuzz.getCamaL5()) {
					priceList.get(i).goldBuzzSignal = new GoldBuzzSignal(1, "101");
				} else if (firstClose.price < goldBuzz.getCamaL5()) {
					priceList.get(i).goldBuzzSignal = new GoldBuzzSignal(-10, "100");

				} else
					priceList.get(i).goldBuzzSignal = new GoldBuzzSignal(1, "0");
			} else if (secondClose.price > firstClose.price) {

				if (firstClose.price > goldBuzz.getCamaPP() && firstClose.price <= goldBuzz.getCamaH1()) {
					priceList.get(i).goldBuzzSignal = new GoldBuzzSignal(1, "106");
				} else if (firstClose.price > goldBuzz.getCamaH1() && firstClose.price <= goldBuzz.getCamaH2()) {
					priceList.get(i).goldBuzzSignal = new GoldBuzzSignal(1, "107");
				} else if (firstClose.price > goldBuzz.getCamaH2() && firstClose.price <= goldBuzz.getCamaH3()) {
					priceList.get(i).goldBuzzSignal = new GoldBuzzSignal(1, "108");
				} else if (firstClose.price > goldBuzz.getCamaH3() && firstClose.price <= goldBuzz.getCamaH4()) {
					priceList.get(i).goldBuzzSignal = new GoldBuzzSignal(1, "109");
				} else if (firstClose.price > goldBuzz.getCamaH4() && firstClose.price <= goldBuzz.getCamaH5()) {
					priceList.get(i).goldBuzzSignal = new GoldBuzzSignal(1, "110");
				} else if (firstClose.price > goldBuzz.getCamaH5()) {
					priceList.get(i).goldBuzzSignal = new GoldBuzzSignal(10, "111");

				} else if (firstClose.price < goldBuzz.getCamaPP() && firstClose.price >= goldBuzz.getCamaL1()) {
					priceList.get(i).goldBuzzSignal = new GoldBuzzSignal(2, "105");
				} else if (firstClose.price < goldBuzz.getCamaL1() && firstClose.price >= goldBuzz.getCamaL2()) {
					priceList.get(i).goldBuzzSignal = new GoldBuzzSignal(2, "104");
				} else if (firstClose.price < goldBuzz.getCamaL2() && firstClose.price >= goldBuzz.getCamaL3()) {
					priceList.get(i).goldBuzzSignal = new GoldBuzzSignal(2, "103");
				} else if (firstClose.price < goldBuzz.getCamaL3() && firstClose.price >= goldBuzz.getCamaL4()) {
					priceList.get(i).goldBuzzSignal = new GoldBuzzSignal(2, "102");
				} else if (firstClose.price < goldBuzz.getCamaL4() && firstClose.price >= goldBuzz.getCamaL5()) {
					priceList.get(i).goldBuzzSignal = new GoldBuzzSignal(2, "101");
				} else if (firstClose.price < goldBuzz.getCamaL5()) {
					priceList.get(i).goldBuzzSignal = new GoldBuzzSignal(-10, "100");

				} else
					priceList.get(i).goldBuzzSignal = new GoldBuzzSignal(1, "0");
			} else
				priceList.get(i).goldBuzzSignal = new GoldBuzzSignal(1, "0");

			String preSignal = "";
			String preSignalLevel = "";
			double tradePrice = 0.0;
			String preSignal1 = "";
			boolean runner = true;
			for (int j = i - 1; j >= 0 && runner; j--) {
				PriceArray prevPrice = priceList.get(j);
				if (null != prevPrice.goldBuzzSignal && (prevPrice.goldBuzzSignal.getSignal() != 1
						|| prevPrice.goldBuzzSignal.getSignalLevel() != "0")) {
					preSignal = prevPrice.goldBuzzSignal.getSignal() + "";
					preSignalLevel = prevPrice.goldBuzzSignal.getSignalLevel();
					tradePrice = prevPrice.price;
					runner = false;
					boolean runner1 = true;
					for (int k = j - 1; k >= 0 && runner1; k--) {
						PriceArray prevPrice1 = priceList.get(k);
						if (null != prevPrice1.goldBuzzSignal && (prevPrice1.goldBuzzSignal.getSignal() != 1
								|| prevPrice1.goldBuzzSignal.getSignalLevel() != "0")) {
							preSignal1 = prevPrice1.goldBuzzSignal.getSignal() + "";
							runner1 = false;
						}
					}
				}
			}
			int tempsignal = Integer.parseInt(priceList.get(i).goldBuzzSignal.getSignalLevel());

			if (("".equalsIgnoreCase(preSignalLevel) && "".equalsIgnoreCase(preSignal)) || tradePrice == 0.0) {

				if (forbiddenGoldBuzzList.contains(instrumentToken) && tempsignal != 100 && tempsignal != 111
						&& tempsignal != 0)
					forbiddenGoldBuzzList.remove(instrumentToken);

			} else if (priceList.get(i).goldBuzzSignal.getSignal() != -10
					&& priceList.get(i).goldBuzzSignal.getSignal() != 10) {

				if (forbiddenGoldBuzzList.contains(instrumentToken) && tempsignal != 100 && tempsignal != 111
						&& tempsignal != 0)
					forbiddenGoldBuzzList.remove(instrumentToken);

				if (tempsignal == Integer.parseInt(preSignalLevel) && ((preSignal.equalsIgnoreCase("2")
						&& priceList.get(i).goldBuzzSignal.getSignal() == 2)
						|| (preSignal.equalsIgnoreCase("0") && priceList.get(i).goldBuzzSignal.getSignal() == 0)))
					priceList.get(i).goldBuzzSignal.setSignal(1);

				if (preSignal.equalsIgnoreCase("2") && tempsignal == 1 + Integer.parseInt(preSignalLevel))
					priceList.get(i).goldBuzzSignal.setSignal(2);
				else if (preSignal.equalsIgnoreCase("0") && tempsignal + 1 == Integer.parseInt(preSignalLevel))
					priceList.get(i).goldBuzzSignal.setSignal(0);

				if ((preSignal.equalsIgnoreCase("2")
						|| (preSignal.equalsIgnoreCase("-1") && preSignal1.equalsIgnoreCase("2")))
						&& ((tempsignal != 0 && Math.abs(Integer.parseInt(preSignalLevel) - tempsignal) > 1)
								|| firstClose.price >= tradePrice * 1.01 || firstClose.price <= tradePrice * 0.995))
					priceList.get(i).goldBuzzSignal.setSignal(0);
				else if ((preSignal.equalsIgnoreCase("0")
						|| (preSignal.equalsIgnoreCase("-1") && preSignal1.equalsIgnoreCase("0")))
						&& ((tempsignal != 0 && Math.abs(Integer.parseInt(preSignalLevel) - tempsignal) > 1)
								|| firstClose.price <= tradePrice * 0.99 || firstClose.price >= tradePrice * 1.005))
					priceList.get(i).goldBuzzSignal.setSignal(2);
			} else if (forbiddenGoldBuzzList.contains(instrumentToken)) {

				if (preSignal.equalsIgnoreCase("2") && preSignalLevel.equalsIgnoreCase("111") && tradePrice != 0.0
						&& firstClose.price >= tradePrice * 1.02)
					priceList.get(i).goldBuzzSignal.setSignal(-1);
				else if (preSignal.equalsIgnoreCase("0") && preSignalLevel.equalsIgnoreCase("100") && tradePrice != 0.0
						&& firstClose.price <= tradePrice * 0.98)
					priceList.get(i).goldBuzzSignal.setSignal(-1);
				else
					priceList.get(i).goldBuzzSignal.setSignal(1);
			}

			if (priceList.get(i).goldBuzzSignal.getSignal() == -1)
				lotSize = 0;
			else if (priceList.get(i).goldBuzzSignal.getSignal() == 0 && lotSize == -2)
				priceList.get(i).goldBuzzSignal.setSignal(1);
			else if (priceList.get(i).goldBuzzSignal.getSignal() == 2 && lotSize == 2)
				priceList.get(i).goldBuzzSignal.setSignal(1);
			else if (priceList.get(i).goldBuzzSignal.getSignal() == 0 && lotSize == 0)
				lotSize = -1;
			else if (priceList.get(i).goldBuzzSignal.getSignal() == 2 && lotSize == 0)
				lotSize = 1;
			else if (priceList.get(i).goldBuzzSignal.getSignal() == 0 && lotSize == -1)
				lotSize = -2;
			else if (priceList.get(i).goldBuzzSignal.getSignal() == 2 && lotSize == 1)
				lotSize = 2;
			else if (priceList.get(i).goldBuzzSignal.getSignal() == 0
					|| priceList.get(i).goldBuzzSignal.getSignal() == 2)
				lotSize = 0;
			else if (priceList.get(i).goldBuzzSignal.getSignal() == 10
					&& !forbiddenGoldBuzzList.contains(instrumentToken))
				lotSize = 1;
			else if (priceList.get(i).goldBuzzSignal.getSignal() == -10
					&& !forbiddenGoldBuzzList.contains(instrumentToken))
				lotSize = -1;

			if (null != priceList.get(i).goldBuzzSignal && priceList.get(i).goldBuzzSignal.getSignal() != 1
					&& priceList.get(i).price != StreamingConfig.MAX_VALUE && priceList.get(i).price != 0.0) {

				if (priceList.get(i).goldBuzzSignal.getSignal() != -10
						&& priceList.get(i).goldBuzzSignal.getSignal() != 10)
					saveGoldBuzzSignal(instrumentToken, priceList.get(i).goldBuzzSignal, firstClose.price,
							priceList.get(i).id);
				else
					saveGoldBuzzSignalOnDirectinalMove(instrumentToken, priceList.get(i).goldBuzzSignal,
							firstClose.price, priceList.get(i).id);
			}

		}
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

				if (signalClose.getSignal() != -10 && signalClose.getSignal() != 10)
					saveGoldBuzzSignal(instrumentToken, signalClose, firstClose, firstRowId);
				else
					saveGoldBuzzSignalOnDirectinalMove(instrumentToken, signalClose, firstClose, firstRowId);

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

	private void saveGoldBuzzSignalOnDirectinalMove(String instrumentToken, GoldBuzzSignal signalClose,
			double firstClose, int firstRowId) throws SQLException {

		String sql = "INSERT INTO " + quoteTable + "_Signal "
				+ "(time,instrumentToken,quantity,processSignal,status,TradePrice,signalParamKey,SignalLevel) "
				+ "values(?,?,?,?,?,?,?,?)";
		PreparedStatement prepStmt = conn.prepareStatement(sql);

		prepStmt.setTimestamp(1, new Timestamp(Calendar.getInstance().getTime().getTime()));
		prepStmt.setString(2, instrumentToken);
		String temp1 = "";

		temp1 = lotSizeOnInstrumentToken(instrumentToken, "SQUAREOFF", false);
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
		prepStmt.setString(5, "active");
		prepStmt.setDouble(6, firstClose);
		prepStmt.setDouble(7, firstRowId);
		prepStmt.setString(8, signalClose.getSignalLevel());
		if (!"".equalsIgnoreCase(temp1) && !forbiddenGoldBuzzList.contains(instrumentToken)) {
			prepStmt.executeUpdate();
		}
		prepStmt.close();

		prepStmt = conn.prepareStatement(sql);

		prepStmt.setTimestamp(1, new Timestamp(Calendar.getInstance().getTime().getTime()));
		prepStmt.setString(2, instrumentToken);

		temp1 = lotSizeOnInstrumentToken(instrumentToken, "SQUAREOFF", true);

		if (!"".equalsIgnoreCase(temp1)) {
			if (signalClose.getSignalLevel().equalsIgnoreCase("100")) {
				prepStmt.setString(4, "SELL");
				prepStmt.setString(3, temp1);
			} else if (signalClose.getSignalLevel().equalsIgnoreCase("111")) {
				prepStmt.setString(4, "BUY");
				prepStmt.setString(3, temp1);
			}
		}
		prepStmt.setString(5, "active");
		prepStmt.setDouble(6, firstClose);
		prepStmt.setDouble(7, firstRowId);
		prepStmt.setString(8, signalClose.getSignalLevel());
		if (!"".equalsIgnoreCase(temp1) && !forbiddenGoldBuzzList.contains(instrumentToken)) {
			prepStmt.executeUpdate();
		}
		prepStmt.close();
		forbiddenGoldBuzzList.add(instrumentToken);
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
			temp1 = lotSizeOnInstrumentToken(instrumentToken, "BUY", false);
			prepStmt.setString(3, temp1);
		} else if (signalClose.getSignal() == 0) {
			prepStmt.setString(4, "SELL");
			temp1 = lotSizeOnInstrumentToken(instrumentToken, "SELL", false);
			prepStmt.setString(3, temp1);
		} else {
			temp1 = lotSizeOnInstrumentToken(instrumentToken, "SQUAREOFF", false);
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
				signalClose = new GoldBuzzSignal(0, "106");
			} else if (firstClose > goldBuzz.getCamaH1() && firstClose <= goldBuzz.getCamaH2()) {
				signalClose = new GoldBuzzSignal(0, "107");
			} else if (firstClose > goldBuzz.getCamaH2() && firstClose <= goldBuzz.getCamaH3()) {
				signalClose = new GoldBuzzSignal(0, "108");
			} else if (firstClose > goldBuzz.getCamaH3() && firstClose <= goldBuzz.getCamaH4()) {
				signalClose = new GoldBuzzSignal(0, "109");
			} else if (firstClose > goldBuzz.getCamaH4() && firstClose <= goldBuzz.getCamaH5()) {
				signalClose = new GoldBuzzSignal(0, "110");
			} else if (firstClose > goldBuzz.getCamaH5()) {
				signalClose = new GoldBuzzSignal(10, "111");

			} else if (firstClose < goldBuzz.getCamaPP() && firstClose >= goldBuzz.getCamaL1()) {
				signalClose = new GoldBuzzSignal(1, "105");
			} else if (firstClose < goldBuzz.getCamaL1() && firstClose >= goldBuzz.getCamaL2()) {
				signalClose = new GoldBuzzSignal(1, "104");
			} else if (firstClose < goldBuzz.getCamaL2() && firstClose >= goldBuzz.getCamaL3()) {
				signalClose = new GoldBuzzSignal(1, "103");
			} else if (firstClose < goldBuzz.getCamaL3() && firstClose >= goldBuzz.getCamaL4()) {
				signalClose = new GoldBuzzSignal(1, "102");
			} else if (firstClose < goldBuzz.getCamaL4() && firstClose >= goldBuzz.getCamaL5()) {
				signalClose = new GoldBuzzSignal(1, "101");
			} else if (firstClose < goldBuzz.getCamaL5()) {
				signalClose = new GoldBuzzSignal(-10, "100");

			} else
				signalClose = new GoldBuzzSignal(1, "0");
		} else if (secondClose > firstClose) {

			if (firstClose > goldBuzz.getCamaPP() && firstClose <= goldBuzz.getCamaH1()) {
				signalClose = new GoldBuzzSignal(1, "106");
			} else if (firstClose > goldBuzz.getCamaH1() && firstClose <= goldBuzz.getCamaH2()) {
				signalClose = new GoldBuzzSignal(1, "107");
			} else if (firstClose > goldBuzz.getCamaH2() && firstClose <= goldBuzz.getCamaH3()) {
				signalClose = new GoldBuzzSignal(1, "108");
			} else if (firstClose > goldBuzz.getCamaH3() && firstClose <= goldBuzz.getCamaH4()) {
				signalClose = new GoldBuzzSignal(1, "109");
			} else if (firstClose > goldBuzz.getCamaH4() && firstClose <= goldBuzz.getCamaH5()) {
				signalClose = new GoldBuzzSignal(1, "110");
			} else if (firstClose > goldBuzz.getCamaH5()) {
				signalClose = new GoldBuzzSignal(10, "111");

			} else if (firstClose < goldBuzz.getCamaPP() && firstClose >= goldBuzz.getCamaL1()) {
				signalClose = new GoldBuzzSignal(2, "105");
			} else if (firstClose < goldBuzz.getCamaL1() && firstClose >= goldBuzz.getCamaL2()) {
				signalClose = new GoldBuzzSignal(2, "104");
			} else if (firstClose < goldBuzz.getCamaL2() && firstClose >= goldBuzz.getCamaL3()) {
				signalClose = new GoldBuzzSignal(2, "103");
			} else if (firstClose < goldBuzz.getCamaL3() && firstClose >= goldBuzz.getCamaL4()) {
				signalClose = new GoldBuzzSignal(2, "102");
			} else if (firstClose < goldBuzz.getCamaL4() && firstClose >= goldBuzz.getCamaL5()) {
				signalClose = new GoldBuzzSignal(2, "101");
			} else if (firstClose < goldBuzz.getCamaL5()) {
				signalClose = new GoldBuzzSignal(-10, "100");

			} else
				signalClose = new GoldBuzzSignal(1, "0");
		} else
			signalClose = new GoldBuzzSignal(1, "0");

		Statement stmt = conn.createStatement();

		String openSql = "SELECT processSignal,SignalLevel,TradePrice FROM " + quoteTable
				+ "_Signal  where instrumentToken='" + instrumentToken + "' order by id desc LIMIT 1";
		ResultSet openRs = stmt.executeQuery(openSql);

		String preSignal = "";
		String preSignalLevel = "";
		double tradePrice = 0.0;

		while (openRs.next()) {
			preSignal = openRs.getString(1);
			preSignalLevel = openRs.getString(2);
			tradePrice = openRs.getDouble(3);
		}
		stmt.close();

		int tempsignal = Integer.parseInt(signalClose.getSignalLevel());

		if (("".equalsIgnoreCase(preSignalLevel) && "".equalsIgnoreCase(preSignal)) || tradePrice == 0.0) {

			if (forbiddenGoldBuzzList.contains(instrumentToken) && tempsignal != 100 && tempsignal != 111
					&& tempsignal != 0)
				forbiddenGoldBuzzList.remove(instrumentToken);

		} else if (signalClose.getSignal() != -10 && signalClose.getSignal() != 10) {

			if (forbiddenGoldBuzzList.contains(instrumentToken) && tempsignal != 100 && tempsignal != 111
					&& tempsignal != 0)
				forbiddenGoldBuzzList.remove(instrumentToken);

			if (tempsignal == Integer.parseInt(preSignalLevel)
					&& ((preSignal.equalsIgnoreCase("BUY") && signalClose.getSignal() == 2)
							|| (preSignal.equalsIgnoreCase("SELL") && signalClose.getSignal() == 0)))
				signalClose.setSignal(1);

			if (preSignal.equalsIgnoreCase("BUY") && tempsignal == 1 + Integer.parseInt(preSignalLevel))
				signalClose.setSignal(2);
			else if (preSignal.equalsIgnoreCase("SELL") && tempsignal + 1 == Integer.parseInt(preSignalLevel))
				signalClose.setSignal(0);

			if (preSignal.contains("BUY")
					&& ((tempsignal != 0 && Math.abs(Integer.parseInt(preSignalLevel) - tempsignal) > 1)
							|| firstClose >= tradePrice * 1.01 || firstClose <= tradePrice * 0.995))
				signalClose.setSignal(0);
			else if (preSignal.contains("SELL")
					&& ((tempsignal != 0 && Math.abs(Integer.parseInt(preSignalLevel) - tempsignal) > 1)
							|| firstClose <= tradePrice * 0.99 || firstClose >= tradePrice * 1.005))
				signalClose.setSignal(2);
		} else if (forbiddenGoldBuzzList.contains(instrumentToken)) {

			if (preSignal.equalsIgnoreCase("BUY") && preSignalLevel.equalsIgnoreCase("111") && tradePrice != 0.0
					&& firstClose >= tradePrice * 1.02)
				signalClose.setSignal(-1);
			else if (preSignal.equalsIgnoreCase("SELL") && preSignalLevel.equalsIgnoreCase("100") && tradePrice != 0.0
					&& firstClose <= tradePrice * 0.98)
				signalClose.setSignal(-1);
			else
				signalClose.setSignal(1);
		}
		return signalClose;
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
						prepStmt.setDouble(27,
								(instrument.getClose() + instrument.getHigh() + instrument.getLow()) / 3.0);
						prepStmt.setDouble(28,
								instrument.getClose() + instrument.getWaitedHighMinusLow() * StreamingConfig.CAMA_H1);
						prepStmt.setDouble(29,
								instrument.getClose() + instrument.getWaitedHighMinusLow() * StreamingConfig.CAMA_H2);
						prepStmt.setDouble(30,
								instrument.getClose() + instrument.getWaitedHighMinusLow() * StreamingConfig.CAMA_H3);
						prepStmt.setDouble(31,
								instrument.getClose() + instrument.getWaitedHighMinusLow() * StreamingConfig.CAMA_H4);
						prepStmt.setDouble(32,
								instrument.getClose() - instrument.getWaitedHighMinusLow() * StreamingConfig.CAMA_L1);
						prepStmt.setDouble(33,
								instrument.getClose() - instrument.getWaitedHighMinusLow() * StreamingConfig.CAMA_L2);
						prepStmt.setDouble(34,
								instrument.getClose() - instrument.getWaitedHighMinusLow() * StreamingConfig.CAMA_L3);
						prepStmt.setDouble(35,
								instrument.getClose() - instrument.getWaitedHighMinusLow() * StreamingConfig.CAMA_L4);

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

	@Override
	public void saveHistoryDataOfSelectedInstruments(List<HistoricalData> historyDataList, String instrumentToken) {

		try {
			HistoricalData historyData = null;

			for (int count = 0; count < historyDataList.size(); count++) {
				historyData = historyDataList.get(count);
				String sql = "INSERT INTO " + quoteTable + "_signalParams "
						+ "(Time,InstrumentToken,high,low,close,psar,timestampGrp) values(?,?,?,?,?,?,?)";
				PreparedStatement prepStmtInsertSignalParams = conn.prepareStatement(sql);
				prepStmtInsertSignalParams.setTimestamp(1,
						new Timestamp(dtTmFmt.parse(dtTmFmt.format(Calendar.getInstance().getTime())).getTime()));
				prepStmtInsertSignalParams.setTimestamp(7,
						new Timestamp(histDataFmt.parse(historyData.timeStamp.split("'+'")[0]).getTime()));
				prepStmtInsertSignalParams.setString(2, instrumentToken);
				prepStmtInsertSignalParams.setDouble(3, historyData.high);
				prepStmtInsertSignalParams.setDouble(4, historyData.low);
				prepStmtInsertSignalParams.setDouble(5, historyData.close);
				prepStmtInsertSignalParams.setDouble(6, historyData.open);
				prepStmtInsertSignalParams.executeUpdate();
				prepStmtInsertSignalParams.close();
			}
		} catch (SQLException | ParseException e) {
			LOGGER.info(
					"StreamingQuoteStorageImpl.calculateAndSaveStrategy(): ERROR: SQLException on fetching data from Table, cause: "
							+ e.getMessage() + ">>" + e.getCause());
			e.printStackTrace();
		}
	}
}