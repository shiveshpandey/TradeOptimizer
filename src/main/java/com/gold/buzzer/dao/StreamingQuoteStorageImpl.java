package com.gold.buzzer.dao;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.HashMap;
import java.util.TimeZone;
import java.util.logging.Logger;

import com.gold.buzzer.models.InstrumentOHLCData;
import com.gold.buzzer.utils.StreamingConfig;

public class StreamingQuoteStorageImpl implements StreamingQuoteStorage {
	private final static Logger LOGGER = Logger.getLogger(StreamingQuoteStorageImpl.class.getName());

	private static final String JDBC_DRIVER = "com.mysql.jdbc.Driver";
	private static final String DB_URL = StreamingConfig.QUOTE_STREAMING_DB_URL;

	private static final String USER = StreamingConfig.QUOTE_STREAMING_DB_USER;
	private static final String PASS = StreamingConfig.QUOTE_STREAMING_DB_PWD;
	SimpleDateFormat histDataFmt = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss");

	private Connection conn = null;

	private static String quoteTable = null;

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
	public void createDaysStreamingQuoteTable() throws SQLException {
		if (conn != null) {
			Statement stmt = conn.createStatement();
			String sql;
			try {
				sql = "CREATE TABLE " + quoteTable + "_instrumentDetails "
						+ "(ID int NOT NULL AUTO_INCREMENT,dt varchar(32), tradingsymbol varchar(32),open DECIMAL(20,4),"
						+ "close DECIMAL(20,4),high DECIMAL(20,4),low DECIMAL(20,4),deliveryToTradeRatio DECIMAL(20,4),"
						+ "lastDeliveryQt DECIMAL(20,4),lastTradedQt DECIMAL(20,4),openminusclose DECIMAL(20,4),"
						+ "highminuslow DECIMAL(20,4),insertdt timestamp, PRIMARY KEY (ID)) ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARSET=utf8;";
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

	@Override
	public void saveInstrumentVolumeData(HashMap<String, ArrayList<InstrumentOHLCData>> stocksSymbolArray) {

		if (conn != null && stocksSymbolArray != null && stocksSymbolArray.size() > 0) {
			try {
				String sql = "UPDATE " + quoteTable
						+ "_instrumentDetails SET lastTradedQt = ?, lastDeliveryQt = ?, deliveryToTradeRatio = ? "
						+ " where tradingSymbol = ? and dt=?";

				PreparedStatement prepStmt = conn.prepareStatement(sql);
				Object[] keyList = stocksSymbolArray.keySet().toArray();
				for (int index = 0; index < stocksSymbolArray.size(); index++) {
					try {
						ArrayList<InstrumentOHLCData> instrumentList = stocksSymbolArray.get(keyList[index].toString());
						for (int count = 0; count < instrumentList.size(); count++) {
							InstrumentOHLCData instrument = instrumentList.get(count);

							prepStmt.setDouble(1, instrument.getClose());
							prepStmt.setDouble(2, instrument.getHigh());
							prepStmt.setDouble(3, instrument.getLow());
							prepStmt.setString(4, instrument.getInstrumentName());
							prepStmt.setString(5, instrument.getDt());
							prepStmt.executeUpdate();
						}
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
	public void saveLast10DaysOHLCData(HashMap<String, ArrayList<InstrumentOHLCData>> stocksSymbolArray) {
		if (conn != null && stocksSymbolArray != null && stocksSymbolArray.size() > 0) {
			try {
				String sql = "INSERT INTO " + quoteTable + "_instrumentDetails "
						+ "(close,high,low,open,tradingsymbol,highminuslow,openminusclose,dt,insertdt) "
						+ "values(?,?,?,?,?,?,?,?,?)";

				PreparedStatement prepStmt = conn.prepareStatement(sql);
				Object[] keyList = stocksSymbolArray.keySet().toArray();
				for (int index = 0; index < stocksSymbolArray.size(); index++) {
					try {
						ArrayList<InstrumentOHLCData> instrumentList = stocksSymbolArray.get(keyList[index].toString());
						for (int count = 0; count < instrumentList.size(); count++) {
							InstrumentOHLCData instrument = instrumentList.get(count);
							prepStmt.setDouble(1, instrument.getClose());
							prepStmt.setDouble(2, instrument.getHigh());
							prepStmt.setDouble(3, instrument.getLow());
							prepStmt.setDouble(4, instrument.getOpen());
							prepStmt.setString(5, instrument.getInstrumentName());
							prepStmt.setDouble(6, instrument.getHigh() - instrument.getLow());
							prepStmt.setDouble(7, instrument.getOpen() - instrument.getClose());
							prepStmt.setString(8, instrument.getDt());
							prepStmt.setTimestamp(9, new Timestamp(Calendar.getInstance().getTime().getTime()));
							prepStmt.executeUpdate();
						}
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