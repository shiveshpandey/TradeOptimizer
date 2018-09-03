package com.gold.buzzer.dao;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
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
						+ "(ID int NOT NULL AUTO_INCREMENT,dt varchar(32), tradingsymbol varchar(32), series varchar(32), open DECIMAL(20,4),"
						+ "close DECIMAL(20,4),high DECIMAL(20,4),low DECIMAL(20,4),deliveryToTradeRatio DECIMAL(20,4),"
						+ "lastDeliveryQt DECIMAL(20,4),lastTradedQt DECIMAL(20,4),openminusclose DECIMAL(20,4),"
						+ "highminuslow DECIMAL(20,4),insertdt timestamp, PRIMARY KEY (ID)) ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARSET=utf8;";
				stmt.executeUpdate(sql);

			} catch (SQLException e) {
				LOGGER.info(
						"StreamingQuoteStorageImpl.createDaysStreamingQuoteTable(): ERROR: SQLException on creating Table, cause: "
								+ e.getMessage() + ">>" + e.getCause());
			}
			try {
				sql = "CREATE TABLE " + quoteTable + "_minuteData "
						+ "(ID int NOT NULL AUTO_INCREMENT,dt varchar(32),tm varchar(32), tradingsymbol varchar(32), series varchar(32), open DECIMAL(20,4),"
						+ "close DECIMAL(20,4),high DECIMAL(20,4),low DECIMAL(20,4),deliveryToTradeRatio DECIMAL(20,4),"
						+ "lastDeliveryQt DECIMAL(20,4),lastTradedQt DECIMAL(20,4),openminusclose DECIMAL(20,4),"
						+ "highminuslow DECIMAL(20,4),insertdt timestamp, PRIMARY KEY (ID)) ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARSET=utf8;";
				stmt.executeUpdate(sql);

			} catch (SQLException e) {
				LOGGER.info(
						"StreamingQuoteStorageImpl.createDaysStreamingQuoteTable(): ERROR: SQLException on creating Table, cause: "
								+ e.getMessage() + ">>" + e.getCause());
			}
			try {
				sql = "CREATE TABLE " + quoteTable + "_5minuteData "
						+ "(ID int NOT NULL AUTO_INCREMENT,dt varchar(32),tm varchar(32), tradingsymbol varchar(32), series varchar(32), open DECIMAL(20,4),"
						+ "close DECIMAL(20,4),high DECIMAL(20,4),low DECIMAL(20,4),deliveryToTradeRatio DECIMAL(20,4),"
						+ "lastDeliveryQt DECIMAL(20,4),lastTradedQt DECIMAL(20,4),openminusclose DECIMAL(20,4),"
						+ "highminuslow DECIMAL(20,4),insertdt timestamp, PRIMARY KEY (ID)) ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARSET=utf8;";
				stmt.executeUpdate(sql);

			} catch (SQLException e) {
				LOGGER.info(
						"StreamingQuoteStorageImpl.createDaysStreamingQuoteTable(): ERROR: SQLException on creating Table, cause: "
								+ e.getMessage() + ">>" + e.getCause());
			}
			try {
				sql = "CREATE TABLE " + quoteTable + "_15minuteData "
						+ "(ID int NOT NULL AUTO_INCREMENT,dt varchar(32),tm varchar(32), tradingsymbol varchar(32), series varchar(32), open DECIMAL(20,4),"
						+ "close DECIMAL(20,4),high DECIMAL(20,4),low DECIMAL(20,4),deliveryToTradeRatio DECIMAL(20,4),"
						+ "lastDeliveryQt DECIMAL(20,4),lastTradedQt DECIMAL(20,4),openminusclose DECIMAL(20,4),"
						+ "highminuslow DECIMAL(20,4),insertdt timestamp, PRIMARY KEY (ID)) ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARSET=utf8;";
				stmt.executeUpdate(sql);

			} catch (SQLException e) {
				LOGGER.info(
						"StreamingQuoteStorageImpl.createDaysStreamingQuoteTable(): ERROR: SQLException on creating Table, cause: "
								+ e.getMessage() + ">>" + e.getCause());
			}
			try {
				sql = "CREATE TABLE " + quoteTable + "_30minuteData "
						+ "(ID int NOT NULL AUTO_INCREMENT,dt varchar(32),tm varchar(32), tradingsymbol varchar(32), series varchar(32), open DECIMAL(20,4),"
						+ "close DECIMAL(20,4),high DECIMAL(20,4),low DECIMAL(20,4),deliveryToTradeRatio DECIMAL(20,4),"
						+ "lastDeliveryQt DECIMAL(20,4),lastTradedQt DECIMAL(20,4),openminusclose DECIMAL(20,4),"
						+ "highminuslow DECIMAL(20,4),insertdt timestamp, PRIMARY KEY (ID)) ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARSET=utf8;";
				stmt.executeUpdate(sql);

			} catch (SQLException e) {
				LOGGER.info(
						"StreamingQuoteStorageImpl.createDaysStreamingQuoteTable(): ERROR: SQLException on creating Table, cause: "
								+ e.getMessage() + ">>" + e.getCause());
			}
			try {
				sql = "CREATE TABLE " + quoteTable + "_60minuteData "
						+ "(ID int NOT NULL AUTO_INCREMENT,dt varchar(32),tm varchar(32), tradingsymbol varchar(32), series varchar(32), open DECIMAL(20,4),"
						+ "close DECIMAL(20,4),high DECIMAL(20,4),low DECIMAL(20,4),deliveryToTradeRatio DECIMAL(20,4),"
						+ "lastDeliveryQt DECIMAL(20,4),lastTradedQt DECIMAL(20,4),openminusclose DECIMAL(20,4),"
						+ "highminuslow DECIMAL(20,4),insertdt timestamp, PRIMARY KEY (ID)) ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARSET=utf8;";
				stmt.executeUpdate(sql);

			} catch (SQLException e) {
				LOGGER.info(
						"StreamingQuoteStorageImpl.createDaysStreamingQuoteTable(): ERROR: SQLException on creating Table, cause: "
								+ e.getMessage() + ">>" + e.getCause());
			}
			try {
				sql = "CREATE TABLE " + quoteTable + "_signal "
						+ "(ID int NOT NULL AUTO_INCREMENT,dt varchar(32),signl varchar(32), tradingsymbol varchar(32), series varchar(32),"
						+ " price DECIMAL(20,4),insertdt timestamp, PRIMARY KEY (ID)) ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARSET=utf8;";
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
						+ " where tradingSymbol = ? and dt = ? and series = ?";

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
							prepStmt.setString(6, instrument.getSeries());
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
						+ "(close,high,low,open,tradingsymbol,highminuslow,openminusclose,dt,insertdt,series) "
						+ "values(?,?,?,?,?,?,?,?,?,?)";

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
							prepStmt.setString(10, instrument.getSeries());
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

	@Override
	public void saveDaysOHLCVMinuteData(ArrayList<InstrumentOHLCData> instrumentList) {
		if (conn != null && instrumentList != null && instrumentList.size() > 0) {
			try {
				String sql = "INSERT INTO " + quoteTable + "_minuteData "
						+ "(close,high,low,open,tradingsymbol,dt,series,insertdt,lastTradedQt) "
						+ "values(?,?,?,?,?,?,?,?,?)";

				PreparedStatement prepStmt = conn.prepareStatement(sql);
				for (int count = 0; count < instrumentList.size(); count++) {
					try {
						InstrumentOHLCData instrument = instrumentList.get(count);
						prepStmt.setDouble(1, instrument.getClose());
						prepStmt.setDouble(2, instrument.getHigh());
						prepStmt.setDouble(3, instrument.getLow());
						prepStmt.setDouble(4, instrument.getOpen());
						prepStmt.setString(5, instrument.getInstrumentName());
						prepStmt.setString(6, instrument.getDt());
						prepStmt.setString(7, instrument.getSeries());
						prepStmt.setTimestamp(8, new Timestamp(Calendar.getInstance().getTime().getTime()));
						prepStmt.setDouble(9, instrument.getHighMinusLow());

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
	public void saveAndGenerateSignal(String symbol) {
		ArrayList<String> symbolList = fetchDistinctSymbolList();

		for (int k = 0; k < symbolList.size(); k++) {
			ArrayList<InstrumentOHLCData> instrumentOHLCDataList = calculateSignal(
					calculateEMASMA(fetchSymbolTimeSeriesData(symbolList.get(k))));
			if (conn != null && instrumentOHLCDataList != null && instrumentOHLCDataList.size() > 0) {
				try {
					String sql = "INSERT INTO " + quoteTable + "_signal "
							+ "(price,signl,tradingsymbol,dt,series,insertdt) " + "values(?,?,?,?,?,?)";

					PreparedStatement prepStmt = conn.prepareStatement(sql);
					for (int count = 0; count < instrumentOHLCDataList.size(); count++) {
						try {
							InstrumentOHLCData instrument = instrumentOHLCDataList.get(count);
							prepStmt.setDouble(1, instrument.getClose());
							prepStmt.setString(3, instrument.getInstrumentName());
							prepStmt.setString(4, instrument.getDt());
							prepStmt.setString(5, instrument.getSeries());
							prepStmt.setTimestamp(6, new Timestamp(Calendar.getInstance().getTime().getTime()));
							if (instrument.getHighMinusLow() == 200.0)
								prepStmt.setString(2, "BUYSLOW");
							else if (instrument.getHighMinusLow() == -200.0)
								prepStmt.setString(2, "SELLSLOW");
							else if (instrument.getHighMinusLow() == 100.0)
								prepStmt.setString(2, "BUYFAST");
							else if (instrument.getHighMinusLow() == -100.0)
								prepStmt.setString(2, "SELLFAST");

							prepStmt.executeUpdate();

						} catch (SQLException e) {
							LOGGER.info(
									"StreamingQuoteStorageImpl.saveAndGenerateSignal(): ERROR: SQLException on fetching data from Table, cause: "
											+ e.getMessage() + ">>" + e.getCause());
						}
					}
					prepStmt.close();

				} catch (SQLException e) {
					LOGGER.info(
							"StreamingQuoteStorageImpl.saveAndGenerateSignal(): ERROR: SQLException on fetching data from Table, cause: "
									+ e.getMessage() + ">>" + e.getCause());
				}
			} else {
				LOGGER.info("StreamingQuoteStorageImpl.saveAndGenerateSignal(): ERROR: DB conn is null !!!");
			}
		}
	}

	private ArrayList<String> fetchDistinctSymbolList() {
		ArrayList<String> tradingSymbolList = new ArrayList<String>();
		if (conn != null) {
			try {
				Statement stmt = conn.createStatement();
				String openSql = "SELECT distinct tradingsymbol FROM " + quoteTable + "_minuteData";
				ResultSet openRs = stmt.executeQuery(openSql);

				while (openRs.next()) {
					tradingSymbolList.add(openRs.getString("tradingsymbol"));
				}
				stmt.close();
			} catch (SQLException e) {
				LOGGER.info(
						"StreamingQuoteStorageImpl.fetchDistinctSymbolList(): ERROR: SQLException on fetching data from Table, cause: "
								+ e.getMessage() + ">>" + e.getCause());
			}
		} else {
			LOGGER.info("StreamingQuoteStorageImpl.fetchDistinctSymbolList(): ERROR: DB conn is null !!!");
		}
		return tradingSymbolList;
	}

	private ArrayList<InstrumentOHLCData> calculateSignal(ArrayList<InstrumentOHLCData> calculateEMASMA) {
		ArrayList<InstrumentOHLCData> instrumentOHLCDataList = new ArrayList<InstrumentOHLCData>();
		for (int i = 0; i < calculateEMASMA.size(); i++) {
			InstrumentOHLCData instrumentOHLCData = calculateEMASMA.get(i);
			if (instrumentOHLCData.getHigh() > instrumentOHLCData.getFastEma()) {
				instrumentOHLCData.setHighMinusLow(-100.0);
				instrumentOHLCDataList.add(instrumentOHLCData);
			}
			if (instrumentOHLCData.getLow() < instrumentOHLCData.getFastEmaLow()) {
				instrumentOHLCData.setHighMinusLow(100.0);
				instrumentOHLCDataList.add(instrumentOHLCData);
			}
			if (instrumentOHLCData.getHigh() > instrumentOHLCData.getSlowEma()) {
				instrumentOHLCData.setHighMinusLow(-200.0);
				instrumentOHLCDataList.add(instrumentOHLCData);
			}
			if (instrumentOHLCData.getLow() < instrumentOHLCData.getSlowEmaLow()) {
				instrumentOHLCData.setHighMinusLow(200.0);
				instrumentOHLCDataList.add(instrumentOHLCData);
			}
		}
		return instrumentOHLCDataList;
	}

	private ArrayList<InstrumentOHLCData> fetchSymbolTimeSeriesData(String tradingsymbol) {
		ArrayList<InstrumentOHLCData> instrumentOHLCDataList = new ArrayList<InstrumentOHLCData>();
		if (conn != null) {
			try {
				Statement stmt = conn.createStatement();
				String openSql = "SELECT close,high,low,open,tradingsymbol,dt,series,insertdt,lastTradedQt FROM "
						+ quoteTable + "_minuteData where tradingsymbol = '" + tradingsymbol + "' order by dt asc";
				ResultSet openRs = stmt.executeQuery(openSql);

				while (openRs.next()) {
					InstrumentOHLCData cama = new InstrumentOHLCData();

					cama.setClose(openRs.getDouble("close"));
					cama.setHigh(openRs.getDouble("high"));
					cama.setLow(openRs.getDouble("low"));
					cama.setOpen(openRs.getDouble("open"));
					cama.setDt(openRs.getString("dt"));
					cama.setSeries(openRs.getString("series"));
					cama.setInstrumentName(openRs.getString("tradingsymbol"));
					instrumentOHLCDataList.add(cama);
				}
				stmt.close();
			} catch (SQLException e) {
				LOGGER.info(
						"StreamingQuoteStorageImpl.fetchSymbolTimeSeriesData(): ERROR: SQLException on fetching data from Table, cause: "
								+ e.getMessage() + ">>" + e.getCause());
			}
		} else {
			LOGGER.info("StreamingQuoteStorageImpl.fetchSymbolTimeSeriesData(): ERROR: DB conn is null !!!");
		}

		return instrumentOHLCDataList;
	}

	public ArrayList<InstrumentOHLCData> calculateEMASMA(ArrayList<InstrumentOHLCData> instrumentList) {
		int fastEmaPeriods = 9;
		int slowEmaPeriods = 16;

		double fastEmaAccFactor = (double) 2 / (fastEmaPeriods + 1);
		double slowEmaAccFactor = (double) 2 / (slowEmaPeriods + 1);

		for (int i = 0; i < instrumentList.size(); i++) {

			// For High -----------

			if (i == slowEmaPeriods - 1) {
				for (int j = 0; j < slowEmaPeriods - 1; j++) {
					instrumentList.get(i)
							.setSlowEma(instrumentList.get(i).getSlowEma() + instrumentList.get(j).getHigh());
				}
				instrumentList.get(i).setSlowEma(instrumentList.get(i).getSlowEma() / (slowEmaPeriods));
			} else if (i >= slowEmaPeriods) {
				instrumentList.get(i).setSlowEma(
						((instrumentList.get(i).getHigh() - instrumentList.get(i - 1).getSlowEma()) * slowEmaAccFactor)
								+ instrumentList.get(i - 1).getSlowEma());
			}

			if (i == fastEmaPeriods - 1) {
				for (int j = 0; j < fastEmaPeriods - 1; j++) {
					instrumentList.get(i)
							.setFastEma(instrumentList.get(i).getFastEma() + instrumentList.get(j).getHigh());
				}
				instrumentList.get(i).setFastEma(instrumentList.get(i).getFastEma() / (fastEmaPeriods));
			} else if (i >= fastEmaPeriods) {
				instrumentList.get(i).setFastEma(
						((instrumentList.get(i).getHigh() - instrumentList.get(i - 1).getFastEma()) * fastEmaAccFactor)
								+ instrumentList.get(i - 1).getFastEma());
			}

			if (i >= fastEmaPeriods - 1) {
				double tempVal = 0.0;
				for (int j = i; j > i - fastEmaPeriods; j--) {
					tempVal = tempVal + instrumentList.get(j).getHigh();
				}
				instrumentList.get(i).setFastSma(tempVal / (fastEmaPeriods));
			}

			if (i >= slowEmaPeriods - 1) {
				double tempVal = 0.0;
				for (int j = i; j > i - slowEmaPeriods; j--) {
					tempVal = tempVal + instrumentList.get(j).getHigh();
				}
				instrumentList.get(i).setSlowSma(tempVal / (slowEmaPeriods));
			}

			// For Low -----------

			if (i == slowEmaPeriods - 1) {
				for (int j = 0; j < slowEmaPeriods - 1; j++) {
					instrumentList.get(i)
							.setSlowEmaLow(instrumentList.get(i).getSlowEmaLow() + instrumentList.get(j).getLow());
				}
				instrumentList.get(i).setSlowEmaLow(instrumentList.get(i).getSlowEmaLow() / (slowEmaPeriods));
			} else if (i >= slowEmaPeriods) {
				instrumentList.get(i)
						.setSlowEmaLow(((instrumentList.get(i).getLow() - instrumentList.get(i - 1).getSlowEmaLow())
								* slowEmaAccFactor) + instrumentList.get(i - 1).getSlowEmaLow());
			}

			if (i == fastEmaPeriods - 1) {
				for (int j = 0; j < fastEmaPeriods - 1; j++) {
					instrumentList.get(i)
							.setFastEmaLow(instrumentList.get(i).getFastEmaLow() + instrumentList.get(j).getLow());
				}
				instrumentList.get(i).setFastEmaLow(instrumentList.get(i).getFastEmaLow() / (fastEmaPeriods));
			} else if (i >= fastEmaPeriods) {
				instrumentList.get(i)
						.setFastEmaLow(((instrumentList.get(i).getLow() - instrumentList.get(i - 1).getFastEmaLow())
								* fastEmaAccFactor) + instrumentList.get(i - 1).getFastEmaLow());
			}

			if (i >= fastEmaPeriods - 1) {
				double tempVal = 0.0;
				for (int j = i; j > i - fastEmaPeriods; j--) {
					tempVal = tempVal + instrumentList.get(j).getLow();
				}
				instrumentList.get(i).setFastSmaLow(tempVal / (fastEmaPeriods));
			}

			if (i >= slowEmaPeriods - 1) {
				double tempVal = 0.0;
				for (int j = i; j > i - slowEmaPeriods; j--) {
					tempVal = tempVal + instrumentList.get(j).getLow();
				}
				instrumentList.get(i).setSlowSmaLow(tempVal / (slowEmaPeriods));
			}
		}
		return instrumentList;
	}
}