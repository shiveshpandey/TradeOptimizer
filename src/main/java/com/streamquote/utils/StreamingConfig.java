package com.streamquote.utils;

public class StreamingConfig {

	public static final String QUOTE_STREAMING_START_TIME = "09:00:00";
	public static final String QUOTE_STREAMING_END_TIME = "15:15:00";
	public static final String QUOTE_PRIORITY_SETTING_TIME = "09:22:00";
	public static final String DB_CONNECTION_CLOSING_TIME = "15:30:00";
	public static final int tokenCountForTrade = 30;
	public static final int secondsValue = 1000;
	public static final int averagePerScriptInvestment = 50000;

	public static final String USER_ID = "TNRS4216";
	public static final String API_KEY = "nwjyiweqm6z@4pmii";
	public static final String API_SECRET_KEY = "67s1q1k5yl9il439f80nrv6f@bunmwhq6";
	public static final String ACCESS_CONTROL_ALLOW_ORIGIN = "Access-Control-Allow-Origin";

	public static final String last10DaysOHLCZipFilePath = "C:/Users/shiva/Downloads/";
	public static final String last10DaysOHLCFilePrefix = "2017/AUG/";

	public static String[] last10DaysOHLCFileNames = { "cm16AUG2017bhav.csv.zip", "cm15AUG2017bhav.csv.zip",
			"cm14AUG2017bhav.csv.zip", "cm11AUG2017bhav.csv.zip", "cm10AUG2017bhav.csv.zip", "cm09AUG2017bhav.csv.zip",
			"cm08AUG2017bhav.csv.zip", "cm05AUG2017bhav.csv.zip", "cm04AUG2017bhav.csv.zip", "cm03AUG2017bhav.csv.zip",
			"cm02AUG2017bhav.csv.zip" };

	public static String[] last10DaysVolumeDataFileNames = { "MTO_16082017.DAT", "MTO_15082017.DAT", "MTO_14082017.DAT",
			"MTO_11082017.DAT", "MTO_10082017.DAT", "MTO_09082017.DAT", "MTO_08082017.DAT", "MTO_05082017.DAT",
			"MTO_04082017.DAT", "MTO_03082017.DAT" };

	public static String[] nseVolatilityDataFileNames = { "CMVOLT_16082017.CSV", "CMVOLT_15082017.CSV",
			"CMVOLT_14082017.CSV", "CMVOLT_11082017.CSV", "CMVOLT_10082017.CSV", "CMVOLT_09082017.CSV",
			"CMVOLT_08082017.CSV", "CMVOLT_05082017.CSV", "CMVOLT_04082017.CSV", "CMVOLT_03082017.CSV" };

	public static final String QUOTE_STREAMING_DB_URL = "jdbc:mysql://localhost:3306/StreamQuoteDB";
	public static final String QUOTE_STREAMING_DB_USER = "root";
	public static final String QUOTE_STREAMING_DB_PWD = "root";
	public static final String QUOTE_STREAMING_DB_TABLE_NAME_PRE_APPENDER = "Zerodha";
	public static final String QUOTE_STREAMING_DB_TABLE_NAME_POST_APPENDER = "_Date_";

	public static final String[] QUOTE_STREAMING_TRADING_HOLIDAYS = { "26-01-2017", "24-02-2017", "13-03-2017",
			"04-04-2017", "14-04-2017", "01-05-2017", "26-06-2017", "15-08-2017", "25-08-2017", "02-10-2017",
			"19-10-2017", "20-10-2017", "25-12-2017" };

	public static String[] stockListCollectingUrls = {
			"https://www.nseindia.com/live_market/dynaContent/live_analysis/most_active/allTopVolume1.json",
			"https://www.nseindia.com/live_market/dynaContent/live_analysis/most_active/allTopValue1.json",
			"https://www.nseindia.com/live_market/dynaContent/live_analysis/gainers/niftyGainers1.json",
			"https://www.nseindia.com/live_market/dynaContent/live_analysis/losers/niftyLosers1.json",
			"https://www.nseindia.com/live_market/dynaContent/live_analysis/gainers/allTopGainers1.json",
			"https://www.nseindia.com/live_market/dynaContent/live_analysis/losers/allTopLosers1.json",
			"https://www.nseindia.com/live_market/dynaContent/live_analysis/gainers/fnoGainers1.json",
			"https://www.nseindia.com/live_market/dynaContent/live_analysis/losers/fnoLosers1.json",
			"https://www.nseindia.com/live_market/dynaContent/live_analysis/gainers/secLt20Gainers1.json",
			"https://www.nseindia.com/live_market/dynaContent/live_analysis/losers/secLt20Losers1.json",
			"https://www.nseindia.com/live_market/dynaContent/live_analysis/gainers/secGt20Gainers1.json",
			"https://www.nseindia.com/live_market/dynaContent/live_analysis/losers/secGt20Losers1.json",
			"https://www.nseindia.com/live_market/dynaContent/live_analysis/gainers/jrNiftyGainers1.json",
			"https://www.nseindia.com/live_market/dynaContent/live_analysis/losers/jrNiftyLosers1.json" };

	public static String[] last10DaysOHLCFileUrls = {
			"https://www.nseindia.com/content/historical/EQUITIES/" + last10DaysOHLCFilePrefix
					+ last10DaysOHLCFileNames[0],
			"https://www.nseindia.com/content/historical/EQUITIES/" + last10DaysOHLCFilePrefix
					+ last10DaysOHLCFileNames[1],
			"https://www.nseindia.com/content/historical/EQUITIES/" + last10DaysOHLCFilePrefix
					+ last10DaysOHLCFileNames[2],
			"https://www.nseindia.com/content/historical/EQUITIES/" + last10DaysOHLCFilePrefix
					+ last10DaysOHLCFileNames[3],
			"https://www.nseindia.com/content/historical/EQUITIES/" + last10DaysOHLCFilePrefix
					+ last10DaysOHLCFileNames[4],
			"https://www.nseindia.com/content/historical/EQUITIES/" + last10DaysOHLCFilePrefix
					+ last10DaysOHLCFileNames[5],
			"https://www.nseindia.com/content/historical/EQUITIES/" + last10DaysOHLCFilePrefix
					+ last10DaysOHLCFileNames[6],
			"https://www.nseindia.com/content/historical/EQUITIES/" + last10DaysOHLCFilePrefix
					+ last10DaysOHLCFileNames[7],
			"https://www.nseindia.com/content/historical/EQUITIES/" + last10DaysOHLCFilePrefix
					+ last10DaysOHLCFileNames[8],
			"https://www.nseindia.com/content/historical/EQUITIES/" + last10DaysOHLCFilePrefix
					+ last10DaysOHLCFileNames[9] };

	public static String[] last10DaysVolumeDataFileUrls = {
			"https://www.nseindia.com/archives/equities/mto/" + last10DaysVolumeDataFileNames[0],
			"https://www.nseindia.com/archives/equities/mto/" + last10DaysVolumeDataFileNames[1],
			"https://www.nseindia.com/archives/equities/mto/" + last10DaysVolumeDataFileNames[2],
			"https://www.nseindia.com/archives/equities/mto/" + last10DaysVolumeDataFileNames[3],
			"https://www.nseindia.com/archives/equities/mto/" + last10DaysVolumeDataFileNames[4],
			"https://www.nseindia.com/archives/equities/mto/" + last10DaysVolumeDataFileNames[5],
			"https://www.nseindia.com/archives/equities/mto/" + last10DaysVolumeDataFileNames[6],
			"https://www.nseindia.com/archives/equities/mto/" + last10DaysVolumeDataFileNames[7],
			"https://www.nseindia.com/archives/equities/mto/" + last10DaysVolumeDataFileNames[8],
			"https://www.nseindia.com/archives/equities/mto/" + last10DaysVolumeDataFileNames[9] };

	public static String[] nseVolatilityDataUrl = {
			"https://www.nseindia.com/archives/nsccl/volt/" + nseVolatilityDataFileNames[0],
			"https://www.nseindia.com/archives/nsccl/volt/" + nseVolatilityDataFileNames[1],
			"https://www.nseindia.com/archives/nsccl/volt/" + nseVolatilityDataFileNames[2],
			"https://www.nseindia.com/archives/nsccl/volt/" + nseVolatilityDataFileNames[3],
			"https://www.nseindia.com/archives/nsccl/volt/" + nseVolatilityDataFileNames[4],
			"https://www.nseindia.com/archives/nsccl/volt/" + nseVolatilityDataFileNames[5],
			"https://www.nseindia.com/archives/nsccl/volt/" + nseVolatilityDataFileNames[6],
			"https://www.nseindia.com/archives/nsccl/volt/" + nseVolatilityDataFileNames[7],
			"https://www.nseindia.com/archives/nsccl/volt/" + nseVolatilityDataFileNames[8],
			"https://www.nseindia.com/archives/nsccl/volt/" + nseVolatilityDataFileNames[9] };

	public static final String USER_AGENT_VALUE = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/59.0.3071.109 Safari/537.36";
	public static final Double MAX_VALUE = 9999999999.0000000000;
	public static final String nifty200InstrumentCsvUrl = "https://www.nseindia.com/content/indices/ind_nifty200list.csv";

	public static final String QUOTE_STREAMING_MODE_QUOTE = "quote";
	public static final String QUOTE_STREAMING_DEFAULT_MODE = QUOTE_STREAMING_MODE_QUOTE;

	public static final double CAMA_H1 = 0.091;
	public static final double CAMA_H2 = 0.183;
	public static final double CAMA_H3 = 0.275;
	public static final double CAMA_H4 = 0.55;
	public static final double CAMA_L1 = 0.091;
	public static final double CAMA_L2 = 0.183;
	public static final double CAMA_L3 = 0.275;
	public static final double CAMA_L4 = 0.55;

	public static String getStreamingQuoteTbNameAppendFormat(String date) {
		return QUOTE_STREAMING_DB_TABLE_NAME_PRE_APPENDER + QUOTE_STREAMING_DB_TABLE_NAME_POST_APPENDER + date;
	}

}
