package com.streamquote.utils;

public class StreamingConfig {

	public static final String QUOTE_STREAMING_START_TIME = "09:22:01";
	public static final String QUOTE_STREAMING_END_TIME = "15:10:01";
	public static final String HISTORICAL_DATA_STREAM_START_TIME = "09:22:01";
	public static final String HISTORICAL_DATA_STREAM_END_TIME = "15:10:01";
	public static final int averagePerScriptInvestment = 50000;

	public static final String QUOTE_STREAMING_DB_URL = "jdbc:mysql://localhost:3306/StreamQuoteDB";
	public static final String QUOTE_STREAMING_DB_USER = "root";
	public static final String QUOTE_STREAMING_DB_PWD = "root";
	public static final String QUOTE_STREAMING_DB_TABLE_NAME_PRE_APPENDER = "StreamingQuote";
	public static final String QUOTE_STREAMING_DB_TABLE_NAME_POST_APPENDER = "_Date_";

	public static final String[] QUOTE_STREAMING_TRADING_HOLIDAYS = { "26-01-2017", "24-02-2017", "13-03-2017",
			"04-04-2017", "14-04-2017", "01-05-2017", "26-06-2017", "15-08-2017", "25-08-2017", "02-10-2017",
			"19-10-2017", "20-10-2017", "25-12-2017" };

	public static final String QUOTE_STREAMING_MODE_QUOTE = "quote";
	public static final String QUOTE_STREAMING_DEFAULT_MODE = QUOTE_STREAMING_MODE_QUOTE;

	public static final String USER_ID = "TNRS4216";
	public static final String API_KEY = "nwjyiweqm6z@4pmii";
	public static final String API_SECRET_KEY = "67s1q1k5yl9il439f80nrv6f@bunmwhq6";
	public static final String ACCESS_CONTROL_ALLOW_ORIGIN = "Access-Control-Allow-Origin";
	public static String[] stockListCollectingUrls = {
			"https://www.nseindia.com/live_market/dynaContent/live_analysis/most_active/allTopVolume1.json",
			"https://www.nseindia.com/live_market/dynaContent/live_analysis/most_active/allTopValue1.json",
			"https://www.nseindia.com/live_market/dynaContent/live_analysis/gainers/niftyGainers1.json",
			"https://www.nseindia.com/live_market/dynaContent/live_analysis/losers/niftyLosers1.json",
			"https://www.nseindia.com/live_market/dynaContent/live_analysis/gainers/allTopGainers1.json"/*
																										 * ,
																										 * "https://www.nseindia.com/live_market/dynaContent/live_analysis/losers/allTopLosers1.json",
																										 * "https://www.nseindia.com/live_market/dynaContent/live_analysis/gainers/fnoGainers1.json",
																										 * "https://www.nseindia.com/live_market/dynaContent/live_analysis/losers/fnoLosers1.json",
																										 * "https://www.nseindia.com/live_market/dynaContent/live_analysis/gainers/secLt20Gainers1.json",
																										 * "https://www.nseindia.com/live_market/dynaContent/live_analysis/losers/secLt20Losers1.json",
																										 * "https://www.nseindia.com/live_market/dynaContent/live_analysis/gainers/secGt20Gainers1.json",
																										 * "https://www.nseindia.com/live_market/dynaContent/live_analysis/losers/secGt20Losers1.json",
																										 * "https://www.nseindia.com/live_market/dynaContent/live_analysis/gainers/jrNiftyGainers1.json",
																										 * "https://www.nseindia.com/live_market/dynaContent/live_analysis/losers/jrNiftyLosers1.json"
																										 */ };

	public static String HIST_DATA_END_DATE = null;
	public static boolean tickerConnectionErrorOccured = false;
	public static String HIST_DATA_START_DATE = "2017-07-01";
	public static final String USER_AGENT_VALUE = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/59.0.3071.109 Safari/537.36";
	public static final Double MAX_VALUE = 9999999999.0000000000;
	public static final String nseVolatilityDataUrl = "https://www.nseindia.com/archives/nsccl/volt/CMVOLT_21072017.CSV";
	public static final String nifty200InstrumentCsvUrl = "https://www.nseindia.com/content/indices/ind_nifty200list.csv";

	public static String getStreamingQuoteTbNameAppendFormat(String date) {
		return QUOTE_STREAMING_DB_TABLE_NAME_PRE_APPENDER + QUOTE_STREAMING_DB_TABLE_NAME_POST_APPENDER + date;
	}

}
