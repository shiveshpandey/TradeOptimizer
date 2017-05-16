package com.streamquote.utils;

public class StreamingConfig {

	public static final String QUOTE_STREAMING_START_TIME = "00:15:01";
	public static final String QUOTE_STREAMING_END_TIME = "13:15:01";
	public static final String HISTORICAL_DATA_STREAM_START_TIME = "00:15:01";

	public static final String STREAMING_QUOTE_WS_URL_TEMPLATE = "wss://websocket.kite.trade/?";
	public static final Integer QUOTE_STREAMING_REINITIATE_DELAY_ON_INITIATE_FAIL = 500;
	public static final Integer QUOTE_STREAMING_WS_HEARTBIT_CHECK_TIME = 3000;
	public static final Integer QUOTE_STREAMING_WS_DATA_CHECK_TIME_ON_SUBSCRIBE = 5000;
	public static final Integer QUOTE_STREAMING_WS_SUBSCRIBE_DELAY_ON_INITIATE = 500;
	public static final Integer QUOTE_STREAMING_REINITIATE_RETRY_LIMIT = 5;
	public static final Boolean QUOTE_STREAMING_START_AT_BOOTUP = false;

	public static final String QUOTE_STREAMING_DB_URL = "jdbc:mysql://localhost:3306/StreamQuoteDB";
	public static final String QUOTE_STREAMING_DB_USER = "root";
	public static final String QUOTE_STREAMING_DB_PWD = "root";
	public static final String QUOTE_STREAMING_DB_TABLE_NAME_PRE_APPENDER = "StreamingQuoteMode_Mode";
	public static final String QUOTE_STREAMING_DB_TABLE_NAME_POST_APPENDER = "_Date";
	public static final Boolean QUOTE_STREAMING_DB_STORE_REQD = true;

	public static final Boolean QUOTE_STREAMING_HEART_BIT_MSG_PRINT = true;
	public static final Boolean QUOTE_STREAMING_QUOTE_FLOW_MSG_PRINT = true;
	public static final Boolean QUOTE_STREAMING_WEB_SERVICE_MSG_PRINT = true;

	public static String[] QUOTE_STREAMING_INSTRUMENTS_ARR = { "121345", "1793" };
	public static final String[] QUOTE_STREAMING_TRADING_HOLIDAYS = { "26-01-2016", "07-03-2016", "24-03-2016",
			"25-03-2016", "14-04-2016", "15-04-2016", "19-04-2016", "06-07-2016", "15-08-2016", "05-09-2016",
			"13-09-2016", "11-10-2016", "12-10-2016", "31-10-2016", "14-11-2016" };

	public static final String QUOTE_STREAMING_MODE_LTP = "ltp";
	public static final String QUOTE_STREAMING_MODE_QUOTE = "quote";
	public static final String QUOTE_STREAMING_MODE_FULL = "full";
	public static final String QUOTE_STREAMING_DEFAULT_MODE = QUOTE_STREAMING_MODE_QUOTE;

	public static final String USER_ID = "TNRS4216";
	public static final String API_KEY = "nwjyiweqm6z@4pmii";
	public static final String API_SECRET_KEY = "67s1q1k5yl9il439f80nrv6f@bunmwhq6";
	public static final String HIST_DATA_END_DATE = "2017-05-12";
	public static final String HIST_DATA_START_DATE = "2017-05-08";

	public static String getStreamingQuoteTbNameAppendFormat(String date) {
		return QUOTE_STREAMING_DB_TABLE_NAME_PRE_APPENDER + QUOTE_STREAMING_DEFAULT_MODE
				+ QUOTE_STREAMING_DB_TABLE_NAME_POST_APPENDER + date;
	}

}
