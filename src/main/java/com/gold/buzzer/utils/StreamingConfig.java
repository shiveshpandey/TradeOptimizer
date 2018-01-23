package com.gold.buzzer.utils;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.TimeZone;

public class StreamingConfig {

	public static final String QUOTE_STREAMING_START_TIME = "09:00:00";
	public static final String QUOTE_STREAMING_END_TIME = "15:15:00";
	public static final String QUOTE_PRIORITY_SETTING_TIME = "09:22:00";
	public static final String DB_CONNECTION_CLOSING_TIME = "15:30:00";
	public static final int tokenCountForTrade = 100;
	public static final int secondsValue = 1000;
	public static final int averagePerScriptInvestment = 50000;

	public static final String USER_ID = "TNRS4216";
	public static final String API_KEY = "nwjyiweqm6z@4pmii";
	public static final String API_SECRET_KEY = "67s1q1k5yl9il439f80nrv6f@bunmwhq6";
	public static final String ACCESS_CONTROL_ALLOW_ORIGIN = "Access-Control-Allow-Origin";

	public static final String last10DaysOHLCZipFilePath = "C:/Users/shiva/Downloads/";

	public static final String QUOTE_STREAMING_DB_URL = "jdbc:mysql://localhost:3306/StreamQuoteDB";
	public static final String QUOTE_STREAMING_DB_USER = "root";
	public static final String QUOTE_STREAMING_DB_PWD = "root";
	public static final String QUOTE_STREAMING_DB_TABLE_NAME_PRE_APPENDER = "Zerodha";
	public static final String QUOTE_STREAMING_DB_TABLE_NAME_POST_APPENDER = "_Date_";

	public static final String[] QUOTE_STREAMING_TRADING_HOLIDAYS = { "26-01-2018", "13-02-2018", "02-03-2018",
			"29-03-2018", "30-03-2018", "01-05-2018", "15-08-2018", "22-08-2018", "13-09-2018", "20-09-2018",
			"02-10-2018", "18-10-2018", "07-11-2018", "08-11-2018", "23-11-2018", "25-12-2018" };

	public static Object[] last10DaysOHLCFileNames = last10DaysFileNameListString(QUOTE_STREAMING_TRADING_HOLIDAYS,
			"cm", "bhav.csv.zip", "ddMMMyyyy", 10, true);

	public static Object[] last10DaysVolumeDataFileNames = last10DaysFileNameListString(
			QUOTE_STREAMING_TRADING_HOLIDAYS, "MTO_", ".DAT", "ddMMyyyy", 10, false);

	public static Object[] nseVolatilityDataFileNames = last10DaysFileNameListString(QUOTE_STREAMING_TRADING_HOLIDAYS,
			"CMVOLT_", ".CSV", "ddMMyyyy", 10, false);

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
			"https://www.nseindia.com/content/historical/EQUITIES/" + last10DaysOHLCFileNames[0],
			"https://www.nseindia.com/content/historical/EQUITIES/" + last10DaysOHLCFileNames[1],
			"https://www.nseindia.com/content/historical/EQUITIES/" + last10DaysOHLCFileNames[2],
			"https://www.nseindia.com/content/historical/EQUITIES/" + last10DaysOHLCFileNames[3],
			"https://www.nseindia.com/content/historical/EQUITIES/" + last10DaysOHLCFileNames[4],
			"https://www.nseindia.com/content/historical/EQUITIES/" + last10DaysOHLCFileNames[5],
			"https://www.nseindia.com/content/historical/EQUITIES/" + last10DaysOHLCFileNames[6],
			"https://www.nseindia.com/content/historical/EQUITIES/" + last10DaysOHLCFileNames[7],
			"https://www.nseindia.com/content/historical/EQUITIES/" + last10DaysOHLCFileNames[8],
			"https://www.nseindia.com/content/historical/EQUITIES/" + last10DaysOHLCFileNames[9] };

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
	public static final String googleFinanceUrl = "https://www.google.com/finance/getprices?f=d%2Cc%2Ch%2Cl%2Co&i=60&p=10d&q=ZZZZZ&x=NSE";
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

	public static String googleFetchDataString(String instrumentName, int days, int period) {
		return "https://www.google.com/finance/getprices?f=d%2Cc%2Ch%2Cl%2Co&i=" + period + "&p=" + days + "d&q="
				+ instrumentName + "&x=NSE";
	}

	public static String getStreamingQuoteTbNameAppendFormat(String date) {
		return QUOTE_STREAMING_DB_TABLE_NAME_PRE_APPENDER + QUOTE_STREAMING_DB_TABLE_NAME_POST_APPENDER + date;
	}

	@SuppressWarnings("deprecation")
	public static Object[] last10DaysFileNameListString(String[] holidayList, String prefix, String postfix,
			String dateFormatString, int noOfDays, boolean prefixYearMonth) {
		TimeZone.setDefault(TimeZone.getTimeZone("IST"));
		DateFormat dateFormat = new SimpleDateFormat(dateFormatString);
		TimeZone timeZone = TimeZone.getTimeZone("IST");
		dateFormat.setTimeZone(timeZone);
		Calendar cal = Calendar.getInstance();
		ArrayList<String> dateList = new ArrayList<String>();

		for (int i = 0; dateList.size() < noOfDays && i < 100; i++) {
			cal.add(Calendar.DATE, -1);
			try {
				Date today = dateFormat.parse(dateFormat.format(cal.getTime()));
				boolean itsHoliday = false;
				for (String tradingDay : holidayList) {
					Date refDay = new SimpleDateFormat("dd-MM-yyyy").parse(tradingDay);
					if (today.compareTo(refDay) == 0)
						itsHoliday = true;
				}
				if (!itsHoliday && today.getDay() + 1 != Calendar.SATURDAY && today.getDay() + 1 != Calendar.SUNDAY) {
					dateList.add(dateFormat.format(cal.getTime()));
				}

			} catch (ParseException e) {
				e.printStackTrace();
			}
		}
		if (prefixYearMonth) {
			for (int i = 0; i < dateList.size(); i++) {
				dateList.set(i,
						dateList.get(i).substring(dateList.get(i).length() - 4, dateList.get(i).length()) + "/"
								+ dateList.get(i).substring(2, dateList.get(i).length() - 4).toUpperCase() + "/"
								+ prefix + dateList.get(i).toUpperCase() + postfix);
			}
		} else {
			for (int i = 0; i < dateList.size(); i++) {
				dateList.set(i, prefix + dateList.get(i).toUpperCase() + postfix);
			}
		}
		return dateList.toArray();
	}

}
