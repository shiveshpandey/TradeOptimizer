package com.trade.optimizer.utils;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.TimeZone;

public class TradingHolidays {

	public static boolean isHoliday() {
		DateFormat dateFormat = new SimpleDateFormat("dd-MM-yyyy");
		TimeZone timeZone = TimeZone.getTimeZone("IST");
		dateFormat.setTimeZone(timeZone);
		String todayString = dateFormat.format(Calendar.getInstance(timeZone).getTime());
		String tradingHolidaysArr[] = StreamingConfig.QUOTE_STREAMING_TRADING_HOLIDAYS;
		try {
			Date today = dateFormat.parse(todayString);
			for (String tradingDay : tradingHolidaysArr) {
				Date refDay = dateFormat.parse(tradingDay);
				if (today.compareTo(refDay) == 0) {
					return true;
				}
			}
		} catch (ParseException e) {
			e.printStackTrace();
			return true;
		}
		return false;
	}
}
