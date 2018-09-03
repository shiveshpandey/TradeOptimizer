package com.gold.buzzer.dao;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;

import com.gold.buzzer.models.InstrumentOHLCData;

public interface StreamingQuoteStorage {

	void initializeJDBCConn();

	void createDaysStreamingQuoteTable() throws SQLException;

	void saveInstrumentVolumeData(HashMap<String, ArrayList<InstrumentOHLCData>> stocksSymbolArray);

	void saveLast10DaysOHLCData(HashMap<String, ArrayList<InstrumentOHLCData>> stocksSymbolArray);

	void saveDaysOHLCVMinuteData(ArrayList<InstrumentOHLCData> instrumentOHLCDataList);

	void saveAndGenerateSignal(String symbol);
}
