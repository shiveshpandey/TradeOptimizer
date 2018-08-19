package com.gold.buzzer.main;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URL;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.logging.Logger;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;

import javax.servlet.ServletContext;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.DefaultHttpClient;
import org.json.JSONException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;

import com.gold.buzzer.dao.StreamingQuoteStorage;
import com.gold.buzzer.dao.StreamingQuoteStorageImpl;
import com.gold.buzzer.models.InstrumentOHLCData;
import com.gold.buzzer.models.InstrumentVolatilityScore;
import com.gold.buzzer.utils.StreamingConfig;

@SuppressWarnings("deprecation")
@Controller
public class GRSRuleEngine {

	private final static Logger LOGGER = Logger.getLogger(GRSRuleEngine.class.getName());

	private StreamingQuoteStorage streamingQuoteStorage = new StreamingQuoteStorageImpl();
	private List<InstrumentVolatilityScore> instrumentVolatilityScoreList = new ArrayList<InstrumentVolatilityScore>();
	private static Set<String> nseTradableInstrumentSymbol = new HashSet<String>();

	@Autowired
	ServletContext context;
	@Autowired
	HttpServletRequest request;
	@Autowired
	HttpServletResponse response;

	public List<InstrumentVolatilityScore> markInstrumentsTradable() {

		@SuppressWarnings({ "resource" })
		HttpClient client = new DefaultHttpClient();

		HttpGet get = new HttpGet(StreamingConfig.nifty200InstrumentCsvUrl);
		get.addHeader(StreamingConfig.ACCESS_CONTROL_ALLOW_ORIGIN, "*");
		get.addHeader("user-agent", StreamingConfig.USER_AGENT_VALUE);
		instrumentVolatilityScoreList = new ArrayList<InstrumentVolatilityScore>();
		try {
			HttpResponse response = client.execute(get);
			BufferedReader rd = new BufferedReader(new InputStreamReader(response.getEntity().getContent()));
			String line = "";

			String[] readData;
			boolean firstLine = true;
			InstrumentVolatilityScore instrumentVolatilityScore;
			while ((line = rd.readLine()) != null) {
				try {
					readData = line.split(",");
					if (!firstLine) {
						instrumentVolatilityScore = new InstrumentVolatilityScore();
						instrumentVolatilityScore.setInstrumentName(readData[2]);
						instrumentVolatilityScore.setTradable("tradable");

						instrumentVolatilityScoreList.add(instrumentVolatilityScore);
						nseTradableInstrumentSymbol.add(readData[2]);
					}
					firstLine = false;
				} catch (Exception e) {
					LOGGER.info("Error GRSRuleEngine :- " + e.getMessage() + " >> " + e.getCause());
				}
			}
		} catch (IOException e) {
			LOGGER.info("Error GRSRuleEngine :- " + e.getMessage() + " >> " + e.getCause());
		}
		return instrumentVolatilityScoreList;
	}

	public void saveInstrumentVolumeData() {
		@SuppressWarnings({ "resource" })
		HttpClient client = new DefaultHttpClient();
		HashMap<String, ArrayList<InstrumentOHLCData>> instrumentVolumeLast10DaysDataList = new HashMap<String, ArrayList<InstrumentOHLCData>>();
		for (int count = 0; count < StreamingConfig.getLast10DaysVolumeDataFileUrls().length; count++) {
			HttpGet get = new HttpGet(StreamingConfig.getLast10DaysVolumeDataFileUrls()[2]);
			get.addHeader(StreamingConfig.ACCESS_CONTROL_ALLOW_ORIGIN, "*");
			get.addHeader("user-agent", StreamingConfig.USER_AGENT_VALUE);

			try {
				HttpResponse response = client.execute(get);
				BufferedReader rd = new BufferedReader(new InputStreamReader(response.getEntity().getContent()));
				String line = "";
				String[] readData;
				int lineSkip = 4;
				InstrumentOHLCData instrumentOHLCData;
				while ((line = rd.readLine()) != null) {
					try {
						readData = line.split(",");
						if (lineSkip == 0) {
							instrumentOHLCData = new InstrumentOHLCData();
							instrumentOHLCData.setInstrumentName(readData[2]);
							instrumentOHLCData.setClose(Double.parseDouble(readData[4]));
							instrumentOHLCData.setHigh(Double.parseDouble(readData[5]));
							instrumentOHLCData.setLow(Double.parseDouble(readData[6]));
							instrumentOHLCData.setDt(StreamingConfig.last10DaysDates[count].toString());
							if (nseTradableInstrumentSymbol.contains(readData[2])) {
								if (instrumentVolumeLast10DaysDataList.containsKey(readData[2]))
									instrumentVolumeLast10DaysDataList.get(readData[2]).add(instrumentOHLCData);
								else {
									ArrayList<InstrumentOHLCData> tempList = new ArrayList<InstrumentOHLCData>();
									tempList.add(instrumentOHLCData);
									instrumentVolumeLast10DaysDataList.put(readData[2], tempList);
								}
							}
						} else
							lineSkip = lineSkip - 1;
					} catch (Exception e) {
						LOGGER.info("Error GRSRuleEngine :- " + e.getMessage() + " >> " + e.getCause());
					}
				}
			} catch (IOException e) {
				LOGGER.info("Error GRSRuleEngine :- " + e.getMessage() + " >> " + e.getCause());
			}
		}
		streamingQuoteStorage.saveInstrumentVolumeData(instrumentVolumeLast10DaysDataList);
	}

	@RequestMapping(value = "/start", method = { RequestMethod.POST, RequestMethod.GET })
	public void startProcess() {
		try {
			StreamingConfig.TEMP_TEST_DATE = "19082018";

			System.out.println(StreamingConfig.TEMP_TEST_DATE);
			nseTradableInstrumentSymbol = new HashSet<String>();
			createInitialDayTables();

			instrumentVolatilityScoreList = markInstrumentsTradable();
			saveLast10DaysOHLCData();
			saveInstrumentVolumeData();
		} catch (JSONException e) {
			LOGGER.info("Error GRSRuleEngine.startProcess(): " + e.getMessage() + " >> " + e.getCause());
		}
	}

	private void createInitialDayTables() {
		if (streamingQuoteStorage != null) {

			streamingQuoteStorage.initializeJDBCConn();
			try {
				streamingQuoteStorage.createDaysStreamingQuoteTable();
			} catch (SQLException e) {
				LOGGER.info("Error GRSRuleEngine :- " + e.getMessage() + " >> " + e.getCause());
			}
		}
	}

	private void saveLast10DaysOHLCData() {
		HashMap<String, ArrayList<InstrumentOHLCData>> instrumentOHLCLast10DaysDataList = new HashMap<String, ArrayList<InstrumentOHLCData>>();
		for (int index = 0; index < StreamingConfig.getLast10DaysOHLCFileUrls().length; index++) {
			try {
				URL website = new URL(StreamingConfig.getLast10DaysOHLCFileUrls()[index]);
				BufferedInputStream in = null;
				FileOutputStream fout = null;

				try {
					in = new BufferedInputStream(website.openStream());
					fout = new FileOutputStream(
							StreamingConfig.last10DaysOHLCZipFilePath + StreamingConfig.last10DaysOHLCFileNames[index]);

					final byte data[] = new byte[1024];
					int count;
					while ((count = in.read(data, 0, 1024)) != -1) {
						fout.write(data, 0, count);
					}
				} finally {
					if (in != null)
						in.close();

					if (fout != null)
						fout.close();
				}
				ZipFile zipFile = new ZipFile(
						StreamingConfig.last10DaysOHLCZipFilePath + StreamingConfig.last10DaysOHLCFileNames[index]);
				Enumeration<? extends ZipEntry> entries = zipFile.entries();

				while (entries.hasMoreElements()) {
					ZipEntry entry = entries.nextElement();
					InputStream stream = zipFile.getInputStream(entry);
					BufferedReader rd = new BufferedReader(new InputStreamReader(stream));
					String line = "";
					String[] readData;
					boolean firstLine = true;
					while ((line = rd.readLine()) != null) {
						try {
							readData = line.split(",");
							if (!firstLine) {
								InstrumentOHLCData instrumentOHLCData = new InstrumentOHLCData();
								instrumentOHLCData.setInstrumentName(readData[0]);
								instrumentOHLCData.setClose(Double.parseDouble(readData[5]));
								instrumentOHLCData.setHigh(Double.parseDouble(readData[3]));
								instrumentOHLCData.setLow(Double.parseDouble(readData[4]));
								instrumentOHLCData.setOpen(Double.parseDouble(readData[2]));
								instrumentOHLCData.setDt(StreamingConfig.last10DaysDates[index].toString());
								if (nseTradableInstrumentSymbol.contains(readData[0])) {
									if (instrumentOHLCLast10DaysDataList.containsKey(readData[0]))
										instrumentOHLCLast10DaysDataList.get(readData[0]).add(instrumentOHLCData);
									else {
										ArrayList<InstrumentOHLCData> tempList = new ArrayList<InstrumentOHLCData>();
										tempList.add(instrumentOHLCData);
										instrumentOHLCLast10DaysDataList.put(readData[0], tempList);
									}
								}
							}
							firstLine = false;
						} catch (Exception e) {
							LOGGER.info("Error GRSRuleEngine :- " + e.getMessage() + " >> " + e.getCause());
						}
					}
				}
				zipFile.close();
			} catch (Exception e) {
				LOGGER.info("Error GRSRuleEngine :- " + e.getMessage() + " >> " + e.getCause());
			}
		}
		streamingQuoteStorage.saveLast10DaysOHLCData(instrumentOHLCLast10DaysDataList);
	}
}
