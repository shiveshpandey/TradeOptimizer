package com.gold.buzzer.main;

import java.io.IOException;

public class Test {

	public static void main(String[] args) throws IOException {

		/*
		 * HttpClient client = new DefaultHttpClient();
		 * 
		 * HttpGet get = new
		 * HttpGet(StreamingConfig.last10DaysVolumeDataFileUrls[2]);
		 * get.addHeader(StreamingConfig.ACCESS_CONTROL_ALLOW_ORIGIN, "*");
		 * get.addHeader("user-agent", StreamingConfig.USER_AGENT_VALUE);
		 * 
		 * try { HttpResponse response = client.execute(get); BufferedReader rd
		 * = new BufferedReader(new
		 * InputStreamReader(response.getEntity().getContent())); String line =
		 * "";
		 * 
		 * String[] readData; int lineSkip = 4; InstrumentVolatilityScore
		 * instrumentVolatilityScore; while ((line = rd.readLine()) != null) {
		 * try { readData = line.split(","); if (lineSkip == 0) {
		 * instrumentVolatilityScore = new InstrumentVolatilityScore();
		 * instrumentVolatilityScore.setInstrumentName(readData[2]);
		 * instrumentVolatilityScore.setPrice(Double.parseDouble(readData[4]));
		 * instrumentVolatilityScore.setDailyVolatility(Double.parseDouble(
		 * readData[5]) * 100.0);
		 * instrumentVolatilityScore.setAnnualVolatility(Double.parseDouble(
		 * readData[6]) * 100.0); } else lineSkip = lineSkip - 1; } catch
		 * (Exception e) { } } } catch (IOException e) { }
		 */
	}
}
