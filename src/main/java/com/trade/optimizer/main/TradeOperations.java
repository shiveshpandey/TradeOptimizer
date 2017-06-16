package com.trade.optimizer.main;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Logger;

import org.json.JSONObject;

import com.neovisionaries.ws.client.WebSocketException;
import com.streamquote.dao.StreamingQuoteStorage;
import com.trade.optimizer.exceptions.KiteException;
import com.trade.optimizer.kiteconnect.KiteConnect;
import com.trade.optimizer.models.HistoricalData;
import com.trade.optimizer.models.Holding;
import com.trade.optimizer.models.IndicesQuote;
import com.trade.optimizer.models.Instrument;
import com.trade.optimizer.models.Margins;
import com.trade.optimizer.models.Order;
import com.trade.optimizer.models.Position;
import com.trade.optimizer.models.Quote;
import com.trade.optimizer.models.Tick;
import com.trade.optimizer.models.Trade;
import com.trade.optimizer.models.TriggerRange;
import com.trade.optimizer.tickerws.KiteTicker;
import com.trade.optimizer.tickerws.OnConnect;
import com.trade.optimizer.tickerws.OnDisconnect;
import com.trade.optimizer.tickerws.OnTick;

public class TradeOperations {
	private final static Logger LOGGER = Logger.getLogger(TradeOptimizer.class.getName());

	/** Gets Margin. */
	public void getMargins(KiteConnect kiteconnect) throws KiteException {
		// Get margins returns margin model, you can pass equity or commodity as
		// arguments to get margins of respective segments.
		Margins margins = kiteconnect.getMargins("equity");
		LOGGER.info(margins.available.cash);
		LOGGER.info(margins.utilised.debits);
	}

	/**
	 * Place order.
	 * 
	 * @param quantity
	 * @param streamingQuoteDAOModeQuote
	 */
	public void placeOrder(KiteConnect kiteconnect, String instrumentToken, String buyOrSell, String quantity,
			StreamingQuoteStorage streamingQuoteDAOModeQuote) throws KiteException {
		/**
		 * Place order method requires a map argument which contains,
		 * tradingsymbol, exchange, transaction_type, order_type, quantity,
		 * product, price, trigger_price, disclosed_quantity, validity
		 * squareoff_value, stoploss_value, trailing_stoploss and variety (value
		 * can be regular, bo, co, amo) place order which will return order
		 * model which will have only orderId in the order model
		 *
		 * Following is an example param for SL order, if a call fails then
		 * KiteException will have error message in it Success of this call
		 * implies only order has been placed successfully, not order execution
		 */
		String[] instrumentDetails = streamingQuoteDAOModeQuote.getInstrumentDetailsOnTokenId(instrumentToken);
		Map<String, Object> param = new HashMap<String, Object>();

		param.put("quantity", quantity);
		param.put("order_type", "SL");
		param.put("tradingsymbol", instrumentDetails[1]);
		param.put("product", "CNC");
		param.put("exchange", instrumentDetails[2]);
		param.put("transaction_type", buyOrSell);
		param.put("validity", "DAY");
		param.put("price", "158.0");
		param.put("trigger_price", "157.5");
		param.put("tag", "myTag"); // tag is optional and it cannot be more
									// than 8 characters and only
									// alphanumeric is allowed
		// Order order = kiteconnect.placeOrder(param, "regular");
		LOGGER.info("Order Placed for : " + instrumentDetails[1]);
	}

	/** Place bracket order. */
	public void placeBracketOrder(KiteConnect kiteconnect) throws KiteException {
		/**
		 * Bracket order:- following is example param for bracket order*
		 * trailing_stoploss and stoploss_value are points not tick or price
		 */
		Map<String, Object> param10 = new HashMap<String, Object>() {

			private static final long serialVersionUID = 1L;

			{
				put("quantity", "1");
				put("order_type", "LIMIT");
				put("price", "1.4");
				put("transaction_type", "BUY");
				put("tradingsymbol", "ANKITMETAL");
				put("trailing_stoploss", "1");
				put("stoploss_value", "1");
				put("exchange", "NSE");
				put("validity", "DAY");
				put("squareoff_value", "1");
				put("product", "MIS");
			}
		};
		Order order10 = kiteconnect.placeOrder(param10, "bo");
		LOGGER.info(order10.orderId);
	}

	/** Place cover order. */
	public void placeCoverOrder(KiteConnect kiteconnect) throws KiteException {
		/**
		 * Cover Order:- following is example param for cover order and params
		 * sample key: quantity value: 1 key: price value: 0 key:
		 * transaction_type value: BUY key: tradingsymbol value: HINDALCO key:
		 * exchange value: NSE key: validity value: DAY key: trigger_price
		 * value: 157 key: order_type value: MARKET key: variety value: co key:
		 * product value: MIS
		 */
		Map<String, Object> param11 = new HashMap<String, Object>() {
			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			{
				put("price", "0");
				put("transaction_type", "BUY");
				put("quantity", "1");
				put("tradingsymbol", "HINDALCO");
				put("exchange", "NSE");
				put("validity", "DAY");
				put("trigger_price", "157");
				put("order_type", "MARKET");
				put("product", "MIS");
			}
		};
		Order order11 = kiteconnect.placeOrder(param11, "co");
		LOGGER.info(order11.orderId);
	}

	/** Get trigger range. */
	public void getTriggerRange(KiteConnect kiteconnect) throws KiteException {
		// You need to send a map with transaction_type, exchange and
		// tradingsymbol to get trigger range.
		Map<String, Object> params12 = new HashMap<>();
		params12.put("transaction_type", "SELL");
		TriggerRange triggerRange = kiteconnect.getTriggerRange("NSE", "RELIANCE", params12);
		LOGGER.info(String.valueOf(triggerRange.start));
	}

	/**
	 * Get orderbook.
	 * 
	 * @return
	 */
	public List<Order> getOrders(KiteConnect kiteconnect) throws KiteException {
		// Get orders returns order model which will have list of orders inside,
		// which can be accessed as follows,
		Order order = kiteconnect.getOrders();
		return order.orders;
		// for (int i = 0; i < order1.orders.size(); i++) {
		// LOGGER.info(order1.orders.get(i).tradingSymbol + " " +
		// order1.orders.get(i).orderId);
		// }
		// LOGGER.info("list of orders size is " + order1.orders.size());
	}

	/**
	 * Get order details
	 * 
	 * @return
	 */
	public Order getOrder(KiteConnect kiteconnect) throws KiteException {
		Order order = kiteconnect.getOrder("161028000217306");
		return order;
		// for (int i = 0; i < order.orders.size(); i++) {
		// LOGGER.info(order.orders.get(i).orderId + " " +
		// order.orders.get(i).status);
		// }
		// LOGGER.info("list size is " + order.orders.size());
	}

	/** Get tradebook */
	public void getTrades(KiteConnect kiteconnect) throws KiteException {
		// Returns tradebook.
		Trade trade = kiteconnect.getTrades();
		LOGGER.info(String.valueOf(trade.trades.size()));
	}

	/** Get trades for an order. */
	public void getTradesWithOrderId(KiteConnect kiteconnect) throws KiteException {
		// Returns trades for the given order.
		Trade trade1 = kiteconnect.getTrades("161007000088484");
		LOGGER.info(String.valueOf(trade1.trades.size()));
	}

	/** Modify order. */
	public void modifyOrder(KiteConnect kiteconnect) throws KiteException {
		// Order modify request will return order model which will contain only
		// order_id.
		Map<String, Object> params = new HashMap<String, Object>() {

			private static final long serialVersionUID = 1L;

			{
				put("quantity", "1");
				put("order_type", "SL");
				put("tradingsymbol", "HINDALCO");
				put("product", "CNC");
				put("exchange", "NSE");
				put("transaction_type", "BUY");
				put("validity", "DAY");
				put("price", "158.0");
				put("trigger_price", "157.5");
			}
		};
		Order order21 = kiteconnect.modifyOrder("161007000088484", params, "regular");
		LOGGER.info(order21.orderId);
	}

	/** Modify second leg SL-M order of bracket order */
	public void modifySecondLegBoSLM(KiteConnect kiteconnect) throws KiteException {
		Map<String, Object> params = new HashMap<String, Object>() {

			private static final long serialVersionUID = 1L;

			{
				put("order_id", "161220000183239");
				put("parent_order_id", "161220000178120");
				put("tradingsymbol", "ASHOKLEY");
				put("exchange", "NSE");
				put("quantity", "1");
				put("product", "MIS");
				put("validity", "DAY");
				put("trigger_price", "72.45");
				put("price", "0");
				put("order_type", "SL-M");
				put("transaction_type", "SELL");
			}
		};
		Order order = kiteconnect.modifyOrder("161220000183239", params, "bo");
		LOGGER.info(order.orderId);
	}

	/** Modify second leg LIMIT order of bracket order */
	public void modifySecondLegBoLIMIT(KiteConnect kiteconnect) throws KiteException {
		Map<String, Object> params = new HashMap<String, Object>() {

			private static final long serialVersionUID = 1L;

			{
				put("order_id", "161220000183238");
				put("parent_order_id", "161220000178120");
				put("tradingsymbol", "ASHOKLEY");
				put("exchange", "NSE");
				put("quantity", "1");
				put("product", "MIS");
				put("validity", "DAY");
				put("price", "82.45");
				put("trigger_price", "0");
				put("order_type", "LIMIT");
				put("transaction_type", "SELL");
			}
		};
		Order order = kiteconnect.modifyOrder("161220000183238", params, "bo");
		LOGGER.info(order.orderId);
	}

	/**
	 * Cancel an order
	 * 
	 * @param order
	 */
	public void cancelOrder(KiteConnect kiteconnect, Order order) throws KiteException {
		LOGGER.info(order.orderId + " regular" + " cancelled");
	}

	public void exitBracketOrder(KiteConnect kiteconnect) throws KiteException {
		Map<String, Object> params = new HashMap<>();
		params.put("parent_order_id", "161129000165203");
		Order order = kiteconnect.cancelOrder(params, "161129000221590", "bo");
		LOGGER.info(order.orderId);
	}

	/** Get all positions. */
	public void getPositions(KiteConnect kiteconnect) throws KiteException {
		// Get positions returns position model which contains list of
		// positions.
		Position position = kiteconnect.getPositions();
		LOGGER.info(String.valueOf(position.netPositions.size()));
	}

	/** Get holdings. */
	public Holding getHoldings(KiteConnect kiteconnect) throws KiteException {
		return kiteconnect.getHoldings();
	}

	/** Converts position */
	public void modifyProduct(KiteConnect kiteconnect) throws KiteException {
		// Modify product can be used to change MIS to NRML(CNC) or NRML(CNC) to
		// MIS.
		Map<String, Object> param6 = new HashMap<String, Object>() {

			private static final long serialVersionUID = 1L;

			{
				put("exchange", "NSE");
				put("tradingsymbol", "RELIANCE");
				put("transaction_type", "BUY");
				put("position_type", "day"); // can also be overnight
				put("quantity", "1");
				put("old_product", "MIS");
				put("new_product", "CNC");
			}
		};
		JSONObject jsonObject6 = kiteconnect.modifyProduct(param6);
		LOGGER.info(String.valueOf(jsonObject6));
	}

	/** Get all instruments that can be traded using kite connect. */
	public void getAllInstruments(KiteConnect kiteconnect) throws KiteException, IOException {
		// Get all instruments list. This call is very expensive as it involves
		// downloading of large data dump.
		// Hence, it is recommended that this call be made once and the results
		// stored locally once every morning before market opening.
		List<Instrument> instruments = kiteconnect.getInstruments();
		LOGGER.info(String.valueOf(instruments.size()));
	}

	/** Get instruments for the desired exchange. */
	public List<Instrument> getInstrumentsForExchange(KiteConnect kiteconnect, String exchangeName)
			throws KiteException, IOException {
		// Get instruments for an exchange.
		return kiteconnect.getInstruments(exchangeName);
		// LOGGER.info(nseInstruments.size());
	}

	/**
	 * Get quote for a scrip. For indices use getQuoteIndices.
	 */
	@SuppressWarnings("unused")
	public void getQuote(KiteConnect kiteconnect) throws KiteException {
		// Get quotes returns quote for desired tradingsymbol.
		Quote quote = kiteconnect.getQuote("NSE", "RELIANCE");
	}

	/** Get quote for a scrip. */
	public void getQuoteIndices(KiteConnect kiteconnect) throws KiteException {
		// Get quotes returns quote for desired tradingsymbol.
		IndicesQuote quote = kiteconnect.getQuoteIndices("NSE", "NIFTY 50");
		LOGGER.info(String.valueOf(quote.lastPrice));
	}

	/** Get historical data for an instrument. */
	public HistoricalData getHistoricalData(KiteConnect kiteconnect, String fromDate, String toDate, String interval,
			String tokenName) throws KiteException {
		/**
		 * Get historical data dump, requires from and to date, intrument token,
		 * interval returns historical data object which will have list of
		 * historical data inside the object
		 */
		Map<String, Object> param = new HashMap<String, Object>();
		param.put("from", fromDate);
		param.put("to", toDate);

		return kiteconnect.getHistoricalData(param, tokenName, interval);
		// LOGGER.info(historicalData.dataArrayList.size());
		// LOGGER.info(historicalData.dataArrayList.get(0).volume);
		// LOGGER.info(historicalData.dataArrayList.get(historicalData.dataArrayList.size()
		// - 1).volume);
	}

	/** Logout user. */
	public void logout(KiteConnect kiteconnect) throws KiteException {
		/** Logout user and kill session. */
		JSONObject jsonObject10 = kiteconnect.logout();
		LOGGER.info(String.valueOf(jsonObject10));
	}

	/**
	 * Demonstrates ticker connection, subcribing for instruments, unsubscribing
	 * for instruments, set mode of tick data, ticker disconnection
	 */
	@SuppressWarnings({ "rawtypes", "unchecked" })
	public void tickerUsage(KiteConnect kiteconnect) throws IOException, WebSocketException {
		/**
		 * To get live price use com.trade.optimizer.tickerws websocket
		 * connection. It is recommended to use only one websocket connection at
		 * any point of time and make sure you stop connection, once user goes
		 * out of app.
		 */
		final ArrayList tokens = new ArrayList<>();
		tokens.add(53287175);
		final KiteTicker tickerProvider = new KiteTicker(kiteconnect);
		tickerProvider.setOnConnectedListener(new OnConnect() {
			@Override
			public void onConnected() {
				try {
					/**
					 * Subscribe ticks for token. By default, all tokens are
					 * subscribed for modeQuote.
					 */
					tickerProvider.subscribe(tokens);
				} catch (IOException e) {
					e.printStackTrace();
				} catch (WebSocketException e) {
					e.printStackTrace();
				} catch (KiteException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		});

		tickerProvider.setOnDisconnectedListener(new OnDisconnect() {
			@Override
			public void onDisconnected() {
				// your code goes here
			}
		});

		tickerProvider.setOnTickerArrivalListener(new OnTick() {
			@Override
			public void onTick(ArrayList<Tick> ticks) {
				LOGGER.info(String.valueOf(ticks.size()));
			}
		});

		/**
		 * for reconnection of ticker when there is abrupt network
		 * disconnection, use the following code by default tryReconnection is
		 * set to false
		 */
		tickerProvider.setTryReconnection(true);
		// minimum value must be 5 for time interval for reconnection
		try {
			tickerProvider.setTimeIntervalForReconnection(5);
		} catch (KiteException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		// set number to times ticker can try reconnection, for infinite retries
		// use -1
		tickerProvider.setMaxRetries(10);

		/**
		 * connects to com.trade.optimizer.tickerws server for getting live
		 * quotes
		 */
		tickerProvider.connect();

		/**
		 * You can check, if websocket connection is open or not using the
		 * following method.
		 */
		boolean isConnected = tickerProvider.isConnectionOpen();
		LOGGER.info(String.valueOf(isConnected));

		/**
		 * set mode is used to set mode in which you need tick for list of
		 * tokens. Ticker allows three modes, modeFull, modeQuote, modeLTP. For
		 * getting only last traded price, use modeLTP For getting last traded
		 * price, last traded quantity, average price, volume traded today,
		 * total sell quantity and total buy quantity, open, high, low, close,
		 * change, use modeQuote For getting all data with depth, use modeFull
		 */
		tickerProvider.setMode(tokens, KiteTicker.modeLTP);

		// Unsubscribe for a token.
		tickerProvider.unsubscribe(tokens);

		// After using com.trade.optimizer.tickerws, close websocket connection.
		tickerProvider.disconnect();
	}
}
