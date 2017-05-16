package com.streamquote.websocket;

import java.io.IOException;
import java.net.URI;
import java.nio.ByteBuffer;
import java.util.Timer;
import java.util.TimerTask;

import javax.websocket.ClientEndpoint;
import javax.websocket.CloseReason;
import javax.websocket.ContainerProvider;
import javax.websocket.OnClose;
import javax.websocket.OnMessage;
import javax.websocket.OnOpen;
import javax.websocket.Session;
import javax.websocket.WebSocketContainer;

import com.streamquote.utils.StreamingConfig;

@ClientEndpoint
public class WebsocketClientEndpoint {
	Session userSession = null;
	private MessageHandler messageHandler;
	private WebServiceSessionNotifier sessionNotifier = null;
	private static Timer hbTimer = null;
	private static final int hbTimeDelay = StreamingConfig.QUOTE_STREAMING_WS_HEARTBIT_CHECK_TIME;
	private boolean terminate = false;

	public WebsocketClientEndpoint(URI endpointURI, WebServiceSessionNotifier sessionNotifier) {
		try {
			this.sessionNotifier = sessionNotifier;
			System.out.println("WebsocketClientEndpoint.WebsocketClientEndpoint(): creating WebSocketContainer...");
			WebSocketContainer container = ContainerProvider.getWebSocketContainer();
			container.connectToServer(this, endpointURI);
		} catch (Exception e) {
			System.out.println(
					"WebsocketClientEndpoint.WebsocketClientEndpoint(): ERROR: Exception on container connectToServer, reason: "
							+ e.getMessage());
			this.sessionNotifier.notifyWsInitiateFailed();
		}
	}

	@OnOpen
	public void onOpen(Session userSession) {
		System.out.println("WebsocketClientEndpoint.onOpen(): Opening WebSocket...");
		this.userSession = userSession;

		sessionNotifier.notifyWsSessionOpened();
	}

	@OnClose
	public void onClose(Session userSession, CloseReason reason) {
		System.out.println(
				"WebsocketClientEndpoint.onClose(): Closing Websocket.... Reason[" + reason.getReasonPhrase() + "]");
		try {
			this.userSession.close();
		} catch (IOException e) {
			System.out.println("WebsocketClientEndpoint.onClose(): ERROR: IOException on userSession close!!!");
			e.printStackTrace();
		}
		this.userSession = null;

		if (hbTimer != null) {
			hbTimer.cancel();
			hbTimer = null;
		}

		sessionNotifier.notifyWsSessionClosed(terminate);
	}

	@OnMessage
	public void onMessage(ByteBuffer buffer) {
		if (messageHandler != null) {
			messageHandler.handleMessage(buffer);
		}

		fireWSHeartBitMonitorTimer();
	}

	@OnMessage
	public void onMessage(String message) {
		System.out.println("WebsocketClientEndpoint.onMessage(): [String Message]: \n" + message);
	}

	public void addMessageHandler(MessageHandler msgHandler) {
		System.out.println("WebsocketClientEndpoint.addMessageHandler(): Adding MessageHandler...");
		this.messageHandler = msgHandler;
	}

	public void sendMessage(String message) {
		System.out.println("WebsocketClientEndpoint.sendMessage(): sending message");
		this.userSession.getAsyncRemote().sendText(message);
	}

	public void forceClose(boolean terminate) {
		System.out.println("WebsocketClientEndpoint.forceClose(): Force Closing Websocket....");
		try {
			this.terminate = terminate;
			this.userSession.close();
		} catch (IOException e) {
			System.out
					.println("WebsocketClientEndpoint.forceClose(): ERROR: IOException on userSession force close!!!");
			e.printStackTrace();
		}
		this.userSession = null;
	}

	public static interface MessageHandler {
		public void handleMessage(ByteBuffer buffer);
	}

	private void fireWSHeartBitMonitorTimer() {
		if (hbTimer != null) {
			hbTimer.cancel();
		}
		hbTimer = new Timer("WS HeartBit Timer");
		hbTimer.schedule(new TimerTask() {
			@Override
			public void run() {
				System.out.println(
						"WebsocketClientEndpoint.onMessage().new TimerTask().run(): ERROR: Streaming Quote WS HeartBit Timer Fired, notifying session notifier !!!");
				sessionNotifier.notifyWsHeartBitExpired();
			}
		}, hbTimeDelay);
	}
}
