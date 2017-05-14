package com.streamquote.websocket;

public interface IwsSessionNotifier {

	public void notifyWsInitiateFailed();

	public void notifyWsSessionOpened();

	public void notifyWsSessionClosed(boolean toTerminate);

	public void notifyWsDataMissedAfterSubscribe();

	public void notifyWsHeartBitExpired();
}
