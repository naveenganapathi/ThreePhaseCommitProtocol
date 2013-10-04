package com.tpc.util;

public class Message {
	private int processId;
	private String message;
	
	public String getMessage() {
		return message;
	}
	public void setMessage(String msg) {
		this.message = msg;
	}
	public int getProcessId() {
		return processId;
	}
	public void setProcessId(int processId) {
		this.processId = processId;
	}
}
