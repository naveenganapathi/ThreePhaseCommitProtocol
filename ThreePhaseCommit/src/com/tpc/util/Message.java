package com.tpc.util;

public class Message {
	private int processId;
	private int transactionId;
	private String message;
	
	public Message(int processId,int transactionId,String message) {
		this.processId = processId;
		this.transactionId = transactionId;
		this.message = message;
	}
	public Message() {
		// TODO Auto-generated constructor stub
	}
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
	public int getTransactionId() {
		return transactionId;
	}
	public void setTransactionId(int transactionId) {
		this.transactionId = transactionId;
	}
	@Override
	public String toString() {
		return "Message [processId=" + processId + ", transactionId=" + transactionId + ", message=" + message + "]";
	}
	
	
}
