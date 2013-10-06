package com.tpc.util;

public class LogRecord {

	private int transactionId;
	private String message;
	
	public LogRecord(int transactionId,String message) {
		this.transactionId = transactionId;
		this.message = message;
	}
	
	public LogRecord() {
		
	}
	
	public int getTransactionId() {
		return transactionId;
	}
	public void setTransactionId(int transactionId) {
		this.transactionId = transactionId;
	}
	public String getMessage() {
		return message;
	}
	public void setMessage(String message) {
		this.message = message;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((message == null) ? 0 : message.hashCode());
		result = prime * result + transactionId;
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		LogRecord other = (LogRecord) obj;
		if (message == null) {
			if (other.message != null)
				return false;
		} else if (!message.equals(other.message))
			return false;
		if (transactionId != other.transactionId)
			return false;
		return true;
	}

	@Override
	public String toString() {
		return "LogRecord [transactionId=" + transactionId + ", message=" + message + "]";
	}
	
	
	
}
