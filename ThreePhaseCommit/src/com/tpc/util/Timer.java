package com.tpc.util;

import java.util.Date;

public class Timer {

	long start;
	long timeoutInSeconds;
	
	public Timer(long a) {
		timeoutInSeconds = a;
	}
	public long getStart() {
		return start;
	}
	public void setStart(long start) {
		this.start = start;
	}
	public long getTimeoutInSeconds() {
		return timeoutInSeconds;
	}
	public void setTimeoutInSeconds(long timeout) {
		this.timeoutInSeconds = timeout;
	}
	
	public boolean hasTimedOut() {
		long cur = new Date().getTime();
		if (cur - start > timeoutInSeconds* 1000) {
			return true;
		} else {
			return false;
		}
	}
	
	public void start() {
		this.start = new Date().getTime();
				
	}
}
