package com.tpc.util;

import java.util.Map.Entry;
import java.util.HashMap;
import java.util.Map;

public class FaultModel {
	Map<String,Integer> Vals = new HashMap<String,Integer>();
	public FaultModel(String condn) {
		if(condn==null) return;
		String t[] = condn.split(",");
		int v = Integer.parseInt(t[2]);
		if(t[0].equals("RECEIVE")) {
			v++;
		}
		Vals.put(t[0]+","+t[1], v);
		
	}
	
//	boolean hasBreached() {
//		return this.Vals.containsValue(0);
//	}
	
	boolean hasBreached(String key) {
		if(this.Vals.containsKey(key)) {
			return (this.Vals.get(key) == 0);
		}
		return false;
	}
	
	void updateModel(String val) {
		if(this.Vals.containsKey(val)) {
			this.Vals.put(val, Vals.get(val) - 1);
		}
	}
}
