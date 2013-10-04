package com.tpc.util;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map.Entry;

public class ThreePhaseCommitUtility {

	private static String ACTIVE_PROCESS_FILENAME = "active_process_";
	private static String LOCAL_STATE_FILENAME = "local_state_";

	public static HashMap<String,String> fetchLocalState(int processId) throws IOException{
		HashMap<String,String> playList = new HashMap<String,String>();
		BufferedReader br = null;		
		String sCurrentLine; 
		br = new BufferedReader(new FileReader(LOCAL_STATE_FILENAME + "" + processId + ".txt"));
		while ((sCurrentLine = br.readLine()) != null) {
			String[] playListStr = sCurrentLine.split(";");
			int playListStrSize = playListStr.length;
			for(int i=0;i<playListStrSize;i++) {
				String[] splitList = playListStr[0].split(":");
				playList.put(splitList[0],splitList[1]);
			}
		}
		br.close();
		return playList;
	
	}
	
	public static boolean saveLocalState(int processId, HashMap<String,String> playList) throws IOException{
		if(playList == null) return false;
		StringBuffer sb = new StringBuffer();
		for(Entry<String, String> entry : playList.entrySet())
			sb.append(entry.getKey() + ":" + entry.getValue() + ";");
		File file = new File(LOCAL_STATE_FILENAME + "" + processId +".txt");
		if (!file.exists()) {
			file.createNewFile();
		}
		FileWriter fw = new FileWriter(file.getAbsoluteFile());
		BufferedWriter bw = new BufferedWriter(fw);
		bw.write(sb.toString());
		bw.close();
		return true;
	}

	public static ArrayList<Integer> getActiveProcess(int processId) throws IOException{
		BufferedReader br = null;	
		ArrayList<Integer> processIdList = new ArrayList<Integer>();
		String sCurrentLine; 
		br = new BufferedReader(new FileReader(ACTIVE_PROCESS_FILENAME + "" + processId + ".txt")); 
		while ((sCurrentLine = br.readLine()) != null) {
			String[] psList = sCurrentLine.split(",");
			int psListSize = psList.length;
			for(int i=0;i<psListSize;i++) {
				processIdList.add(Integer.parseInt(psList[i]));
			}
		}
		br.close();
		return processIdList;
	}

	public static boolean saveActiveProcess(int processId, ArrayList<Integer> processIdList) throws IOException{
		if(processIdList == null || processIdList.isEmpty()) return false;
		StringBuffer sb = new StringBuffer();
		int processIdListSize = processIdList.size();
		for(int i=0;i<processIdListSize;i++) {
			sb.append(processIdList.get(i));
		}
		File file = new File(ACTIVE_PROCESS_FILENAME + "" + processId +".txt");
		if (!file.exists()) {
			file.createNewFile();
		}
		FileWriter fw = new FileWriter(file.getAbsoluteFile());
		BufferedWriter bw = new BufferedWriter(fw);
		bw.write(sb.toString());
		bw.close();
		return true;
	}
}
