package com.tpc.util;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map.Entry;

public class ThreePhaseCommitUtility {
	
	private static String ACTIVE_PROCESS_FILENAME = "active_process_";
	private static String LOCAL_STATE_FILENAME = "local_state_";
	public static final String DT_LOG_FILE_PREFIX = "DT_log_";
	/**
	 * To serialize the message to be sent
	 * @param message
	 * @return
	 */
	public static String serializeMessage(Message message) {
		if(message == null)
			return null;
		else
		  return message.getProcessId()+":"+message.getTransactionId()+":"+message.getMessage();		
	}
	
	/**
	 * To deserialize the received message.
	 * @param rawMessage
	 * @return
	 */
	public static Message deserializeMessage(String rawMessage) {
		if(rawMessage == null) {
			return null;
		} else {
			Message message = new Message();
			String messageParts[] = rawMessage.split(":");
			message.setMessage(messageParts[2]);
			message.setProcessId(Integer.parseInt(messageParts[0]));
			message.setTransactionId(Integer.parseInt(messageParts[1]));
			return message;
		}
	}
	
	/**
	 * To deserialize a list of messages.
	 * @param rawMessages
	 * @return
	 */
	public static List<Message> deserializeMessages(List<String> rawMessages) {
		if (rawMessages == null) {
			return null;			
		} else {
			List<Message> messages = new ArrayList<Message>();
			for (String rawMessage : rawMessages) {
				messages.add(deserializeMessage(rawMessage));
			}
			return messages;
		}
	}
	
	/**
	 * To write content to DT Log.
	 * @param process
	 * @param message
	 * @return
	 * @throws IOException 
	 */
	public static boolean writeMessageToDTLog (int process,LogRecord record) throws IOException {
		PrintWriter out = new PrintWriter(new BufferedWriter(new FileWriter(DT_LOG_FILE_PREFIX+process+".txt", true)));
	    out.println(record.getTransactionId()+":"+record.getMessage());
	    out.close();
		return true;
	}
	
	/**
	 * To fetch the latest message from DT log.
	 * @param process
	 * @return
	 * @throws IOException
	 */
	public static LogRecord fetchMessageFromDTLog(int process) throws IOException  {
		BufferedReader br = new BufferedReader(new FileReader(DT_LOG_FILE_PREFIX+process+".txt"));
		String temp,res = null;
		while((temp = br.readLine()) != null) {
			res = temp;
		}
		br.close();
		if (res == null) {
			return null;
		}
		String val[] = res.split(":");
		LogRecord record = new LogRecord();
		record.setMessage(val[1]);
		record.setTransactionId(Integer.parseInt(val[0]));
		return record;
	}

	/**
	 * To fetch the local state of a process 
	 * @param processId
	 * @return
	 * @throws IOException
	 */
	public static HashMap<String,String> fetchLocalState(int processId) throws IOException{
		HashMap<String,String> playList = new HashMap<String,String>();
		BufferedReader br = null;		
		String sCurrentLine; 
		br = new BufferedReader(new FileReader(LOCAL_STATE_FILENAME + "" + processId + ".txt"));
		while ((sCurrentLine = br.readLine()) != null) {
			String[] playListStr = sCurrentLine.split(";");
			for(String playListEntry : playListStr) {
				String[] splitList = playListEntry.split(":");
				playList.put(splitList[0],splitList[1]);
			}
		}
		br.close();
		return playList;
	
	}
	
	/**
	 * To save the local state of a process
	 * @param processId
	 * @param playList
	 * @return
	 * @throws IOException
	 */
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

	/**
	 * To get the list of active processes for a process
	 * @param processId
	 * @return
	 * @throws IOException
	 */
	public static ArrayList<Integer> getActiveProcess(int processId) throws IOException{
		BufferedReader br = null;	
		ArrayList<Integer> processIdList = new ArrayList<Integer>();
		String sCurrentLine; 
		br = new BufferedReader(new FileReader(ACTIVE_PROCESS_FILENAME + "" + processId + ".txt")); 
		while ((sCurrentLine = br.readLine()) != null) {
			String[] psList = sCurrentLine.split(",");
			for(String psListEntry : psList) {
				processIdList.add(Integer.parseInt(psListEntry));
			}
		}
		br.close();
		return processIdList;
	}

	/**
	 * To save the list of active proceesses for a process
	 * @param processId
	 * @param processIdList
	 * @return
	 * @throws IOException
	 */
	public static boolean saveActiveProcess(int processId, ArrayList<Integer> processIdList) throws IOException{
		if(processIdList == null || processIdList.isEmpty()) return false;
		StringBuffer sb = new StringBuffer();
		for(int pId : processIdList) {
			sb.append(pId);
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
