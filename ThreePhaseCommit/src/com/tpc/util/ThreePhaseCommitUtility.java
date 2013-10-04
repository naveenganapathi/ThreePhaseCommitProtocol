package com.tpc.util;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.util.ArrayList;
import java.util.List;

/**
 * Util functions for 3PC protocol
 * @author naveen,vignesh
 *
 */
public class ThreePhaseCommitUtility {
	
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
		  return message.getProcessId()+":"+message.getMessage();		
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
			message.setMessage(messageParts[1]);
			message.setProcessId(Integer.parseInt(messageParts[0]));
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
	public static boolean writeMessageToDTLog (int process, String message) throws IOException {
		Writer writer = new BufferedWriter(new OutputStreamWriter(
		          new FileOutputStream(DT_LOG_FILE_PREFIX+process+".txt"), "utf-8"));
		writer.write(message+"\n");
		writer.close();
		return true;
	}
	
	/**
	 * To fetch the latest message from DT log.
	 * @param process
	 * @return
	 * @throws IOException
	 */
	public static String fetchMessageFromDTLog(int process) throws IOException  {
		BufferedReader br = new BufferedReader(new FileReader(DT_LOG_FILE_PREFIX+process+".txt"));
		String temp,res = null;
		while((temp = br.readLine()) != null) {
			res = temp;
		}
		br.close();
		return res;
	}
}
