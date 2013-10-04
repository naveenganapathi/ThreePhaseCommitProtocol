package com.tpc;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.tpc.util.Config;
import com.tpc.util.Message;
import com.tpc.util.NetController;
import com.tpc.util.ThreePhaseCommitUtility;

/***
 * 
 * @author naveen,vignesh
 * 
 */
public class ThreePhaseCommitProcess {
	public static final String STATE_REQ = "STATE_REQ";
	public static final String ABORT = "ABORT";
	private static final Logger log = Logger.getLogger("ThreePhaseCommitProcess");
	private static final int NUM_PARTICIPANTS = 2;
	public static void main(String args[]) {
		
		
		if (args.length < 2) {
			throw new IllegalArgumentException("please provide intial co-ordinator id and current process id");
		}
		try {
			NetController netController = new NetController(new Config("C:\\Users\\vignesh\\git\\ThreePhaseCommitProtocol\\ThreePhaseCommit\\bin\\com\\tpc\\util\\"+"config_"+args[1]+".txt"));			
			List<Integer> activeProcesses = new ArrayList<Integer>();
			Map<String,String> playList;
			int processId = Integer.parseInt(args[0]);
			int coordinatorId = Integer.parseInt(args[1]);
			
			activeProcesses.add(0);
			activeProcesses.add(1);
			activeProcesses.add(2);
			//current process is the initial co-ordinator
			if (processId == coordinatorId) {
				while (true) {
					Message message = new Message();
					message.setProcessId(processId);
					message.setMessage(STATE_REQ);
					List<Integer> participants = new ArrayList<Integer>(activeProcesses);
					
					participants.remove(processId);
					// Send STATE REQ to all participants
					for(Integer participant : participants) {
						boolean sendStatus = netController.sendMsg(participant, ThreePhaseCommitUtility.serializeMessage(message));
						if(!sendStatus) {
							log.log(Level.INFO, "send to process"+participant+" failed");
						}
					}
					
					List<String> reportMessages = new ArrayList<String>();
					while (reportMessages.size() == NUM_PARTICIPANTS) {
						reportMessages.addAll(netController.getReceivedMsgs());						
					}
					
					for (String reportMessage : reportMessages) {
						Message msg = ThreePhaseCommitUtility.deserializeMessage(reportMessage);
						if (msg.getMessage().equals(ABORT)) {							
							//write ABORT to log if not already written
							if (!ThreePhaseCommitUtility.fetchMessageFromDTLog(processId).equals(ABORT)) {
								ThreePhaseCommitUtility.writeMessageToDTLog(processId, ABORT);
							}
							Message abortMessage = new Message();
							abortMessage.setMessage(ABORT);
							abortMessage.setProcessId(processId);
							for (Integer parts : participants) {								
								netController.sendMsg(parts, ThreePhaseCommitUtility.serializeMessage(abortMessage));
							}
						}
					}
					
					//TODO assuming that all participants sent yes
					Message preCommitMessage = new Message();
					preCommitMessage.setMessage("PRE_COMMIT");
					preCommitMessage.setProcessId(processId);
					for(Integer parts : participants) {
						netController.sendMsg(parts, ThreePhaseCommitUtility.serializeMessage(preCommitMessage));
					}
					
					List<String> ackMessages = new ArrayList<String>();
					while (ackMessages.size() == NUM_PARTICIPANTS) {
						ackMessages.addAll(netController.getReceivedMsgs());						
					}
					
					for (String ackMessage : ackMessages) {
						Message ackMsg = ThreePhaseCommitUtility.deserializeMessage(ackMessage);
						if (!ackMsg.getMessage().equals("ACK")) {
							throw new IllegalStateException("din receive ack");
						}
					}
					
					//write commit to DT log
					ThreePhaseCommitUtility.writeMessageToDTLog(processId, "COMMIT");
					
					
					//send COMMIT to everyone
					Message commitMessage = new Message();
					commitMessage.setMessage("COMMIT");
					commitMessage.setProcessId(processId);
					for(Integer parts : participants) {
						netController.sendMsg(parts, ThreePhaseCommitUtility.serializeMessage(commitMessage));
					}
				}				
			} else {  // current process is a participant.
				List<String> receivedMsgs;
				while((receivedMsgs = netController.getReceivedMsgs()).isEmpty());
				for(String message : receivedMsgs) {
					Message msg = ThreePhaseCommitUtility.deserializeMessage(message);
					if(msg.getProcessId() == coordinatorId && msg.getMessage().equals("STATE_REQ")){
						Message voteYes = new Message();
						voteYes.setProcessId(processId);
						voteYes.setMessage("YES");
						netController.sendMsg(processId, ThreePhaseCommitUtility.serializeMessage(voteYes));						
					}
				}
				while((receivedMsgs = netController.getReceivedMsgs()).isEmpty());
				for(String message : receivedMsgs) {
					Message msg = ThreePhaseCommitUtility.deserializeMessage(message);
					if(msg.getProcessId() == coordinatorId && msg.getMessage().equals("ABORT")){
						ThreePhaseCommitUtility.writeMessageToDTLog(processId, "ABORT");
					}
					if(msg.getProcessId() == coordinatorId && msg.getMessage().equals("COMMIT")){
						ThreePhaseCommitUtility.writeMessageToDTLog(processId, "COMMIT");
					}
					if(msg.getProcessId() == coordinatorId && msg.getMessage().equals("PRE_COMMIT")){
						Message ack = new Message();
						ack.setProcessId(processId);
						ack.setMessage("ACK");
						netController.sendMsg(processId, ThreePhaseCommitUtility.serializeMessage(ack));
						ThreePhaseCommitUtility.writeMessageToDTLog(processId, "COMMIT");
					}
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
