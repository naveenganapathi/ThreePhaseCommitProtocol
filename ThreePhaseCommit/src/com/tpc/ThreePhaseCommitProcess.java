package com.tpc;

import java.util.List;
import java.util.Map;

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

	public static void main(String args[]) {
		if (args.length < 2) {
			throw new IllegalArgumentException("please provide intial co-ordinator id and current process id");
		}
		try {
			NetController netController = new NetController(new Config("config_"+args[1]+".txt"));
			List<Integer> activeProcesses;
			Map<String,String> playList;
			int processId = Integer.parseInt(args[0]);
			int coordinatorId = Integer.parseInt(args[1]);
			
			//current process is the initial co-ordinator
			if (processId == coordinatorId) {
				
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
