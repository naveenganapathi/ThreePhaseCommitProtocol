package com.tpc;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.tpc.util.Config;
import com.tpc.util.LogRecord;
import com.tpc.util.Message;
import com.tpc.util.NetController;
import com.tpc.util.ThreePhaseCommitUtility;
import com.tpc.util.Timer;

/***
 * 
 * @author naveen,vignesh
 * 
 */
public class ThreePhaseCommitProcess {
	public static final String STATE_REQ = "STATE_REQ";
	public static final String VOTE_REQ= "VOTE_REQ";
	public static final String COMMIT = "COMMIT";
	public static final String PRE_COMMIT = "PRE_COMMIT";
	public static final String ABORT = "ABORT";
	public static final String YES = "YES";
	public static final String NO = "NO";
	public static final String ACK = "ACK";
	public static final String NEW_CO_OD = "NEW_CO_OD";
	public static final int TIME_OUT_IN_SECONDS = 2000; 
	private static final Logger log = Logger.getLogger("ThreePhaseCommitProcess");
	private static final int NUM_PARTICIPANTS = 2;
	public static int currTrans  = 0;
	private static NetController netController;			
	private static List<Integer> activeProcesses = new ArrayList<Integer>();
	private static Map<String,String> playList;
	private static int processId;
	private static int coordinatorId;
	private static String participant_waiting_for;
	public static void main(String args[]) {
		if (args.length < 2) {
			throw new IllegalArgumentException("please provide intial co-ordinator id and current process id");
		}
		try {
			netController = new NetController(new Config("/home/naveen/git/ThreePhaseCommitProtocol/ThreePhaseCommit/bin/com/tpc/util/config_"+args[1]+".txt"));
			processId = Integer.parseInt(args[1]);
			coordinatorId = Integer.parseInt(args[0]);
			activeProcesses.add(0);
			activeProcesses.add(1);
			activeProcesses.add(2);
			while(true) {
				//current process is the initial co-ordinator
				if (processId == coordinatorId) {
					while (true) {
						System.out.println("sleeping for 5 seconds before starting next trans");
						Thread.sleep(5000);
						currTrans++;
						Message message = new Message(processId,currTrans,VOTE_REQ);
						List<Integer> participants = new ArrayList<Integer>(activeProcesses);
						System.out.println("starting new transaction!");
						participants.remove(processId);
						// Send STATE REQ to all participants
						for(Integer participant : participants) {
							boolean sendStatus = netController.sendMsg(participant, ThreePhaseCommitUtility.serializeMessage(message));
							if(!sendStatus) {
								log.log(Level.INFO, "send to process"+participant+" failed");
							}
						}

						List<String> reportMessages = new ArrayList<String>();
						while (reportMessages.size() != NUM_PARTICIPANTS) {
							reportMessages.addAll(netController.getReceivedMsgs());						
						}

						for (String reportMessage : reportMessages) {
							Message msg = ThreePhaseCommitUtility.deserializeMessage(reportMessage);
							if (msg.getMessage().equals(ABORT)) {							
								//write ABORT to log if not already written
								LogRecord curRecord = new LogRecord(msg.getTransactionId(), ABORT);
								if (!ThreePhaseCommitUtility.fetchMessageFromDTLog(processId).equals(curRecord)) {
									ThreePhaseCommitUtility.writeMessageToDTLog(processId, new LogRecord(msg.getTransactionId(),ABORT));
								}
								Message abortMessage = new Message(processId,currTrans,ABORT);
								for (Integer parts : participants) {								
									netController.sendMsg(parts, ThreePhaseCommitUtility.serializeMessage(abortMessage));
								}
							}
						}

						//TODO assuming that all participants sent yes
						Message preCommitMessage = new Message(processId,currTrans,PRE_COMMIT);
						for(Integer parts : participants) {
							netController.sendMsg(parts, ThreePhaseCommitUtility.serializeMessage(preCommitMessage));
						}

						List<String> ackMessages = new ArrayList<String>();
						while (ackMessages.size() != NUM_PARTICIPANTS) {
							ackMessages.addAll(netController.getReceivedMsgs());						
						}

						for (String ackMessage : ackMessages) {
							Message ackMsg = ThreePhaseCommitUtility.deserializeMessage(ackMessage);
							if (!ackMsg.getMessage().equals("ACK")) {
								throw new IllegalStateException("din receive ack");
							}
						}

						//write commit to DT log
						ThreePhaseCommitUtility.writeMessageToDTLog(processId, new LogRecord(currTrans,COMMIT));


						//send COMMIT to everyone
						Message commitMessage = new Message(processId,currTrans,COMMIT);
						for(Integer parts : participants) {
							netController.sendMsg(parts, ThreePhaseCommitUtility.serializeMessage(commitMessage));
						}
					}				
				} else {  // current process is a participant.
					List<String> receivedMsgs = new ArrayList<String>();
					while(true) {
						Timer timer = new Timer();
						timer.setTimeoutInSeconds(TIME_OUT_IN_SECONDS);
						timer.start();
						while(receivedMsgs.isEmpty() && !timer.hasTimedOut()){
							receivedMsgs.addAll(netController.getReceivedMsgs());
						}
						
						// if time-out has happened
						if(timer.hasTimedOut() &&
								(participant_waiting_for.equals(COMMIT) || 
										participant_waiting_for.equals(PRE_COMMIT) || 
										participant_waiting_for.equals(ABORT))) {
							initiateElection();
							if(coordinatorId == processId) {
								// invoke co-ordinator's termination protocol
							} else {
								// invoke participant's termination protocol
							}								
						}
						
						// if there is some message received
						for(String message : receivedMsgs) {
							Message msg = ThreePhaseCommitUtility.deserializeMessage(message);
							if(msg.getProcessId() == coordinatorId && msg.getMessage().equals(VOTE_REQ)){
								Message voteYes = new Message();
								voteYes.setProcessId(processId);
								voteYes.setMessage(YES);
								netController.sendMsg(coordinatorId, ThreePhaseCommitUtility.serializeMessage(voteYes));
								participant_waiting_for=PRE_COMMIT;
							}
							else if(msg.getProcessId() == coordinatorId && msg.getMessage().equals(ABORT)){
								ThreePhaseCommitUtility.writeMessageToDTLog(processId, new LogRecord(msg.getTransactionId(), ABORT));
							}
							else if(msg.getProcessId() == coordinatorId && msg.getMessage().equals(COMMIT)){
								ThreePhaseCommitUtility.writeMessageToDTLog(processId, new LogRecord(msg.getTransactionId(),  COMMIT));
							}
							else if(msg.getProcessId() == coordinatorId && msg.getMessage().equals(PRE_COMMIT)){
								Message ack = new Message();
								ack.setProcessId(processId);
								ack.setMessage(ACK);
								netController.sendMsg(coordinatorId, ThreePhaseCommitUtility.serializeMessage(ack));
								participant_waiting_for=COMMIT;
								//ThreePhaseCommitUtility.writeMessageToDTLog(processId, COMMIT);
							}else if(msg.getMessage().equals(NEW_CO_OD)){
								coordinatorId = msg.getProcessId();
							}
						}
						receivedMsgs.clear();
						// if the participant is the new co-ordinator, then break and go to the mail while loop
						if(coordinatorId == processId)
							break;
					}
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public static void initiateElection(){
		int oldCoordinatorId = coordinatorId;
		int newCoordinatorId = (coordinatorId + 1) % (NUM_PARTICIPANTS + 1);
		if(processId == newCoordinatorId) {
			coordinatorId = newCoordinatorId;
			Message newCood = new Message();
			newCood.setProcessId(processId);
			newCood.setMessage(NEW_CO_OD);
			List<Integer> participants = new ArrayList<Integer>(activeProcesses);
			for (Integer parts : participants) {
				if(parts == oldCoordinatorId) {
					participants.remove(parts);
					continue;
				}
				if(parts == newCoordinatorId) continue;
				netController.sendMsg(parts, ThreePhaseCommitUtility.serializeMessage(newCood));
			}		
		}
	}
	
	public static void participantTermination() {
		List<String> receivedMsgs = new ArrayList<String>();
		Timer timer = new Timer();
		timer.setTimeoutInSeconds(TIME_OUT_IN_SECONDS);
		timer.start();
		while(receivedMsgs.isEmpty() && !timer.hasTimedOut()){
			receivedMsgs.addAll(netController.getReceivedMsgs());
		}
		
		// if time-out has happened
		if(timer.hasTimedOut()) {
			
		}
	}
}
