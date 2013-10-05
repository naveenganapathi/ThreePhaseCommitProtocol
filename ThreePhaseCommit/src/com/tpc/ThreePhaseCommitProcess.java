package com.tpc;

import java.io.IOException;
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
	public static final String VOTE_REQ = "VOTE_REQ";
	public static final String COMMIT = "COMMIT";
	public static final String PRE_COMMIT = "PRE_COMMIT";
	public static final String ABORT = "ABORT";
	public static final String YES = "YES";
	public static final String NO = "NO";
	public static final String ABORTED = "ABORTED";
	public static final String ACK = "ACK";
	public static final String COMMITTED = "COMMITTED";
	public static final String COMMITTABLE = "COMMITTABLE";
	public static final String UNCERTAIN = "UNCERTAIN";
	public static final String NEW_CO_OD = "NEW_CO_OD";
	public static final int TIME_OUT_IN_SECONDS = 2000;
	private static final Logger log = Logger.getLogger("ThreePhaseCommitProcess");
	private static final int NUM_PARTICIPANTS = 2;
	public static int currTrans = 0;
	private static NetController netController;
	private static List<Integer> activeProcesses = new ArrayList<Integer>();
	private static Map<String, String> playList;
	private static int processId;
	private static int coordinatorId;
	private static String participant_waiting_for;

	public static void main(String args[]) {
		if (args.length < 2) {
			throw new IllegalArgumentException("please provide intial co-ordinator id and current process id");
		}
		try {
			netController = new NetController(new Config("C:\\Users\\vignesh\\git\\ThreePhaseCommitProtocol\\ThreePhaseCommit\\bin\\com\\tpc\\util\\" + "config_" + args[1] + ".txt"));
			processId = Integer.parseInt(args[1]);
			coordinatorId = Integer.parseInt(args[0]);
			String myVote = YES;
			activeProcesses.add(0);
			activeProcesses.add(1);
			activeProcesses.add(2);

			// current process is the initial co-ordinator
			while (true) {
				if (processId == coordinatorId) {
					while (true) {
						System.out.println("sleeping for 5 seconds before starting next trans");
						Thread.sleep(5000);
						currTrans++;
						Message message = new Message(processId, currTrans, VOTE_REQ);
						ThreePhaseCommitUtility.writeMessageToDTLog(processId, new LogRecord(currTrans, "start-3PC"));
						List<Integer> participants = new ArrayList<Integer>(activeProcesses);
						System.out.println("starting new transaction!");
						participants.remove(processId);					
						// Send VOTE REQ to all participants
						System.out.println("sending vote requests!");
						for (Integer participant : participants) {
							boolean sendStatus = netController.sendMsg(participant, ThreePhaseCommitUtility.serializeMessage(message));
							if (!sendStatus) {
								log.log(Level.INFO, "send to process" + participant + " failed");
							}
						}

						// Get VOTE responses from participants.
						Timer timer = new Timer();
						timer.start();
						List<String> reportMessages = new ArrayList<String>();
						while (reportMessages.size() != NUM_PARTICIPANTS || timer.hasTimedOut()) {
							reportMessages.addAll(netController.getReceivedMsgs());
						}

						// some process timed out. aborting
						if (reportMessages.size() != NUM_PARTICIPANTS) {
							System.out.println("a few processes timedout. Hence aborting the transaction");
							ThreePhaseCommitUtility.writeMessageToDTLog(processId, new LogRecord(currTrans, ABORT));
							for (String rawMessage : reportMessages) {
								Message msg = ThreePhaseCommitUtility.deserializeMessage(rawMessage);
								if (msg.getMessage().equals(YES)) {
									netController.sendMsg(msg.getProcessId(), ThreePhaseCommitUtility.serializeMessage(new Message(processId, currTrans, ABORT)));
								}
							}
						}

						// abort if some process sends no.
						boolean abortFlag = false;
						List<Message> forMessages = new ArrayList<Message>();
						List<Message> againstMessages = new ArrayList<Message>();
						for (String reportMessage : reportMessages) {
							Message msg = ThreePhaseCommitUtility.deserializeMessage(reportMessage);
							if (msg.getMessage().equals(NO)) {
								System.out.println("process "+msg.getProcessId()+"has decided abort");
								abortFlag = true;
							} else if (msg.getMessage().equals(YES)) {
								System.out.println("process "+msg.getProcessId()+"is ok with commiting");
								forMessages.add(msg);
							}
						}

						// some processes have decided to abort. ABORT
						if (abortFlag || !myVote.equals(YES)) {
							System.out.println("coordinator has decided to abort");
							ThreePhaseCommitUtility.writeMessageToDTLog(processId, new LogRecord(currTrans, ABORT));
							for (Message msg : forMessages) {
								Message abortMessage = new Message(processId, currTrans, ABORT);
								netController.sendMsg(msg.getProcessId(), ThreePhaseCommitUtility.serializeMessage(abortMessage));
							}
						}

						// all processes have sent yes! COMMIT away
						if (!abortFlag && myVote.equals(YES)) {
							System.out.println("everyone has sent yes. sending PRE COMMIT");
							Message preCommitMessage = new Message(processId, currTrans, PRE_COMMIT);
							for (Integer parts : participants) {
								netController.sendMsg(parts, ThreePhaseCommitUtility.serializeMessage(preCommitMessage));
							}

							List<String> ackMessages = new ArrayList<String>();
							Timer ackTimer = new Timer();
							while (ackMessages.size() != NUM_PARTICIPANTS || ackTimer.hasTimedOut()) {
								ackMessages.addAll(netController.getReceivedMsgs());
							}

							// just a simple check. Ideally should never happen.
							for (String ackMessage : ackMessages) {
								Message ackMsg = ThreePhaseCommitUtility.deserializeMessage(ackMessage);
								if (!ackMsg.getMessage().equals("ACK")) {
									throw new IllegalStateException("din receive ack");
								}
							}

							// write commit to DT log
							ThreePhaseCommitUtility.writeMessageToDTLog(processId, new LogRecord(currTrans, COMMIT));

							// send COMMIT to everyone
							System.out.println("sending commit to everyone!");
							for (Integer parts : participants) {
								netController.sendMsg(parts, ThreePhaseCommitUtility.serializeMessage(new Message(processId, currTrans, COMMIT)));
							}
						}
					}
				} else { // current process is a participant.
					List<String> receivedMsgs = new ArrayList<String>();
					while (true) {
						Timer timer = new Timer();
						timer.setTimeoutInSeconds(TIME_OUT_IN_SECONDS);
						timer.start();
						while (receivedMsgs.isEmpty() && !timer.hasTimedOut()) {
							receivedMsgs.addAll(netController.getReceivedMsgs());
						}

						// if time-out has happened
						if (timer.hasTimedOut() && (participant_waiting_for.equals(COMMIT) || participant_waiting_for.equals(PRE_COMMIT) || participant_waiting_for.equals(ABORT))) {
							int oldCoordinatorId = coordinatorId;
							initiateElection();
							List<Integer> currentActiveParticipants = new ArrayList<Integer>(activeProcesses);							
							if (coordinatorId == processId) {
								// invoke co-ordinator's termination protocol
								currentActiveParticipants.remove(oldCoordinatorId);
								currentActiveParticipants.remove(coordinatorId);
								coordinatorTerminationProtocol(currentActiveParticipants);
								break;
							} 
						}

						// if there is some message received
						for (String message : receivedMsgs) {
							Message msg = ThreePhaseCommitUtility.deserializeMessage(message);
							if (msg.getProcessId() == coordinatorId && msg.getMessage().equals(VOTE_REQ)) {
								Message voteYes = new Message();
								voteYes.setProcessId(processId);
								voteYes.setMessage(YES);
								netController.sendMsg(coordinatorId, ThreePhaseCommitUtility.serializeMessage(voteYes));
								participant_waiting_for = PRE_COMMIT;
							} else if (msg.getProcessId() == coordinatorId && msg.getMessage().equals(ABORT)) {
								ThreePhaseCommitUtility.writeMessageToDTLog(processId, new LogRecord(msg.getTransactionId(), ABORT));
							} else if (msg.getProcessId() == coordinatorId && msg.getMessage().equals(COMMIT)) {
								ThreePhaseCommitUtility.writeMessageToDTLog(processId, new LogRecord(msg.getTransactionId(), COMMIT));
							} else if (msg.getProcessId() == coordinatorId && msg.getMessage().equals(PRE_COMMIT)) {
								Message ack = new Message();
								ack.setProcessId(processId);
								ack.setMessage(ACK);
								netController.sendMsg(coordinatorId, ThreePhaseCommitUtility.serializeMessage(ack));
								participant_waiting_for=COMMIT;
								//ThreePhaseCommitUtility.writeMessageToDTLog(processId, COMMIT);
							}else if(msg.getMessage().equals(NEW_CO_OD)){
								coordinatorId = msg.getProcessId();
								participantTermination();
							}
						}
						receivedMsgs.clear();
					}
				}
			}

		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public static void initiateElection() {
		int oldCoordinatorId = coordinatorId;
		int newCoordinatorId = (coordinatorId + 1) % (NUM_PARTICIPANTS + 1);
		if (processId == newCoordinatorId) {
			coordinatorId = newCoordinatorId;
			Message newCood = new Message();
			newCood.setProcessId(processId);
			newCood.setMessage(NEW_CO_OD);
			List<Integer> participants = new ArrayList<Integer>(activeProcesses);
			for (Integer parts : participants) {
				if (parts == oldCoordinatorId) {
					participants.remove(parts);
					continue;
				}
				if (parts == newCoordinatorId)
					continue;
				netController.sendMsg(parts, ThreePhaseCommitUtility.serializeMessage(newCood));
			}
		}
	}

	public static void participantTermination() {
		List<String> receivedMsgs = new ArrayList<String>();
		Timer timer = new Timer();
		timer.setTimeoutInSeconds(TIME_OUT_IN_SECONDS);
		timer.start();
		while(true) {
			while (receivedMsgs.isEmpty() && !timer.hasTimedOut()) {
				receivedMsgs.addAll(netController.getReceivedMsgs());
			}

			// if time-out has happened
			if (timer.hasTimedOut()) {
				initiateElection();
				if (coordinatorId == processId) {
					// invoke co-ordinator's termination protocol
					// TODO execute co-ordinator termination algo
					break;
				} 
			}
		}
	}
	
	public static void coordinatorTerminationProtocol(List<Integer> participants) throws IOException {
		int totalParticipants = participants.size();
		
		// send STATE_REQ to all processes.
		System.out.println("Starting termination protocol. Co-ordinator is"+processId);
		System.out.println("sending state request");
		for(Integer participant : participants) {
			netController.sendMsg(participant, ThreePhaseCommitUtility.serializeMessage(new Message(coordinatorId, currTrans, STATE_REQ)));
		}
		
		//Wait for all responses or until timeout
		Timer stateReqTimer = new Timer();
		List<String> stateReqresponses = new ArrayList<String>();
		while( stateReqresponses.size() != totalParticipants  && !stateReqTimer.hasTimedOut() ) {
			stateReqresponses.addAll(netController.getReceivedMsgs());
		}
		
		// get co-ordinators state.
		LogRecord recentLogRecord = ThreePhaseCommitUtility.fetchMessageFromDTLog(processId);
		String state = recentLogRecord.getMessage();
		boolean abortFlag, uncertainFlag, commitFlag;
		abortFlag = uncertainFlag = commitFlag = false;
		abortFlag = state.equals(ABORT);
		commitFlag = state.equals(COMMIT);
		uncertainFlag = abortFlag == false && commitFlag == false;
		
		System.out.println("parsing participant states...");
		// get participant state
		if (!abortFlag && !commitFlag) {
			for (String resp : stateReqresponses) {
				Message msg = ThreePhaseCommitUtility.deserializeMessage(resp);
				if (msg.getMessage().equals(ABORT)) {
					abortFlag = true;
					break;
				} else if (msg.getMessage().equals(COMMIT)) {
					commitFlag= true;
					break;
				}
			}
		}		
		
		
		if (abortFlag || uncertainFlag) {
			System.out.println("deciding to abort!");
			if(!state.equals(ABORT)) {
				ThreePhaseCommitUtility.writeMessageToDTLog(processId, new LogRecord(currTrans, ABORT));
			}
			for (Integer participant : participants) {
				netController.sendMsg(participant, ThreePhaseCommitUtility.serializeMessage(new Message(processId, currTrans, ABORT)));
			}
		} else if (commitFlag) {
			System.out.println("deciding to commit!");
			if(!state.equals(COMMIT)) {
				ThreePhaseCommitUtility.writeMessageToDTLog(processId, new LogRecord(currTrans, COMMIT));
			}
			for (Integer participant : participants) {
				netController.sendMsg(participant, ThreePhaseCommitUtility.serializeMessage(new Message(processId, currTrans, COMMIT)));
			}
		}
		
		System.out.println("completing termination protocol");
	}
}
