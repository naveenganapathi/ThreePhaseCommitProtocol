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
	public static final String STATE_RESP = "STATE_RESP";
	public static final int TIME_OUT_IN_SECONDS = 10;
	private static final Logger log = Logger.getLogger("ThreePhaseCommitProcess");
	private static final int NUM_PARTICIPANTS = 2;
	public static int currTrans = 0;
	private static NetController netController;
	private static List<Integer> activeProcesses = new ArrayList<Integer>();
	private static Map<String, String> playList;
	private static int processId;
	private static int coordinatorId;
	private static String participant_waiting_for="";

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

			int cnt = 0;
			// current process is the initial co-ordinator
			while (true) {
				
				if (processId == coordinatorId) {
					while (cnt < 2) {
						cnt++;
						Thread.sleep(5000);
						System.out.println("current cnt:"+cnt);
						currTrans++;
						Message message = new Message(processId, currTrans, VOTE_REQ);
						ThreePhaseCommitUtility.writeMessageToDTLog(processId, new LogRecord(currTrans, "start-3PC"));
						List<Integer> participants = new ArrayList<Integer>(activeProcesses);
						System.out.println("starting new transaction!");
						participants.remove((Object)new Integer(processId));					
						// Send VOTE REQ to all participants
						System.out.println("sending vote requests!");
						for (Integer participant : participants) {
							boolean sendStatus = netController.sendMsg(participant, ThreePhaseCommitUtility.serializeMessage(message));
							if (!sendStatus) {
								log.log(Level.INFO, "send to process" + participant + " failed");
							}
						}

						// Get VOTE responses from participants.
						Timer timer = new Timer(TIME_OUT_IN_SECONDS);						
						timer.start();
						List<String> reportMessages = new ArrayList<String>();
						while (reportMessages.size() < activeProcesses.size() - 1 && !timer.hasTimedOut()) {
							reportMessages.addAll(netController.getReceivedMsgs());
						}

						// some process timed out. aborting
						if (timer.hasTimedOut()) {
							System.out.println("a few processes timedout. Hence aborting the transaction");
							System.out.println("received messages:"+reportMessages);
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
								if (true) {
									System.out.println("crash it now!!!");
									Thread.sleep(5000);
								}
							}

							List<String> ackMessages = new ArrayList<String>();
							Timer ackTimer = new Timer(TIME_OUT_IN_SECONDS);
							ackTimer.start();
							while (ackMessages.size() < activeProcesses.size() - 1 && !ackTimer.hasTimedOut()) {
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
						Timer timer = new Timer(TIME_OUT_IN_SECONDS);
						timer.start();
						System.out.println("Process "+processId+" is waiting for some message");
						while (receivedMsgs.isEmpty() && !timer.hasTimedOut()) {
							receivedMsgs.addAll(netController.getReceivedMsgs());
						}

						// if time-out has happened
						if (timer.hasTimedOut() && (participant_waiting_for.equals(COMMIT) || participant_waiting_for.equals(PRE_COMMIT) || participant_waiting_for.equals(ABORT))) {
							System.out.println("Process "+processId+" has encountered a time out and the participant is waiting for "+participant_waiting_for);
							initiateElection();
							System.out.println("active processes"+activeProcesses);
							List<Integer> currentActiveParticipants = new ArrayList<Integer>(activeProcesses);							
							if (coordinatorId == processId) {
								// invoke co-ordinator's termination protocol
								currentActiveParticipants.remove((Object)new Integer(processId));
								coordinatorTerminationProtocol(currentActiveParticipants);
								break;
							} 
						}

						// if there is some message received
						System.out.println("Received messages :"+receivedMsgs);
						for (String message : receivedMsgs) {
							Message msg = ThreePhaseCommitUtility.deserializeMessage(message);
							if (msg.getProcessId() == coordinatorId && msg.getMessage().equals(VOTE_REQ)) {
								System.out.println("Process "+processId+" received a vote request");
								currTrans = msg.getTransactionId();
								Message voteYes = new Message();
								voteYes.setProcessId(processId);
								voteYes.setMessage(YES);
								ThreePhaseCommitUtility.writeMessageToDTLog(processId, new LogRecord(msg.getTransactionId(), YES));
								netController.sendMsg(coordinatorId, ThreePhaseCommitUtility.serializeMessage(voteYes));
								System.out.println("Process "+processId+" waiting for a pre_commit");
								participant_waiting_for = PRE_COMMIT;
							} else if (msg.getProcessId() == coordinatorId && msg.getMessage().equals(ABORT)) {
								ThreePhaseCommitUtility.writeMessageToDTLog(processId, new LogRecord(msg.getTransactionId(), ABORT));
								System.out.println("Process "+processId+" received an abort");
								participant_waiting_for = "";
							} else if (msg.getProcessId() == coordinatorId && msg.getMessage().equals(COMMIT)) {
								ThreePhaseCommitUtility.writeMessageToDTLog(processId, new LogRecord(msg.getTransactionId(), COMMIT));
								System.out.println("Process "+processId+" received a commit");
								participant_waiting_for = "";
							} else if (msg.getProcessId() == coordinatorId && msg.getMessage().equals(PRE_COMMIT)) {
								System.out.println("Process "+processId+" received a pre-commit");
								Message ack = new Message();
								ack.setProcessId(processId);
								ack.setMessage(ACK);
								netController.sendMsg(coordinatorId, ThreePhaseCommitUtility.serializeMessage(ack));
								participant_waiting_for=COMMIT;
								System.out.println("Process "+processId+" voted with an ACK and waiting for "+participant_waiting_for);
								//ThreePhaseCommitUtility.writeMessageToDTLog(processId, COMMIT);
							}else if(msg.getMessage().equals(NEW_CO_OD)){
								System.out.println("Process "+processId+" got a NEW_CO_OD from process "+msg.getProcessId());
								coordinatorId = msg.getProcessId();
								participantTerminationProtocol();
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
		System.out.println("Process "+processId+" is initiating election protocol");
		int oldCoordinatorId = coordinatorId;
		int newCoordinatorId = (coordinatorId + 1) % (NUM_PARTICIPANTS + 1);
		if (processId == newCoordinatorId) {
			coordinatorId = newCoordinatorId;
			System.out.println("Process "+processId+" has selected "+newCoordinatorId+" as the new co-ordinator");
			Message newCood = new Message();
			newCood.setProcessId(processId);
			newCood.setMessage(NEW_CO_OD);
			System.out.println("Process "+processId+" sends NEW_CO_OD message to other participants");
			List<Integer> participants = new ArrayList<Integer>(activeProcesses);
			for (Integer parts : participants) {
				if (parts == oldCoordinatorId) {
					activeProcesses.remove((Object)new Integer(parts));
					continue;
				}
				if (parts == newCoordinatorId)
					continue;
				netController.sendMsg(parts, ThreePhaseCommitUtility.serializeMessage(newCood));
			}
		}
		System.out.println("Process "+processId+" has ended the election protocol");
	}

	public static void participantTerminationProtocol() throws IOException {
		System.out.println("Process "+processId+" is initiating participant termination protocol");
		List<String> receivedMsgs = new ArrayList<String>();
		Timer timer = new Timer(TIME_OUT_IN_SECONDS);
		timer.start();
		boolean notCommitted = true;
		System.out.println("Process "+processId+" is waiting for STATE_REQ");
		while(notCommitted) {
			while (receivedMsgs.isEmpty() && !timer.hasTimedOut()) {
				receivedMsgs.addAll(netController.getReceivedMsgs());
			}

			// if time-out has happened
			if (timer.hasTimedOut() && (participant_waiting_for.equals(STATE_RESP))) {
				System.out.println("Process "+processId+" has timed out while waiting for response from co-ordinator");
				initiateElection();
				if (coordinatorId == processId) {
					// invoke co-ordinator's termination protocol
					List<Integer> participants = new ArrayList<Integer>(activeProcesses);
					System.out.println("active processes!"+activeProcesses);
					participants.remove((Object)new Integer(processId));
					coordinatorTerminationProtocol(participants);
				} 
			}

			for (String message : receivedMsgs) {
				Message msg = ThreePhaseCommitUtility.deserializeMessage(message);
				if (msg.getProcessId() == coordinatorId && msg.getMessage().equals(STATE_REQ)) {
					Message state = new Message();
					state.setProcessId(processId);
					LogRecord myState = ThreePhaseCommitUtility.fetchRecordForTransaction(processId, msg.getTransactionId());
					System.out.println("msg of process "+processId+" is "+msg);
					if(myState==null || myState.getMessage().equals(ABORT)) {
						state.setMessage(ABORTED);
					} else if(myState!=null && myState.getMessage().equals(COMMIT)) {
						state.setMessage(COMMITTED);
					} else if(myState!=null && myState.getMessage().equals(PRE_COMMIT)) {
						state.setMessage(COMMITTABLE);
					} else {
						state.setMessage(UNCERTAIN);
					}
					netController.sendMsg(coordinatorId, ThreePhaseCommitUtility.serializeMessage(state));
					participant_waiting_for=STATE_RESP;
					System.out.println("Process "+processId+" sends its current state "+state.getMessage());
				} else if(msg.getProcessId() == coordinatorId && msg.getMessage().equals(ABORT)){
					LogRecord myState = ThreePhaseCommitUtility.fetchRecordForTransaction(processId, msg.getTransactionId());
					if(!myState.getMessage().equals(ABORT))
						ThreePhaseCommitUtility.writeMessageToDTLog(processId, new LogRecord(msg.getTransactionId(), ABORT));	
					System.out.println("Process "+processId+" received abort during the termination protocol");
				} else if(msg.getProcessId() == coordinatorId && msg.getMessage().equals(COMMIT)){
					LogRecord myState = ThreePhaseCommitUtility.fetchRecordForTransaction(processId, msg.getTransactionId());
					if(!myState.getMessage().equals(COMMIT))
						ThreePhaseCommitUtility.writeMessageToDTLog(processId, new LogRecord(msg.getTransactionId(), COMMIT));
					System.out.println("Process "+processId+" received commit during the termination protocol");
					notCommitted = false;
				} else {
					Message ack = new Message();
					ack.setProcessId(processId);
					ack.setMessage(ACK);
					netController.sendMsg(coordinatorId, ThreePhaseCommitUtility.serializeMessage(ack));
					System.out.println("Process "+processId+" received pre_commit during the termination protocol and has sent ACK");
				}
			}
			receivedMsgs.clear();
		}
		System.out.println("Process "+processId+" has completed participant termination protocol");
	}

	public static void coordinatorTerminationProtocol(List<Integer> participants) throws IOException {
		int totalParticipants = participants.size();

		// send STATE_REQ to all processes.
		System.out.println("Starting termination protocol. Co-ordinator is"+processId);
		System.out.println("sending state request to"+participants);
		for(Integer participant : participants) {
			System.out.println("sending state req to :"+participant);
			netController.sendMsg(participant, ThreePhaseCommitUtility.serializeMessage(new Message(coordinatorId, currTrans, STATE_REQ)));
		}

		//Wait for all responses or until timeout
		Timer stateReqTimer = new Timer(TIME_OUT_IN_SECONDS);
		stateReqTimer.start();
		List<String> stateReqresponses = new ArrayList<String>();
		while( stateReqresponses.size() < totalParticipants  && !stateReqTimer.hasTimedOut() ) {
			stateReqresponses.addAll(netController.getReceivedMsgs());
		}

		// get co-ordinators state.
		LogRecord recentLogRecord = ThreePhaseCommitUtility.fetchMessageFromDTLog(processId);
		String state = recentLogRecord.getMessage();
		boolean abortFlag, uncertainFlag, commitFlag, commitableFlag;
		abortFlag = uncertainFlag = commitFlag = false;
		abortFlag = state.equals(ABORT);
		commitFlag = state.equals(COMMIT);
		commitableFlag = false;
		uncertainFlag = abortFlag == false && commitFlag == false;

		System.out.println("parsing participant states..."+stateReqresponses);
		// get participant state
		List<Integer> uncertainProcesses = new ArrayList<Integer>();
		if (!abortFlag && !commitFlag) {
			for (String resp : stateReqresponses) {
				Message msg = ThreePhaseCommitUtility.deserializeMessage(resp);
				if (msg.getMessage().equals(ABORTED)) {
					abortFlag = true;
					break;
				} else if (msg.getMessage().equals(COMMITTED)) {
					commitFlag= true;
					break;
				} else if (msg.getMessage().equals(COMMITTABLE)) {
					commitableFlag = true;
				} else if (msg.getMessage().equals(UNCERTAIN)) {
					uncertainFlag = true;
					uncertainProcesses.add(msg.getProcessId());
				}
			}
		}		
	
		if (abortFlag) {
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
		} else if (commitableFlag) {
			System.out.println("commitable, sending pre commit to uncertain processes.");
			for (Integer participant : uncertainProcesses) {
				netController.sendMsg(participant, ThreePhaseCommitUtility.serializeMessage(new Message(processId, currTrans, PRE_COMMIT)));
			}
			
			Timer ackTimer = new Timer(TIME_OUT_IN_SECONDS);
			ackTimer.start();
			List<String> ackMsgs = new ArrayList<String>();
			
			while( ackMsgs.size() != uncertainProcesses.size() || !ackTimer.hasTimedOut() ) {
				ackMsgs.addAll(netController.getReceivedMsgs());
			}
			
			System.out.println("sending commit to all processes");
			ThreePhaseCommitUtility.writeMessageToDTLog(processId, new LogRecord(currTrans, COMMIT));
			for (Integer participant : participants) {
				netController.sendMsg(participant, ThreePhaseCommitUtility.serializeMessage(new Message(processId, currTrans, COMMIT)));
			}
		} else {
			System.out.println("all of em are uncertain. aborting transaction");
			if(!state.equals(ABORT)) {
				ThreePhaseCommitUtility.writeMessageToDTLog(processId, new LogRecord(currTrans, ABORT));
			}
			System.out.println("sending abort to"+participants);
			for (Integer participant : participants) {
				netController.sendMsg(participant, ThreePhaseCommitUtility.serializeMessage(new Message(processId, currTrans, ABORT)));
			}
		}

		System.out.println("completing termination protocol");
	}
}
