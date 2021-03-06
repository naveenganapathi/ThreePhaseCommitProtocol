package com.tpc;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.tpc.util.Config;
import com.tpc.util.FaultModel;
import com.tpc.util.LogRecord;
import com.tpc.util.Message;
import com.tpc.util.NetController;
import com.tpc.util.PlaylistCommand;
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
	public static final String START_3PC="START-3PC";
	public static final String STATE_RESP = "STATE_RESP";
	public static final String GET_DECISION = "GET_DECISION";
//	public static final int TIME_OUT_IN_SECONDS = 10;
	public static int COORDINATOR_TIME_OUT_IN_SECONDS = 10;
	public static int PARTICIPANT_TIME_OUT_IN_SECONDS = 15;
	public static int GET_DECISION_TIME_OUT_IN_SECONDS = 15;
	private static final Logger log = Logger.getLogger("ThreePhaseCommitProcess");
	private static final int NUM_PARTICIPANTS = 2;
	public static int currTrans = 0;
	public static PlaylistCommand currentCommand;
	private static NetController netController;
	private static Set<Integer> activeProcesses = new HashSet<Integer>();
	private static Map<String, String> playList = new HashMap<String, String>();
	private static int processId;
	private static int coordinatorId;
	private static String participant_waiting_for="";
	private static String my_current_state="";
	private static boolean canParticipate = false;
	private static boolean hasElectionHappened = false;
	private static int previousCoordinatorId;
	private static int delay;

	public static void main(String args[]) {
		if (args.length < 3) {
			throw new IllegalArgumentException("please provide intial co-ordinator id and current process id");
		}
		try {
			String t=null;			
			if(args.length == 4)
				t=args[3];
			delay = Integer.parseInt(args[2]);
			COORDINATOR_TIME_OUT_IN_SECONDS+=delay*NUM_PARTICIPANTS;
			PARTICIPANT_TIME_OUT_IN_SECONDS+=delay*NUM_PARTICIPANTS;
			GET_DECISION_TIME_OUT_IN_SECONDS+=delay*NUM_PARTICIPANTS;
			netController = new NetController(new Config("bin/com/tpc/util/" + "config_" + args[1] + ".txt"), new FaultModel(t), delay* 1000);
			processId = Integer.parseInt(args[1]);
			
			coordinatorId = -1;
			String myVote = YES;
			activeProcesses = ThreePhaseCommitUtility.getActiveProcess(processId);
			System.out.println("active processes list obtained from log is"+activeProcesses);
			playList = ThreePhaseCommitUtility.fetchLocalState(processId);
			
			// read the recent log record for any change of state
			LogRecord recentLog = ThreePhaseCommitUtility.fetchMessageFromDTLog(processId);
			if(recentLog!=null) {
				System.out.println("Process "+processId+" has the recentLog of "+recentLog);
				if(recentLog.getMessage().equals(YES) || recentLog.getMessage().equals(START_3PC)) {
					boolean gotDecision = false;
					while(!gotDecision) {
						List<Integer> participants = new ArrayList<Integer>(activeProcesses);
						participants.remove((Object)new Integer(processId));
						Message getDecision = new Message();
						getDecision.setProcessId(processId);
						getDecision.setMessage(GET_DECISION);
						getDecision.setTransactionId(recentLog.getTransactionId());
						for(Integer partcipant : participants) {
							netController.sendMsg(partcipant, ThreePhaseCommitUtility.serializeMessage(getDecision));
						}
						System.out.println("Process "+processId+" has sent a GET_DECISION message to "+activeProcesses);
						List<String> receivedMsgs = new ArrayList<String>();
						Timer getDecisionTimer = new Timer(GET_DECISION_TIME_OUT_IN_SECONDS);
						getDecisionTimer.start();
						while (receivedMsgs.isEmpty() && !getDecisionTimer.hasTimedOut()) {
							receivedMsgs.addAll(netController.getReceivedMsgs());
						}
						System.out.println("Received messages while waiting for decision:"+receivedMsgs);
						for (String message : receivedMsgs) {
							Message msg = ThreePhaseCommitUtility.deserializeMessage(message);
							if(msg.getTransactionId() == recentLog.getTransactionId() && (msg.getMessage().equals(COMMIT)
									|| msg.getMessage().equals(ABORT))) {
								ThreePhaseCommitUtility.writeMessageToDTLog(processId, new LogRecord(msg.getTransactionId(), msg.getMessage(), msg.getPlaylistCommand()));
								gotDecision = true;
								if(msg.getMessage().equals(COMMIT)) {
									executeCommand(msg.getPlaylistCommand());
								}
							}
						}
					}
				} else {
					canParticipate=true;
				}
			} else {
				coordinatorId = Integer.parseInt(args[0]);
				canParticipate=true;
			}
			
			int cnt = 0;
			// current process is the initial co-ordinator
			while (true) {
				if (processId == coordinatorId) {
					
					while (ThreePhaseCommitUtility.getPlaylistCommand() != null && ThreePhaseCommitUtility.getPlaylistCommand().getAction() != null) {
						currentCommand = ThreePhaseCommitUtility.getPlaylistCommand();
						cnt++;
						Thread.sleep(5000);
						System.out.println("current cnt:"+cnt);
						currTrans++;
						List<Integer> totalParticipants = new ArrayList<Integer>();
						System.out.println("reinitializing active processes");
						for (int i =0;i<= NUM_PARTICIPANTS;i++) {
							if (i != processId)
								totalParticipants.add(i);
							activeProcesses.add(i);
						}
						ThreePhaseCommitUtility.saveActiveProcess(processId, activeProcesses);
						Message message = new Message(processId, currTrans, VOTE_REQ,ThreePhaseCommitUtility.getPlaylistCommand());
						ThreePhaseCommitUtility.writeMessageToDTLog(processId, new LogRecord(currTrans, START_3PC, message.getPlaylistCommand()));
						List<Integer> participants = new ArrayList<Integer>(totalParticipants);
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
						Timer timer = new Timer(COORDINATOR_TIME_OUT_IN_SECONDS);						
						timer.start();
						List<String> reportMessages = new ArrayList<String>();
						while (reportMessages.size() < totalParticipants.size() && !timer.hasTimedOut()) {
							List<String> recMsgs = netController.getReceivedMsgs();
							handleGetDecision(recMsgs);
							reportMessages.addAll(recMsgs);
						}

						// some process timed out. aborting
						if (timer.hasTimedOut()) {
							System.out.println("a few processes timedout. Hence aborting the transaction");
							System.out.println("received messages:"+reportMessages);
							ThreePhaseCommitUtility.writeMessageToDTLog(processId, new LogRecord(currTrans, ABORT,currentCommand));
							for (String rawMessage : reportMessages) {
								Message msg = ThreePhaseCommitUtility.deserializeMessage(rawMessage);
								if (msg.getMessage().equals(YES)) {
									netController.sendMsg(msg.getProcessId(), ThreePhaseCommitUtility.serializeMessage(new Message(processId, currTrans, ABORT,currentCommand)));
								}
							}
							continue;
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
							ThreePhaseCommitUtility.writeMessageToDTLog(processId, new LogRecord(currTrans, ABORT,currentCommand));
							for (Message msg : forMessages) {
								Message abortMessage = new Message(processId, currTrans, ABORT,currentCommand);
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
							Timer ackTimer = new Timer(COORDINATOR_TIME_OUT_IN_SECONDS);
							ackTimer.start();
							while (ackMessages.size() < activeProcesses.size() - 1 && !ackTimer.hasTimedOut()) {
								List<String> recMsgs = netController.getReceivedMsgs();
								handleGetDecision(recMsgs);
								ackMessages.addAll(recMsgs);
							}

							// just a simple check. Ideally should never happen.
							for (String ackMessage : ackMessages) {
								Message ackMsg = ThreePhaseCommitUtility.deserializeMessage(ackMessage);
								if (!ackMsg.getMessage().equals("ACK")) {
									throw new IllegalStateException("din receive ack");
								}
							}

							// write commit to DT log
							ThreePhaseCommitUtility.writeMessageToDTLog(processId, new LogRecord(currTrans, COMMIT, currentCommand));
							executeCommand(currentCommand);
							// send COMMIT to everyone
							System.out.println("sending commit to everyone!");
							for (Integer parts : participants) {
								netController.sendMsg(parts, ThreePhaseCommitUtility.serializeMessage(new Message(processId, currTrans, COMMIT, currentCommand)));
							}
							ThreePhaseCommitUtility.removeTopPlaylistCommand();
						}
					}
				} else { // current process is a participant.
					List<String> receivedMsgs = new ArrayList<String>();
					while (processId != coordinatorId) {
						Timer timer = new Timer(PARTICIPANT_TIME_OUT_IN_SECONDS);
						timer.start();
						System.out.println("Process "+processId+" is waiting for some message");
						while (receivedMsgs.isEmpty() && !timer.hasTimedOut()) {
							receivedMsgs.addAll(netController.getReceivedMsgs());
						}

						// if time-out has happened
						if (timer.hasTimedOut() && ( participant_waiting_for.equals(COMMIT) || participant_waiting_for.equals(PRE_COMMIT) || participant_waiting_for.equals(ABORT))) {
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
							if (msg.getMessage().equals(VOTE_REQ)) {
								// set this as the new co-ordinator
								if(msg.getProcessId() != coordinatorId ) {
									coordinatorId = msg.getProcessId();
								}
								System.out.println("Process "+processId+" received a vote request");
								currTrans = msg.getTransactionId();
								currentCommand = msg.getPlaylistCommand();
								Message voteYes = new Message();
								voteYes.setProcessId(processId);
								voteYes.setMessage(YES);
								ThreePhaseCommitUtility.writeMessageToDTLog(processId, new LogRecord(msg.getTransactionId(), YES));
								netController.sendMsg(coordinatorId, ThreePhaseCommitUtility.serializeMessage(voteYes));
								System.out.println("Process "+processId+" waiting for a pre_commit");
								participant_waiting_for = PRE_COMMIT;
								canParticipate = true;
								my_current_state=YES;
							} else if (canParticipate && msg.getProcessId() == coordinatorId && msg.getMessage().equals(ABORT) && currTrans == msg.getTransactionId()) {
								ThreePhaseCommitUtility.writeMessageToDTLog(processId, new LogRecord(msg.getTransactionId(), ABORT, msg.getPlaylistCommand()));
								System.out.println("Process "+processId+" received an abort");
								participant_waiting_for = VOTE_REQ;
								my_current_state=ABORT;
							} else if (canParticipate && msg.getProcessId() == coordinatorId && msg.getMessage().equals(COMMIT) && currTrans == msg.getTransactionId()) {
								executeCommand(msg.getPlaylistCommand());
								ThreePhaseCommitUtility.writeMessageToDTLog(processId, new LogRecord(msg.getTransactionId(), COMMIT, msg.getPlaylistCommand()));
								System.out.println("Process "+processId+" received a commit");
								participant_waiting_for = VOTE_REQ;
								my_current_state=COMMIT;
							} else if (canParticipate && msg.getProcessId() == coordinatorId && msg.getMessage().equals(PRE_COMMIT) && currTrans == msg.getTransactionId()) {
								System.out.println("Process "+processId+" received a pre-commit");
								Message ack = new Message();
								ack.setProcessId(processId);
								ack.setMessage(ACK);
								ack.setTransactionId(currTrans);
								netController.sendMsg(coordinatorId, ThreePhaseCommitUtility.serializeMessage(ack));
								participant_waiting_for=COMMIT;
								my_current_state=PRE_COMMIT;
								System.out.println("Process "+processId+" voted with an ACK and waiting for "+participant_waiting_for);
								//ThreePhaseCommitUtility.writeMessageToDTLog(processId, COMMIT);
							} else if(canParticipate && msg.getMessage().equals(NEW_CO_OD)){
								System.out.println("Process "+processId+" got a NEW_CO_OD from process "+msg.getProcessId());
								if(!hasElectionHappened) {
									System.out.println("removing processes "+coordinatorId);
									System.out.println("active processes "+activeProcesses);
									activeProcesses.remove((Object)new Integer(coordinatorId));
									System.out.println("active processes "+activeProcesses);
									ThreePhaseCommitUtility.saveActiveProcess(processId, activeProcesses);
									hasElectionHappened = false;
								}
								coordinatorId = msg.getProcessId();
								participantTerminationProtocol();
							} else if(canParticipate && msg.getMessage().equals(GET_DECISION)){
								System.out.println("Process "+processId+" got a GET_DECISION from process "+msg.getProcessId());
								activeProcesses.add(msg.getProcessId());
								ThreePhaseCommitUtility.saveActiveProcess(coordinatorId, activeProcesses);
								handleGetDecision(msg);								
							}
						}
						receivedMsgs.clear();
					}
				}
			}

		} catch (Exception e) {
			e.printStackTrace();
			System.exit(1);
		}
	}
	
	public static void executeCommand(PlaylistCommand playlistCommand) throws IOException {
		if("ADD".equals(playlistCommand.getAction())) {
			playList.put(playlistCommand.getSongName(), playlistCommand.getUrl());
		} else if("REMOVE".equals(playlistCommand.getAction())) {
			playList.remove(playlistCommand.getSongName());
		} else {
			if(playList.get(playlistCommand.getSongName())!=null) {
				playList.remove(playlistCommand.getSongName());
				playList.put(playlistCommand.getSongName(), playlistCommand.getUrl());
			} else {
				String keyToRemove = null;
				for(Entry<String,String> entry : playList.entrySet()) {
					if(entry.getValue().equals(playlistCommand.getUrl())) {
						keyToRemove = entry.getKey();
						break;
					}
				}
				playList.remove(keyToRemove);
				playList.put(playlistCommand.getSongName(), playlistCommand.getUrl());
			}
		}
		ThreePhaseCommitUtility.saveLocalState(processId, playList);
	}

	public static void handleGetDecision(List<String> msgs) throws Exception {
		List<String> toBeRemoved = new ArrayList<String>();
		for (String msg : msgs) {
			Message m = ThreePhaseCommitUtility.deserializeMessage(msg);
			if (GET_DECISION.equals(m.getMessage())) {
				handleGetDecision(m);
				toBeRemoved.add(msg);
			}
		}
		msgs.removeAll(toBeRemoved);
	}
	public static void handleGetDecision(Message msg) throws Exception {
		System.out.println("Starting to handle GET_DECISION message");
		LogRecord log = ThreePhaseCommitUtility.fetchRecordForTransaction(processId, msg.getTransactionId());
		Message decision = new Message();
		decision.setMessage(log.getMessage());
		decision.setProcessId(processId);
		decision.setTransactionId(log.getTransactionId());
		decision.setPlaylistCommand(log.getPlayListCommand());
		if (COMMIT.equals(decision.getMessage()) || ABORT.equals(decision.getMessage())) {
			netController.sendMsg(msg.getProcessId(), ThreePhaseCommitUtility.serializeMessage(decision));
			System.out.println("Process "+processId+" has sent a response to a GET_DECISION "+decision);
		}		
	}

	public static void initiateElection() throws Exception {
		hasElectionHappened = true;
		System.out.println("Process "+processId+" is initiating election protocol");
		int oldCoordinatorId = coordinatorId;
		int newCoordinatorId = (coordinatorId + 1) % (NUM_PARTICIPANTS + 1);
		previousCoordinatorId = coordinatorId;
		coordinatorId = newCoordinatorId;
		System.out.println("removing processes "+oldCoordinatorId);
		System.out.println("active processes "+activeProcesses);
		activeProcesses.remove((Object)new Integer(oldCoordinatorId));
		ThreePhaseCommitUtility.saveActiveProcess(coordinatorId, activeProcesses);
		System.out.println("active processes "+activeProcesses);
		if (processId == newCoordinatorId) {
			System.out.println("Process "+processId+" has selected "+newCoordinatorId+" as the new co-ordinator");
			Message newCood = new Message();
			newCood.setProcessId(processId);
			newCood.setMessage(NEW_CO_OD);
			System.out.println("Process "+processId+" sends NEW_CO_OD message to other participants");
			List<Integer> participants = new ArrayList<Integer>(activeProcesses);
			for (Integer parts : participants) {
				if (parts == oldCoordinatorId) {
					continue;
				}
				if (parts == newCoordinatorId)
					continue;
				System.out.println("Sending the new co-od message to the process "+parts);
				netController.sendMsg(parts, ThreePhaseCommitUtility.serializeMessage(newCood));
			}
		}
		System.out.println("Process "+processId+" has ended the election protocol");
	}

	public static void participantTerminationProtocol() throws Exception {
		System.out.println("Process "+processId+" is initiating participant termination protocol");
		List<String> receivedMsgs = new ArrayList<String>();
		boolean notCommitted = true;
		System.out.println("Process "+processId+" is waiting for STATE_REQ");
		while(notCommitted && processId!=coordinatorId) {
			Timer timer = new Timer(PARTICIPANT_TIME_OUT_IN_SECONDS);
			timer.start();
			
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
					state.setTransactionId(msg.getTransactionId());
					System.out.println("msg of process "+processId+" is "+msg);
					if(my_current_state.equals(ABORTED)) {
						state.setMessage(ABORTED);
					} else if(my_current_state.equals(COMMIT)) {
						state.setMessage(COMMITTED);
					} else if(my_current_state.equals(PRE_COMMIT)) {
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
						ThreePhaseCommitUtility.writeMessageToDTLog(processId, new LogRecord(msg.getTransactionId(), ABORT, msg.getPlaylistCommand()));	
					System.out.println("Process "+processId+" received abort during the termination protocol");
					participant_waiting_for="";
					notCommitted = false;
					my_current_state=ABORTED;
				} else if(msg.getProcessId() == coordinatorId && msg.getMessage().equals(COMMIT)){
					LogRecord myState = ThreePhaseCommitUtility.fetchRecordForTransaction(processId, msg.getTransactionId());
					if(!myState.getMessage().equals(COMMIT)) {
						ThreePhaseCommitUtility.writeMessageToDTLog(processId, new LogRecord(msg.getTransactionId(), COMMIT, msg.getPlaylistCommand()));
						executeCommand(msg.getPlaylistCommand());
					}
					participant_waiting_for="";
					my_current_state=COMMIT;
					System.out.println("Process "+processId+" received commit during the termination protocol");
					notCommitted = false;
				} else if(msg.getMessage().equals(GET_DECISION)){
					System.out.println("Process "+processId+" got a GET_DECISION from process "+msg.getProcessId());
					handleGetDecision(msg);								
				} else if(msg.getMessage().equals(PRE_COMMIT)){
					my_current_state=PRE_COMMIT;
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

	public static void coordinatorTerminationProtocol(List<Integer> participants) throws Exception {
		int totalParticipants = participants.size();

		// send STATE_REQ to all processes.
		System.out.println("Starting termination protocol. Co-ordinator is"+processId);
		System.out.println("sending state request to"+participants);
		for(Integer participant : participants) {
			System.out.println("sending state req to :"+participant+ "for trans"+currTrans);
			netController.sendMsg(participant, ThreePhaseCommitUtility.serializeMessage(new Message(coordinatorId, currTrans, STATE_REQ)));
		}

		//Wait for all responses or until timeout
		Timer stateReqTimer = new Timer(COORDINATOR_TIME_OUT_IN_SECONDS);
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
		commitableFlag = my_current_state.equals(PRE_COMMIT);
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
				ThreePhaseCommitUtility.writeMessageToDTLog(processId, new LogRecord(currTrans, ABORT, currentCommand));
			}
			for (Integer participant : participants) {
				netController.sendMsg(participant, ThreePhaseCommitUtility.serializeMessage(new Message(processId, currTrans, ABORT,currentCommand)));
			}
		} else if (commitFlag) {
			System.out.println("deciding to commit!");
			if(!state.equals(COMMIT)) {
				ThreePhaseCommitUtility.writeMessageToDTLog(processId, new LogRecord(currTrans, COMMIT, currentCommand));
				executeCommand(currentCommand);
			}
			for (Integer participant : participants) {
				netController.sendMsg(participant, ThreePhaseCommitUtility.serializeMessage(new Message(processId, currTrans, COMMIT,currentCommand)));
			}
			ThreePhaseCommitUtility.removeTopPlaylistCommand();

		} else if (commitableFlag || COMMIT.equals(participant_waiting_for)) {
			System.out.println("commitable, sending pre commit to uncertain processes.");
			for (Integer participant : uncertainProcesses) {
				netController.sendMsg(participant, ThreePhaseCommitUtility.serializeMessage(new Message(processId, currTrans, PRE_COMMIT)));
			}

			Timer ackTimer = new Timer(COORDINATOR_TIME_OUT_IN_SECONDS);
			ackTimer.start();
			List<String> ackMsgs = new ArrayList<String>();

			while( ackMsgs.size() != uncertainProcesses.size() || !ackTimer.hasTimedOut() ) {
				ackMsgs.addAll(netController.getReceivedMsgs());
			}

			System.out.println("sending commit to all processes");
			ThreePhaseCommitUtility.writeMessageToDTLog(processId, new LogRecord(currTrans, COMMIT, currentCommand));
			ThreePhaseCommitUtility.removeTopPlaylistCommand();
			executeCommand(currentCommand);
			for (Integer participant : participants) {
				netController.sendMsg(participant, ThreePhaseCommitUtility.serializeMessage(new Message(processId, currTrans, COMMIT,currentCommand)));
			}
			System.out.println("sent commit to all processes");
		} else {
			System.out.println("all of em are uncertain. aborting transaction");
			if(!state.equals(ABORT)) {
				ThreePhaseCommitUtility.writeMessageToDTLog(processId, new LogRecord(currTrans, ABORT, currentCommand));
			}
			System.out.println("sending abort to"+participants);
			for (Integer participant : participants) {
				netController.sendMsg(participant, ThreePhaseCommitUtility.serializeMessage(new Message(processId, currTrans, ABORT,currentCommand)));
			}
		}

		System.out.println("completing termination protocol");
	}
}
