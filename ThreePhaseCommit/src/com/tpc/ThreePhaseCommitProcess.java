package com.tpc;

import java.util.List;
import java.util.Map;

import com.tpc.util.Config;
import com.tpc.util.NetController;

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
				
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
