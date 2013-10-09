package com.tpc.util;

public class PlaylistCommand {
	String action;
	String url;
	String songName;
	public PlaylistCommand(String command) {
		if(command==null) return;
		String[] t = command.split(",");
		action = t[0];
		url = t[2];
		songName = t[1];
	}
	public String getAction() {
		return action;
	}
	public void setAction(String action) {
		this.action = action;
	}
	public String getUrl() {
		return url;
	}
	public void setUrl(String url) {
		this.url = url;
	}
	public String getSongName() {
		return songName;
	}
	public void setSongName(String songName) {
		this.songName = songName;
	}
	@Override
	public String toString() {
		return "" + action + "," + songName + "," + url;
	}
}
