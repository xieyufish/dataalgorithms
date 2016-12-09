package com.shell.dataalgorithms.mapreduce.chap09;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

public class MutualFriendModel implements Writable {
	
	private String toUser;
	private String mutualFriend;
	
	public MutualFriendModel() {
		
	}
	
	public MutualFriendModel(String toUser, String mutualFriend) {
		this.toUser = toUser;
		this.mutualFriend = mutualFriend;
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeUTF(toUser);
		out.writeUTF(mutualFriend);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		this.toUser = in.readUTF();
		this.mutualFriend = in.readUTF();
	}

	public String getToUser() {
		return toUser;
	}

	public String getMutualFriend() {
		return mutualFriend;
	}
	
	

}
