package com.shell.dataalgorithms.mapreduce.chap08;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class SecondCommonFriendsMapper extends Mapper<LongWritable, Text, Text, Text> {
	
	private List<String> getFriends(String[] tokens) {
		List<String> friends = new ArrayList<>();
		for (int i = 1; i < tokens.length; i++) {
			friends.add(tokens[i]);
		}
		
		return friends;
	}
	
	private String createKey(String friend1, String friend2) {
		if (friend1.compareTo(friend2) < 0) {
			return friend1 + friend2;
		} else {
			return friend2 + friend1;
		}
	}
	
	@Override
	protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, Text>.Context context)
			throws IOException, InterruptedException {
		
		String[] tokens = value.toString().split(",");
		
		String person = tokens[0];
		List<String> friends = getFriends(tokens);
		
		for (int i = 0; i < friends.size(); i++) {
			for (int j = i + 1; j < friends.size(); j++) {
				context.write(new Text(createKey(friends.get(i), friends.get(j))), new Text(person));
			}
		}
	}

}
