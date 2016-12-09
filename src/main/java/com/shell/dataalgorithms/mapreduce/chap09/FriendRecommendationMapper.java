package com.shell.dataalgorithms.mapreduce.chap09;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class FriendRecommendationMapper extends Mapper<LongWritable, Text, Text, MutualFriendModel> {
	
	@Override
	protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, MutualFriendModel>.Context context)
			throws IOException, InterruptedException {
		
		String line = value.toString();
		String[] tokens = line.split("\t");
		if (tokens == null || tokens.length < 2) {
			return;
		}
		
		String person = tokens[0];
		String friendsListString = tokens[1];
		
		String[] friendsArray = friendsListString.split(",");
		
		for (int i = 0; i < friendsArray.length; i++) {
			context.write(new Text(person), new MutualFriendModel(friendsArray[i], ""));
		}
		
		for (int i = 0; i < friendsArray.length; i++) {
			for (int j = i + 1; j < friendsArray.length; j++) {
				context.write(new Text(friendsArray[i]), new MutualFriendModel(friendsArray[j], person));
				context.write(new Text(friendsArray[j]), new MutualFriendModel(friendsArray[i], person));
			}
		}
	}
}
